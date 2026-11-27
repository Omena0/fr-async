"""Example HTTP server built with the nonblocking socket accept message."""

from urllib.parse import unquote_to_bytes
from typing import Any, Generator
from pathlib import Path
import traceback
import socket
import time
import os

from .messages import Accept, WaitReadable, WaitWritable, Sleep, FileRead
from .classes import TaskGroup, Yieldable, Task, File
from .loop import ExecutionLoop, getEventLoop
from .scheduler import EEVDFScheduler

DEBUG = True

def make_http(data: bytes, code: int = 200) -> bytes:
    return (
        b"HTTP/1.1 %d\r\n"
        b"Content-Length: %d\r\n"
        b"Connection: close\r\n"
        b"\r\n"
        b"%s"
    ) % (code, len(data), data)

def is_in_folder(path_bytes: bytes, folder_bytes: bytes) -> bool:
    path = path_bytes.decode()
    folder = folder_bytes.decode()

    root = Path(folder).resolve()
    candidate = (root / path).resolve()
    return candidate != root and candidate.is_relative_to(root)

def handle_request(request: bytes) -> Generator[FileRead[bytes], Any, bytes]:
    response: bytes = b''

    if request.count(b'\r\n') < 2:
        return make_http(b'Bad request', 400)

    request, headers, *content = request.split(b'\r\n')
    parts = request.split(b'\r\n', 1)[0].split(b' ', 2)
    if len(parts) != 3:
        return make_http(b'Bad request', 400)

    method, path, version = parts

    path = unquote_to_bytes(path)

    # Parse query
    query = b''
    if b'?' in path:
        path, query = path.split(b'?', 1)

    if path == b'/':
        path = b'/index.html'

    path = serve_root + path

    # Invalid method
    if method != b'GET':
        return make_http(b'Method not allowed', 405)

    # Invalid path
    if (not is_in_folder(path, serve_root)
            or not os.path.exists(path)
            or not os.path.isfile(path)):
        return make_http(b'Not found', 404)

    # Send file
    with File(path.decode(errors='ignore'), 'rb') as f:
        content = yield f.read()

    return make_http(content)

serve_root = b'static'
def serve(client: socket.socket, address: tuple[str, int]) -> Generator[Any, Any, None]:
    """Accept clients and send each one a short response."""
    try:
        yield WaitReadable(client)
        request = client.recv(1024)

        response = yield Task(handle_request(request))

        # Write response
        yield WaitWritable(client)
        client.sendall(response)

    except:
        traceback.print_exc()

    finally:
        client.close()

def main_fn() -> Any:
    """Run the example server on localhost port 5000."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 5000))
    listener.listen(1000)

    loop = getEventLoop()

    while True:
        accepts = yield Accept(listener)

        for client, address in accepts:
            loop.schedule(Task(serve(client, address), background=True))

main = Yieldable(main_fn)
if __name__ == "__main__":
    ExecutionLoop(EEVDFScheduler).run(main())
