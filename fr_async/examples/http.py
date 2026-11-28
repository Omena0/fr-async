"""Example HTTP server built with the nonblocking socket accept message."""

from urllib.parse import unquote_to_bytes
from collections.abc import Generator
from pathlib import Path
from typing import Any
import socket
import os

from ..messages import WaitReadable, WaitWritable
from ..scheduler import EEVDFScheduler
from ..loop import ExecutionLoop
from ..utils import server

DEBUG = True

def make_http(data: bytes, code: int = 200, size:int|None=None) -> bytes:
    return (
        b"HTTP/1.1 %d\r\n"
        b"Content-Length: %d\r\n"
        b"Connection: close\r\n"
        b"\r\n"
        b"%s"
    ) % (code, size or len(data), data)

def is_in_folder(path_bytes: bytes, folder_bytes: bytes) -> bool:
    path = path_bytes.decode()
    folder = folder_bytes.decode()

    root = Path(folder).resolve()
    candidate = (root / path).resolve()
    return candidate != root and candidate.is_relative_to(root)

def handle_request(request: bytes) -> tuple[bytes, int | None, int | None]:
    response: bytes = b''

    if request.count(b'\r\n') < 2:
        return make_http(b'Bad request', 400), None, None

    request, headers, *content = request.split(b'\r\n')
    parts = request.split(b'\r\n', 1)[0].split(b' ', 2)
    if len(parts) != 3:
        return make_http(b'Bad request', 400), None, None

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
        return make_http(b'Method not allowed', 405), None, None

    # Invalid path
    if (not is_in_folder(path, serve_root)
            or not os.path.exists(path)
            or not os.path.isfile(path)):
        return make_http(b'Not found', 404), None, None

    # Send file
    fd = os.open(path, os.O_RDONLY)
    size = size = os.fstat(fd).st_size
    return make_http(b'', size=size), fd, size

serve_root = b'.'

@server
def serve(client: socket.socket, address: tuple[str, int]) -> Generator[Any, Any]:
    """Accept clients and send each one a short response."""
    yield WaitReadable(client)
    request = client.recv(1024)

    headers, fd, size = handle_request(request)

    # Write response
    yield WaitWritable(client)

    client.sendall(headers)
    if fd is not None and size is not None:
        out_fd = client.fileno()
        os.sendfile(out_fd, fd, 0, size)

    client.close()

if __name__ == "__main__":
    loop = ExecutionLoop(EEVDFScheduler)
    loop.run(serve(port=5000, backlog=1000))
