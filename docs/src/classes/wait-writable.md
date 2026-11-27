# `WaitWritable`

A socket message that resolves when the socket is reported writable by the
current event loop's selector.

**Source:** [fr_async/messages.py:162](fr_async/messages.py:162)

## Constructor

```python
WaitWritable(sock: socket.socket, *, background: bool = False)
```

Stores `sock` and optionally enables background behavior.

## Public method

### `register`

Registers the socket for `selectors.EVENT_WRITE` with the current loop.

## Usage

```python
from fr_async.messages import WaitWritable

def send_all(sock, data: bytes):
    view = memoryview(data)
    while view:
        yield WaitWritable(sock)
        sent = sock.send(view)
        view = view[sent:]
```

The message only waits for readiness; it does not send bytes or change the
socket's blocking mode. Use a nonblocking socket for the subsequent I/O.
