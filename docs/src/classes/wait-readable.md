# `WaitReadable`

A socket message that resolves when the socket is reported readable by the
current event loop's selector.

**Source:** [fr_async/messages.py:147](fr_async/messages.py:147)

## Constructor

```python
WaitReadable(sock: socket.socket, *, background: bool = False)
```

Stores `sock` and optionally enables background behavior.

## Public method

### `register`

Registers the socket for `selectors.EVENT_READ` with the current loop.

## Usage

```python
from fr_async.messages import WaitReadable

def receive_one(sock):
    yield WaitReadable(sock)
    return sock.recv(4096)
```

The message only waits for readiness; it does not call `recv()` or change the
socket's blocking mode. Use a nonblocking socket for the subsequent I/O.
