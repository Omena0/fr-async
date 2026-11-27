# `Accept`

A message for accepting every connection currently queued on a listening
socket without blocking the event-loop thread.

**Source:** [fr_async/messages.py:177](fr_async/messages.py:177)

## Constructor

```python
Accept(sock: socket.socket, *, background: bool = False)
```

Stores the listening socket and initializes an empty `result` list.

## Public state

`result` contains `(client_socket, address)` pairs accumulated by the message.
Each accepted client socket is switched to nonblocking mode.

## Public methods

### `register`

Sets the listening socket to nonblocking mode and registers it for read
readiness.

### `resolve`

Drains `accept()` until it raises `BlockingIOError`. The message is resolved
when at least one connection has been accepted. The yielded result is a list
of `(socket.socket, tuple[str, int])` pairs.

## Example

```python
from fr_async.messages import Accept

def accept_loop(listener):
    while True:
        clients = yield Accept(listener)
        for client, address in clients:
            handle_client(client, address)
```

`Accept` changes the listener's blocking mode and returns all connections
available at that readiness notification, rather than just one connection.
