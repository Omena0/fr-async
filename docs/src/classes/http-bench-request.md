# `Request`

Mutable state for one active HTTP benchmark request. Instances are reused
between connections by `benchmark()`.

**Source:** [fr_async/http_bench.py:63](fr_async/http_bench.py:63)

## Constructor

```python
Request() -> None
```

Initializes an idle request state. The public attributes track its socket,
file descriptor, timing, connection phase, bytes sent, epoll registration,
response headers, body length, and HTTP status.

## Public state

- `sock` is the active socket or `None`.
- `fd` is the socket file descriptor, or `-1` when inactive.
- `started` and `expires` are the request start time and deadline.
- `phase` tracks the `connect`, `send`, or `receive` stage.
- `sent` tracks bytes sent from the request buffer.
- `registered` records whether the descriptor is registered with epoll.
- `header_buffer` accumulates response headers before parsing.
- `headers_done` indicates whether the header terminator was found.
- `body_received` counts bytes received for the response body.
- `content_length` is the parsed length, or `None` when absent.
- `status` stores the parsed HTTP status code.

## Public method

### `reset`

```python
def reset(
    self,
    sock: socket.socket,
    started: float,
    timeout: float,
) -> None: ...
```

Initializes the state for a new connection: sets the socket and descriptor,
calculates the deadline, resets the phase and counters, and clears response
headers.
