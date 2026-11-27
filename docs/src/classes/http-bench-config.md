# `Config`

Immutable configuration and precomputed connection data for
[`fr_async.http_bench`](../modules/http-bench.md).

**Source:** [fr_async/http_bench.py:23](fr_async/http_bench.py:23)

## Constructor

```python
@dataclass(frozen=True, slots=True)
class Config:
    host: str
    port: int
    path: str
    concurrency: int
    duration: float
    timeout: float
```

The five constructor arguments are required:

- `host` and `port` identify the HTTP server.
- `path` is the request path.
- `concurrency` is the maximum number of in-flight requests and must be at
  least `1`.
- `duration` is the benchmark duration in seconds and must be positive.
- `timeout` is the per-request timeout in seconds and must be positive.

Invalid numeric settings raise `ValueError`. Construction resolves the host
with `socket.getaddrinfo()` and prepares a reusable HTTP/1.1 GET request.

## Derived fields

`family`, `socktype`, `proto`, `sockaddr`, and `request_view` are initialized
by `__post_init__`. They are excluded from the generated initializer and are
not constructor arguments.
