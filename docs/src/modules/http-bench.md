# `fr_async.http_bench`

A standalone HTTP/1.1 GET benchmark client. It is separate from the cooperative
runtime API and uses Linux `select.epoll` directly.

Run it as a module:

```bash
python -m fr_async.http_bench \
  --host 127.0.0.1 \
  --port 5000 \
  --path / \
  --concurrency 64 \
  --duration 30 \
  --timeout 1
```

The defaults are host `127.0.0.1`, port `5000`, path `/`, concurrency `20`,
duration `10` seconds, and timeout `5` seconds. The benchmark prints request
throughput, errors, latency percentiles, a one-percent-low rate, and HTTP
status counts.

## Public classes

- [Config](../classes/http-bench-config.md) — validated benchmark settings
  and precomputed connection/request data. [Source](fr_async/http_bench.py:23)
- [Request](../classes/http-bench-request.md) — reusable state for one active
  HTTP request. [Source](fr_async/http_bench.py:63)

## Public functions

### `send_request`

```python
def send_request(
    request: Request,
    poller: select.epoll,
    request_view: memoryview,
) -> None: ...
```

Sends the remaining request bytes and updates the epoll registration.
[Source](fr_async/http_bench.py:108)

### `parse_headers`

```python
def parse_headers(request: Request) -> None: ...
```

Parses the response status and `Content-Length` once the header terminator is
present. [Source](fr_async/http_bench.py:142)

### `receive_response`

```python
def receive_response(
    request: Request,
    read_buffer: bytearray,
    read_view: memoryview,
) -> int | None: ...
```

Reads until the response body is complete or the socket would block. Returns
the HTTP status code, or `None` when more readiness is needed.
[Source](fr_async/http_bench.py:172)

### `percentile`

```python
def percentile(values: list[float], fraction: float) -> float: ...
```

Calculates a linearly interpolated percentile from sorted values.
[Source](fr_async/http_bench.py:212)

### `benchmark`

```python
def benchmark(config: Config) -> None: ...
```

Runs the benchmark and prints its results.
[Source](fr_async/http_bench.py:220)

### `main`

```python
def main() -> None: ...
```

Parses command-line arguments, creates `Config`, and runs the benchmark.
[Source](fr_async/http_bench.py:402)

## Scope and limitations

The tool creates a fresh connection for each request and sends a single GET
request with `Connection: close`. Its response reader uses `Content-Length`
when present and otherwise waits for connection close; it does not implement
HTTP chunked-transfer decoding. It requires Linux because it uses
`select.epoll`.
