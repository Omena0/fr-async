import argparse
import contextlib
import errno
import select
import socket
from dataclasses import dataclass, field
from math import ceil
from time import perf_counter

_HEADER_END = b"\r\n\r\n"
_EPOLL_WRITE = select.EPOLLOUT | select.EPOLLET
_EPOLL_READ = select.EPOLLIN | select.EPOLLRDHUP | select.EPOLLET
_EPOLL_IO_ERROR = select.EPOLLERR | select.EPOLLHUP
_PENDING_CONNECT = {
    errno.EINPROGRESS,
    errno.EWOULDBLOCK,
    errno.EALREADY,
    errno.EINTR,
}


@dataclass(frozen=True, slots=True)
class Config:
    host: str
    port: int
    path: str
    concurrency: int
    duration: float
    timeout: float
    family: int = field(init=False, repr=False, compare=False)
    socktype: int = field(init=False, repr=False, compare=False)
    proto: int = field(init=False, repr=False, compare=False)
    sockaddr: tuple[object, ...] = field(init=False, repr=False, compare=False)
    request_view: memoryview = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        if self.duration <= 0:
            raise ValueError("duration must be greater than 0")
        if self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")

        address = socket.getaddrinfo(
            self.host,
            self.port,
            type=socket.SOCK_STREAM,
        )[0]
        object.__setattr__(self, "family", address[0])
        object.__setattr__(self, "socktype", address[1])
        object.__setattr__(self, "proto", address[2])
        object.__setattr__(self, "sockaddr", address[4])

        request = (
            f"GET {self.path} HTTP/1.1\r\n"
            f"Host: {self.host}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode()
        object.__setattr__(self, "request_view", memoryview(request))


class Request:
    __slots__ = (
        "body_received",
        "content_length",
        "expires",
        "fd",
        "header_buffer",
        "headers_done",
        "phase",
        "registered",
        "sent",
        "sock",
        "started",
        "status",
    )

    def __init__(self) -> None:
        self.sock: socket.socket | None = None
        self.fd = -1
        self.started = 0.0
        self.expires = 0.0
        self.phase = "connect"
        self.sent = 0
        self.registered = False
        self.header_buffer = bytearray()
        self.headers_done = False
        self.body_received = 0
        self.content_length: int | None = None
        self.status = 0

    def reset(self, sock: socket.socket, started: float, timeout: float) -> None:
        self.sock = sock
        self.fd = sock.fileno()
        self.started = started
        self.expires = started + timeout
        self.phase = "connect"
        self.sent = 0
        self.registered = False
        self.header_buffer.clear()
        self.headers_done = False
        self.body_received = 0
        self.content_length = None
        self.status = 0


def send_request(
    request: Request,
    poller: select.epoll,
    request_view: memoryview,
) -> None:
    sock = request.sock
    assert sock is not None

    while request.sent < len(request_view):
        try:
            view = request_view if request.sent == 0 else request_view[request.sent:]
            sent = sock.send(view)
        except InterruptedError:
            continue
        except BlockingIOError:
            if request.registered:
                poller.modify(request.fd, _EPOLL_WRITE)
            else:
                poller.register(request.fd, _EPOLL_WRITE)
                request.registered = True
            return

        if sent == 0:
            raise ConnectionError("socket connection closed while sending")
        request.sent += sent

    request.phase = "receive"
    if request.registered:
        poller.modify(request.fd, _EPOLL_READ)
    else:
        poller.register(request.fd, _EPOLL_READ)
        request.registered = True


def parse_headers(request: Request) -> None:
    header_buffer = request.header_buffer
    header_end = header_buffer.find(_HEADER_END)
    if header_end < 0:
        return

    lines = header_buffer[:header_end].split(b"\r\n")
    status_line = lines[0]
    first_space = status_line.find(b" ")
    second_space = status_line.find(b" ", first_space + 1)
    status_bytes = status_line[first_space + 1:second_space if second_space >= 0 else None]
    if first_space < 0 or not status_bytes.isdigit():
        msg = f"Invalid HTTP status line: {status_line!r}"
        raise ValueError(msg)
    request.status = int(status_bytes)

    for line in lines[1:]:
        name, separator, value = line.partition(b":")
        if separator and name.lower().take_bytes() == b"content-length":
            content_length = int(value.strip())
            if content_length < 0:
                raise ValueError("Invalid negative Content-Length")
            request.content_length = content_length
            break

    request.body_received = len(header_buffer) - header_end - 4
    header_buffer.clear()
    request.headers_done = True


def receive_response(
    request: Request,
    read_buffer: bytearray,
    read_view: memoryview,
) -> int | None:
    sock = request.sock
    assert sock is not None

    while True:
        try:
            received = sock.recv_into(read_buffer)
        except InterruptedError:
            continue
        except BlockingIOError:
            return None

        if received == 0:
            if not request.headers_done:
                raise ValueError("Malformed HTTP response: missing header terminator")
            if (
                request.content_length is not None
                and request.body_received < request.content_length
            ):
                raise ValueError("Malformed HTTP response: incomplete body")
            return request.status

        if request.headers_done:
            request.body_received += received
        else:
            request.header_buffer.extend(read_view[:received])
            parse_headers(request)

        if (
            request.headers_done
            and request.content_length is not None
            and request.body_received >= request.content_length
        ):
            return request.status


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def benchmark(config: Config) -> None:
    results: dict[int, int] = {}
    errors: dict[str, int] = {}
    latencies: list[float] = []

    window_duration = min(1.0, config.duration)
    window_count = max(1, ceil(config.duration / window_duration))
    window_requests = [0] * window_count

    host = f"[{config.host}]" if ":" in config.host else config.host
    print(f"Target:      http://{host}:{config.port}{config.path}")
    print(f"Concurrency: {config.concurrency}")
    print(f"Duration:    {config.duration}s")
    print("Benchmarking...", flush=True)

    poller = select.epoll()
    active: dict[int, Request] = {}
    free_requests = [Request() for _ in range(config.concurrency)]
    read_buffer = bytearray(65536)
    read_view = memoryview(read_buffer)
    started = perf_counter()
    deadline = started + config.duration
    total_errors = 0

    def record_error(exc: Exception) -> None:
        nonlocal total_errors
        key = f"{type(exc).__name__}: {exc}"
        errors[key] = errors.get(key, 0) + 1
        total_errors += 1

    def close_request(request: Request) -> None:
        active.pop(request.fd, None)
        if request.registered:
            with contextlib.suppress(KeyError, ValueError):
                poller.unregister(request.fd)
        sock = request.sock
        request.sock = None
        request.registered = False
        request.fd = -1
        if sock is not None:
            sock.close()
        free_requests.append(request)

    def complete(request: Request, status: int) -> None:
        completed = perf_counter()
        results[status] = results.get(status, 0) + 1
        latencies.append(completed - request.started)
        offset = completed - started
        if 0 <= offset < config.duration:
            index = int(offset) if config.duration >= 1 else 0
            window_requests[index] += 1
        close_request(request)

    def fail(request: Request, exc: Exception) -> None:
        close_request(request)
        record_error(exc)

    def launch() -> bool:
        now = perf_counter()
        if now >= deadline or not free_requests:
            return False

        request = free_requests.pop()
        sock: socket.socket | None = None
        try:
            sock = socket.socket(config.family, config.socktype | socket.SOCK_NONBLOCK, config.proto)
            request.reset(sock, now, config.timeout)

            error = sock.connect_ex(config.sockaddr)
            if error in [0, errno.EISCONN]:
                request.phase = "send"
                send_request(request, poller, config.request_view)
            elif error in _PENDING_CONNECT:
                poller.register(request.fd, _EPOLL_WRITE)
                request.registered = True
            else:
                raise OSError(error, "socket connection failed")
        except Exception as exc:
            if request.registered:
                with contextlib.suppress(KeyError, ValueError):
                    poller.unregister(request.fd)
            if sock is not None:
                sock.close()
            request.sock = None
            request.registered = False
            request.fd = -1
            free_requests.append(request)
            record_error(exc)
            return False

        active[request.fd] = request
        return True

    try:
        while perf_counter() < deadline and len(active) < config.concurrency:
            launch()

        while active or perf_counter() < deadline:
            now = perf_counter()
            if active:
                next_expiry = min(request.expires for request in active.values())
                timeout = max(0.0, next_expiry - now)
                if now < deadline:
                    timeout = min(timeout, deadline - now)
            else:
                timeout = max(0.0, deadline - now)

            events = poller.poll(timeout, max(1, len(active)))

            for fd, mask in events:
                request = active.get(fd)
                if request is None:
                    continue

                try:
                    if perf_counter() >= request.expires:
                        fail(request, TimeoutError(f"timed out during {request.phase}"))
                        continue

                    if request.phase in ("connect", "send") and mask & (select.EPOLLOUT | _EPOLL_IO_ERROR):
                        if request.phase == "connect":
                            sock = request.sock
                            assert sock is not None
                            error = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                            if error:
                                raise OSError(error, "socket connection failed")
                            request.phase = "send"
                        if request.phase == "send":
                            send_request(request, poller, config.request_view)

                    if request.phase == "receive" and mask & (select.EPOLLIN | select.EPOLLRDHUP | _EPOLL_IO_ERROR):
                        status = receive_response(request, read_buffer, read_view)
                        if status is not None:
                            complete(request, status)
                except Exception as exc:
                    fail(request, exc)

            now = perf_counter()
            for request in tuple(active.values()):
                if now >= request.expires:
                    fail(request, TimeoutError(f"timed out during {request.phase}"))

            while (
                now < deadline
                and len(active) < config.concurrency
                and launch()
            ):
                now = perf_counter()

        elapsed = perf_counter() - started
    finally:
        for request in tuple(active.values()):
            close_request(request)
        poller.close()

    total = sum(results.values())
    print("\nResults")
    print(f"Elapsed:     {elapsed:.2f}s")
    print(f"Requests:    {total}")
    print(f"Throughput:  {total / elapsed:.2f} req/s")
    print(f"Errors:      {total_errors}")

    if latencies:
        print(f"Latency p95: {percentile(latencies, 0.95) * 1000:.2f} ms")
        print(f"Latency p99: {percentile(latencies, 0.99) * 1000:.2f} ms")
        window_rates = [
            count / min(window_duration, config.duration - index * window_duration)
            for index, count in enumerate(window_requests)
        ]
        low_count = max(1, ceil(len(window_rates) * 0.01))
        one_percent_low = sum(sorted(window_rates)[:low_count]) / low_count
        print(f"1% low:      {one_percent_low:.2f} req/s (1s windows)")

    for status, count in sorted(results.items()):
        print(f"HTTP {status}:    {count} ({count / elapsed:.2f} req/s)")

    if errors:
        print("\nErrors:")
        for error, count in sorted(errors.items(), key=lambda item: -item[1])[:10]:
            print(f"  {count:>8}x  {error}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--path", default="/")
    parser.add_argument("-c", "--concurrency", type=int, default=20)
    parser.add_argument("-d", "--duration", type=float, default=10)
    parser.add_argument("-t", "--timeout", type=float, default=5)
    args = parser.parse_args()

    try:
        config = Config(
            host=args.host,
            port=args.port,
            path=args.path,
            concurrency=args.concurrency,
            duration=args.duration,
            timeout=args.timeout,
        )
        benchmark(config)
    except (OSError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
