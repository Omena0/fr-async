import math
import socket
import time
from collections.abc import Generator
from typing import Any, Literal, TypeIs

from .classes import Task, Yieldable
from .loop import ExecutionLoop
from .messages import (
    AsCompleted,
    Messages,
    Sleep,
    ToThread,
    WaitReadable,
    WaitWritable,
)

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

MAX_LATENESS = 0.004
MAX_FAIRNESS_LATENESS = 0.0025

PASSED: list[str] = []
METRICS: dict[str, tuple[float, float, float, float]] = {}


def require[T](value: T | None, message: str) -> T:
    if value is None:
        raise AssertionError(message)
    return value


def check(condition: bool, message: str) -> TypeIs[Literal[True]]:
    if not condition:
        raise AssertionError(message)

    return condition


def report(name: str, details: str = "") -> None:
    PASSED.append(name)
    suffix = f" — {details}" if details else ""
    print(f"[PASS] {name}{suffix}")


def percentile(values: list[float], fraction: float) -> float:
    values = sorted(values)
    index = min(len(values) - 1, math.ceil(len(values) * fraction) - 1)
    return values[index]


def ms(seconds: float) -> str:
    return f"{seconds * 1000:<06.2f} ms"


def record_metric(
    name: str,
    value: float,
    *,
    target: float,
    limit: float,
    weight: float = 1.0,
) -> None:
    """
    Record a lower-is-better performance metric.

    The limit is a hard pass/fail threshold.
    The target is the point at which the metric receives 100 points.
    """

    check(math.isfinite(value), f"{name}: non-finite measurement")
    check(value >= 0, f"{name}: negative measurement")

    check(
        value <= limit,
        f"{name}: {ms(value)} exceeds the {ms(limit)} limit",
    )

    METRICS[name] = (value, target, limit, weight)


def metric_score(value: float, target: float, limit: float) -> float:
    if value <= target:
        return 100.0
    if value >= limit:
        return 0.0

    x = (value - target) / (limit - target)
    return 100.0 * (1.0 - x) ** 2


def calculate_score() -> float:
    if not METRICS:
        return 0.0

    weighted_score = sum(
        metric_score(value, target, limit) * weight
        for value, target, limit, weight in METRICS.values()
    )

    total_weight = sum(
        weight
        for _, _, _, weight in METRICS.values()
    )

    return weighted_score / total_weight


def print_score() -> None:
    print()
    print("Performance metrics (target = 0 ms)")

    total_weight = sum(weight for _, _, _, weight in METRICS.values())

    for name, (value, target, limit, weight) in METRICS.items():
        score = metric_score(value, target, limit)
        contribution = score * weight / total_weight

        print(
            f"  {name:>40}: {score:>6.2f}% "
            f"(weight={weight:<03}, contribution={contribution:<04.1f}%, "
            f"overhead={ms(value):<06}, imit={ms(limit):<06})"
        )

    print()
    print(f"General score: {calculate_score():.3f}%")



# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def timer_probe(index: int, delay: float, observations: list[tuple[int, float, float]]) -> Generator[Any, Any]:
    start = time.monotonic()

    yield Sleep(delay)

    elapsed = time.monotonic() - start
    observations.append((index, delay, elapsed))


def order_probe(delay: float, label: str, observations: list[tuple[str, float]]) -> Generator[Any, Any]:
    yield Sleep(delay)
    observations.append((label, time.monotonic()))


def thread_probe(delay: float, observations: list[float]) -> Generator[Any, Any]:
    yield ToThread(time.sleep, delay)
    observations.append(time.monotonic())


def delayed_return[T](delay: float, value: T) -> tuple[T, float]:
    time.sleep(delay)
    return value, time.monotonic()


def delayed_identity(delay: Any, value: Any) -> Any:
    time.sleep(delay)
    return value


def socket_sender(sock: socket.socket) -> Generator[Any, Any]:
    yield Sleep(0.04)
    sock.send(b"x")


def socket_sender_payload(sock: socket.socket, delay: float, payload: bytes) -> Generator[Any, Any]:
    yield Sleep(delay)
    sock.sendall(payload)


def socket_reader(sock: socket.socket, received: list[bytes]) -> Generator[Any, Any]:
    yield WaitWritable(sock)
    yield WaitReadable(sock)
    received.append(sock.recv(1))


def socket_reader_payload(sock: socket.socket, received: list[bytes]) -> Generator[Any, Any]:
    yield WaitReadable(sock)
    received.append(sock.recv(128))


def finite_ticker(count: int, period: float, observations: list[float]) -> Generator[Any, Any]:
    for _ in range(count):
        yield Sleep(period)
        observations.append(time.monotonic())


def short_background_task(index: int, observations: list[tuple[int, float]]) -> Generator[Any, Any]:
    yield None
    observations.append((index, time.monotonic()))


def finite_thread_task(delay: float, observations: list[float]) -> Generator[Any, Any]:
    yield ToThread(time.sleep, delay)
    observations.append(time.monotonic())


# ------------------------------------------------------------
# 1. Immediate, negative and very short sleeps
# ------------------------------------------------------------

def test_sleep_edges() -> Generator[Any, Any]:
    measurements: dict[str, float] = {}

    for name, delay in (
        ("zero", 0),
        ("negative", -0.02),
        ("1ms", 0.001),
    ):
        start: float = time.monotonic()
        yield Sleep(delay)
        elapsed: float = time.monotonic() - start

        check(
            elapsed + 0.002 >= max(delay, 0),
            f"{name} Sleep returned too early: {ms(elapsed)}",
        )
        check(
            elapsed < 0.20,
            f"{name} Sleep took unexpectedly long: {ms(elapsed)}",
        )

        measurements[name] = elapsed

    report(
        "zero / negative / short sleeps",
        ", ".join(f"{k}: {ms(v)}" for k, v in measurements.items()),
    )


# ------------------------------------------------------------
# 2. Basic timer accuracy and jitter
# ------------------------------------------------------------

def test_sleep_accuracy() -> Generator[Any, Any]:
    delays: list[float] = [0.002, 0.005, 0.010, 0.020] * 5
    latenesses: list[float] = []

    for delay in delays:
        start: float = time.monotonic()
        yield Sleep(delay)
        elapsed: float = time.monotonic() - start

        lateness: float = elapsed - delay
        latenesses.append(lateness)

        check(
            lateness >= -0.002,
            f"Sleep({delay}) returned early by {ms(-lateness)}",
        )

    maximum = max(latenesses)
    p50 = percentile(latenesses, 0.50)
    p95 = percentile(latenesses, 0.95)

    record_metric(
        "timer maximum lateness",
        max(0.0, maximum),
        target=0.0,
        limit=MAX_LATENESS,
        weight=2.0,
    )

    report(
        "basic timer accuracy",
        f"p50={ms(max(0.0, p50))}, "
        f"p95={ms(max(0.0, p95))}, "
        f"max={ms(maximum)}",
    )


# ------------------------------------------------------------
# 3. Periodic timers using absolute deadlines
# ------------------------------------------------------------

def test_periodic_timing() -> Generator[Any, Any]:
    period = 0.025
    count = 24

    start: float = time.monotonic()
    latenesses: list[float] = []

    for tick in range(1, count + 1):
        deadline: float = start + tick * period
        remaining: float = max(0.0, deadline - time.monotonic())

        yield Sleep(remaining)

        actual: float = time.monotonic()
        latenesses.append(actual - deadline)

    maximum: float = max(latenesses)
    final_drift: float = latenesses[-1]

    check(
        maximum >= -0.002,
        f"Periodic timer returned too early: {ms(-maximum)}",
    )

    record_metric(
        "periodic timer maximum lateness",
        max(0.0, maximum),
        target=0.0,
        limit=MAX_LATENESS,
        weight=1.5,
    )

    report(
        "absolute-deadline periodic timing",
        f"{count} ticks at {period}s, "
        f"max lateness={ms(maximum)}, "
        f"final drift={ms(final_drift)}",
    )


# ------------------------------------------------------------
# 4. Multiple blockers on one task
# ------------------------------------------------------------

def test_multiple_blockers() -> Generator[Any, Any]:
    delays = (0.02, 0.05, 0.12)

    start = time.monotonic()
    yield Messages([Sleep(delay) for delay in delays])
    elapsed = time.monotonic() - start

    check(
        elapsed >= max(delays) - 0.003,
        f"Task resumed before all blockers resolved: {ms(elapsed)}",
    )
    check(
        elapsed < 0.30,
        f"Task took too long to resume: {ms(elapsed)}",
    )

    record_metric(
        "multiple-blocker overhead",
        max(0.0, elapsed - max(delays)),
        target=0.0,
        limit=MAX_LATENESS,
    )

    report(
        "multiple blockers",
        f"expected >= {ms(max(delays))}, actual={ms(elapsed)}",
    )


# ------------------------------------------------------------
# 5. Many concurrent timers
# ------------------------------------------------------------

def test_concurrent_timers() -> Generator[Any, Any]:
    delays = [
        0.12, 0.02, 0.10, 0.04,
        0.08, 0.03, 0.11, 0.05,
        0.07, 0.01, 0.09, 0.06,
        0.04, 0.10, 0.02, 0.08,
    ]

    observations: list[tuple[int, float, float]] = []

    for index, delay in enumerate(delays):
        yield Task(
            timer_probe(index, delay, observations),
            background=True,
        )

    yield Sleep(0.30)

    check(
        len(observations) == len(delays),
        f"Only {len(observations)}/{len(delays)} timers completed",
    )

    latenesses = [
        elapsed - delay
        for _, delay, elapsed in observations
    ]

    minimum = min(latenesses)
    maximum = max(latenesses)

    check(
        minimum >= -0.003,
        f"A timer fired early by {ms(-minimum)}",
    )

    record_metric(
        "concurrent timer maximum lateness",
        max(0.0, maximum),
        target=0.0,
        limit=MAX_LATENESS,
        weight=1.5,
    )

    report(
        "concurrent timers",
        f"{len(observations)} timers, "
        f"min lateness={ms(minimum)}, "
        f"max lateness={ms(maximum)}",
    )


# ------------------------------------------------------------
# 6. Selector wakeups caused by thread completions
# ------------------------------------------------------------

def test_thread_wakeups() -> Generator[Any, Any]:
    delays = (0.015, 0.030, 0.045, 0.060, 0.075, 0.090, 0.105)
    observations: list[float] = []

    for delay in delays:
        yield Task(
            thread_probe(delay, observations),
            background=True,
        )

    start = time.monotonic()
    yield Sleep(0.14)
    elapsed = time.monotonic() - start

    # Allow completions arriving near the timer deadline to be handled.
    yield Sleep(0.04)

    check(
        len(observations) == len(delays),
        f"Only {len(observations)}/{len(delays)} thread completions handled",
    )
    check(
        elapsed < 0.25,
        f"Timer was delayed by thread wakeups: {ms(elapsed)}",
    )

    record_metric(
        "timer lateness during thread wakeups",
        max(0.0, elapsed - 0.14),
        target=0.0,
        limit=MAX_LATENESS,
    )

    report(
        "timer accuracy during thread wakeups",
        f"timer elapsed={ms(elapsed)}, "
        f"thread completions={len(observations)}",
    )


# ------------------------------------------------------------
# 7. Socket readiness and timers
# ------------------------------------------------------------

def test_socket_events() -> Generator[Any, Any]:
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)

    received: list[bytes] = []

    try:
        start: float = time.monotonic()

        yield Task(socket_sender(right), background=True)
        yield Task(socket_reader(left, received))

        elapsed = time.monotonic() - start

        check(
            received == [b"x"],
            f"Unexpected socket data: {received!r}",
        )

        record_metric(
            "socket readiness lateness",
            max(0.0, elapsed - 0.04),
            target=0.0,
            limit=MAX_LATENESS,
        )
    finally:
        left.close()
        right.close()

    report("socket readiness", "writable and readable events")


# ------------------------------------------------------------
# 8. AsCompleted streams results while timers keep running
# ------------------------------------------------------------

def ticker(observations: list[float]) -> Generator[Any, Any]:
    while True:
        yield Sleep(0.025)
        observations.append(time.monotonic())


def test_as_completed() -> Generator[Any, Any]:
    ticks: list[float] = []
    ticker_task = yield Task(ticker(ticks), background=True)

    start = time.monotonic()

    operations = [
        ToThread(delayed_return, 0.06, "fast"),
        ToThread(delayed_return, 0.15, "middle"),
        ToThread(delayed_return, 0.24, "slow"),
    ]

    received = list((yield AsCompleted(*operations)))
    elapsed = time.monotonic() - start

    ticker_task.stop()
    yield Sleep(0.06)

    labels = [value for value, _ in received]
    completion_times = [completed_at for _, completed_at in received]

    check(
        labels == ["fast", "middle", "slow"],
        f"Unexpected completion order: {labels}",
    )
    check(
        completion_times == sorted(completion_times),
        "Results were not consumed in completion order",
    )
    check(
        len(ticks) >= 4,
        f"Background ticker barely ran during AsCompleted: {len(ticks)} ticks",
    )
    check(
        ticker_task.done,
        "Background ticker did not stop",
    )

    record_metric(
        "AsCompleted completion overhead",
        max(0.0, elapsed - 0.24),
        target=0.0,
        limit=MAX_LATENESS,
    )

    report(
        "AsCompleted streaming",
        f"order={labels}, "
        f"elapsed={ms(elapsed)}, "
        f"background ticks={len(ticks)}",
    )


# ------------------------------------------------------------
# 9. Exceptions from ordinary ToThread
# ------------------------------------------------------------

def failing_thread() -> None:
    time.sleep(0.02)
    raise ValueError("expected thread exception")


def test_thread_exception() -> Generator[Any, Any]:
    caught = False

    try:
        yield ToThread(failing_thread)
    except ValueError as exc:
        caught = str(exc) == "expected thread exception"

    check(caught, "ToThread exception was not propagated correctly")

    report("ToThread exception propagation")


# ------------------------------------------------------------
# 10. Exceptions from AsCompleted
# ------------------------------------------------------------

def failing_delayed_thread() -> None:
    time.sleep(0.025)
    raise LookupError("expected AsCompleted exception")


def test_as_completed_exception() -> Generator[Any, Any]:
    caught = False

    operations: list[ToThread[tuple[Any, float] | None, Any]] = [
        ToThread(failing_delayed_thread),
        ToThread(delayed_return, 0.10, "later"),
    ]

    try:
        for _ in (yield AsCompleted(*operations)):
            pass

    except LookupError as exc:
        caught: bool = str(exc) == "expected AsCompleted exception"

    check(caught, "AsCompleted exception was not propagated correctly")

    report("AsCompleted exception propagation")


# ------------------------------------------------------------
# 11. Background task stopping while sleeping
# ------------------------------------------------------------

def stoppable_ticker(events: list[tuple[str, float]]) -> Generator[Any, Any]:
    try:
        while True:
            yield Sleep(0)
            events.append(("tick", time.monotonic()))
    finally:
        events.append(("closed", time.monotonic()))


def test_background_stop() -> Generator[Any, Any]:
    events: list[tuple[str, float]] = []

    task = yield Task(
        stoppable_ticker(events),
        background=True,
    )

    yield Sleep(0.02)

    stop_requested = time.monotonic()
    task.stop()

    # Give its outstanding Sleep time to expire.
    yield Sleep(0.15)

    check(task.done, "Background task did not finish")
    check(
        any(kind == "closed" for kind, _ in events),
        "Generator finally block did not execute",
    )

    stopped_at = next(
        timestamp for kind, timestamp in events if kind == "closed"
    )

    latency = stopped_at - stop_requested

    check(
        0 <= latency < 0.15,
        f"Unexpected cancellation latency: {ms(latency)}",
    )

    record_metric(
        "background stop latency",
        latency,
        target=0.0,
        limit=0.0005,
    )

    report(
        "background task stopping",
        f"cooperative stop latency={ms(latency)}",
    )


# ------------------------------------------------------------
# 12. Ready-queue fairness
# ------------------------------------------------------------

def cooperative_spinner(duration: float) -> Generator[Any, Any]:
    deadline = time.monotonic() + duration

    while time.monotonic() < deadline:
        chunk_end = time.monotonic() + 0.0005

        while time.monotonic() < chunk_end:
            pass

        yield None


def test_ready_queue_fairness() -> Generator[Any, Any]:
    yield Task(
        cooperative_spinner(0.06),
        background=True,
    )

    start = time.monotonic()
    yield Sleep(0.01)
    elapsed = time.monotonic() - start

    lateness = max(0.0, elapsed - 0.01)

    record_metric(
        "ready-queue timer lateness",
        lateness,
        target=0.0,
        limit=MAX_FAIRNESS_LATENESS,
        weight=2.0,
    )

    report(
        "ready-queue fairness",
        f"10ms timer lateness={ms(lateness)}",
    )


# ============================================================
# Additional tests
# ============================================================


# ------------------------------------------------------------
# 13. Timers should complete in deadline order
# ------------------------------------------------------------

def test_sleep_deadline_order() -> Generator[Any, Any]:
    delays = (
        (0.015, "first"),
        (0.045, "second"),
        (0.075, "third"),
        (0.105, "fourth"),
    )

    observations: list[tuple[str, float]] = []

    for delay, label in delays:
        yield Task(
            order_probe(delay, label, observations),
            background=True,
        )

    yield Sleep(0.15)

    labels = [label for label, _ in observations]
    expected = [label for _, label in delays]

    check(
        labels == expected,
        f"Timers completed in unexpected order: {labels}",
    )

    report(
        "timer deadline ordering",
        f"order={labels}",
    )


# ------------------------------------------------------------
# 14. High timer fan-out
# ------------------------------------------------------------

def test_sleep_fanout() -> Generator[Any, Any]:
    count = 64
    observations: list[tuple[int, float, float]] = []

    delays: list[float] = [
        0.005 + (index % 8) * 0.002
        for index in range(count)
    ]

    for index, delay in enumerate(delays):
        yield Task(
            timer_probe(index, delay, observations),
            background=True,
        )

    yield Sleep(0.08)

    check(
        len(observations) == count,
        f"Only {len(observations)}/{count} fan-out timers completed",
    )

    latenesses = [
        elapsed - delay
        for _, delay, elapsed in observations
    ]

    maximum = max(latenesses)
    minimum = min(latenesses)

    check(
        minimum >= -0.003,
        f"A fan-out timer fired early by {ms(-minimum)}",
    )

    record_metric(
        "64-timer fan-out lateness",
        max(0.0, maximum),
        target=0.0,
        limit=MAX_LATENESS,
    )

    report(
        "64 concurrent timers",
        f"max lateness={ms(maximum)}",
    )


# ------------------------------------------------------------
# 15. Mixed Message blockers
# ------------------------------------------------------------

def test_mixed_message_blockers() -> Generator[Any, Any]:
    start = time.monotonic()

    yield Messages([
        Sleep(0.025),
        ToThread(time.sleep, 0.045),
    ])

    elapsed = time.monotonic() - start

    check(
        elapsed >= 0.042,
        f"Task resumed before thread blocker completed: {ms(elapsed)}",
    )
    check(
        elapsed < 0.20,
        f"Mixed blockers took too long: {ms(elapsed)}",
    )

    record_metric(
        "mixed blocker overhead",
        max(0.0, elapsed - 0.045),
        target=0.0,
        limit=MAX_LATENESS,
    )

    report(
        "mixed Sleep and ToThread blockers",
        f"expected >= 45ms, actual={ms(elapsed)}",
    )


# ------------------------------------------------------------
# 16. WaitWritable independently
# ------------------------------------------------------------

def test_socket_writable() -> Generator[Any, Any]:
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)

    try:
        yield WaitWritable(left)

        sent = left.send(b"w")
        received = right.recv(1)

        check(sent == 1, f"Unexpected send result: {sent}")
        check(
            received == b"w",
            f"Unexpected received data: {received!r}",
        )
    finally:
        left.close()
        right.close()

    report("WaitWritable", "writable socket accepted data")


# ------------------------------------------------------------
# 17. WaitReadable delivers the complete payload
# ------------------------------------------------------------

def test_socket_payload() -> Generator[Any, Any]:
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)

    payload = b"custom payload\x00with binary data"
    received: list[bytes] = []

    try:
        yield Task(
            socket_sender_payload(right, 0.025, payload),
            background=True,
        )

        yield Task(socket_reader_payload(left, received))

        check(
            received == [payload],
            f"Payload mismatch: {received!r}",
        )
    finally:
        left.close()
        right.close()

    report(
        "socket payload delivery",
        f"{len(payload)} bytes received correctly",
    )


# ------------------------------------------------------------
# 18. Peer closure should wake WaitReadable
# ------------------------------------------------------------

def test_socket_eof() -> Generator[Any, Any]:
    left, right = socket.socketpair()
    left.setblocking(False)

    try:
        right.close()

        yield WaitReadable(left)

        received = left.recv(1)

        check(
            received == b"",
            f"Expected EOF, received {received!r}",
        )
    finally:
        left.close()

    report("socket peer EOF", "peer closure woke WaitReadable")


# ------------------------------------------------------------
# 19. Multiple independent socket pairs
# ------------------------------------------------------------

def test_socket_multiple_pairs() -> Generator[Any, Any]:
    pairs = [socket.socketpair() for _ in range(2)]

    for left, right in pairs:
        left.setblocking(False)
        right.setblocking(False)

    payloads: tuple[bytes, bytes] = (b"socket-one", b"socket-two")
    received: list[list[bytes]] = [[], []]

    try:
        for index, (_, right) in enumerate(pairs):
            yield Task(
                socket_sender_payload(
                    right,
                    0.02 + index * 0.02,
                    payloads[index],
                ),
                background=True,
            )

        for index, (left, _) in enumerate(pairs):
            yield Task(
                socket_reader_payload(left, received[index]),
            )

        for index, payload in enumerate(payloads):
            check(
                received[index] == [payload],
                f"Socket {index} received {received[index]!r}",
            )
    finally:
        for left, right in pairs:
            left.close()
            right.close()

    report("multiple socket pairs", "both independent sockets delivered data")


# ------------------------------------------------------------
# 20. ToThread return-value propagation
# ------------------------------------------------------------

def test_thread_return_value() -> Generator[Any, Any]:
    result = yield ToThread(pow, 2, 10)

    check(
        result == 1024,
        f"Unexpected ToThread result: {result!r}",
    )

    report("ToThread return value", f"pow(2, 10) = {result}")


# ------------------------------------------------------------
# 21. ToThread keyword arguments
# ------------------------------------------------------------

def thread_transform(value: int, *, multiplier: int, offset: int) -> int:
    return value * multiplier + offset


def test_thread_keyword_arguments() -> Generator[Any, Any]:
    result = yield ToThread(
        thread_transform,
        7,
        multiplier=6,
        offset=1,
    )

    check(
        result == 43,
        f"Unexpected keyword-argument result: {result!r}",
    )

    report("ToThread keyword arguments", f"result={result}")


# ------------------------------------------------------------
# 22. ToThread operations should run concurrently
# ------------------------------------------------------------

def test_thread_parallelism() -> Generator[Any, Any]:
    count = 4
    delay = 0.08

    operations = [
        ToThread(time.sleep, delay)
        for _ in range(count)
    ]

    start: float = time.monotonic()

    results: list[None] = list((yield AsCompleted(*operations)))

    elapsed: float = time.monotonic() - start

    check(
        len(results) == count,
        f"Only {len(results)}/{count} thread operations completed",
    )
    check(
        elapsed < 0.25,
        f"Thread operations appear serialized: {ms(elapsed)}",
    )

    overhead = elapsed - delay

    record_metric(
        "four-thread parallel execution",
        overhead,
        target=0,
        limit=0.002,
        weight=1.5,
    )

    report(
        "thread parallelism",
        f"{count} × {ms(delay)} operations completed in {ms(elapsed)}",
    )


# ------------------------------------------------------------
# 23. AsCompleted should yield the first result early
# ------------------------------------------------------------

def test_as_completed_first_result() -> Generator[Any, Any]:
    operations = [
        ToThread(delayed_return, 0.14, "slow"),
        ToThread(delayed_return, 0.08, "middle"),
        ToThread(delayed_return, 0.02, "fast"),
    ]

    start: float = time.monotonic()
    received: list[tuple[Any, float]] = []
    first_result_latency = None

    for result in (yield AsCompleted(*operations)):
        if first_result_latency is None:
            first_result_latency = time.monotonic() - start

        received.append(result)

    first_result_latency = require(
        first_result_latency,
        "No first result"
    )

    check(
        len(received) == 3,
        f"Only {len(received)}/3 operations completed",
    )
    check(
        received[0][0] == "fast",
        f"First result was not the fastest operation: {received[0]!r}",
    )
    check(
        first_result_latency < 0.12,
        f"AsCompleted waited too long for its first result: "
        f"{ms(first_result_latency)}",
    )

    overhead = first_result_latency - 0.02

    record_metric(
        "AsCompleted first-result latency",
        overhead,
        target=0,
        limit=0.002,
        weight=1.5,
    )

    report(
        "AsCompleted first-result latency",
        f"first={received[0][0]}, latency={ms(first_result_latency)}",
    )


# ------------------------------------------------------------
# 24. AsCompleted with a larger batch
# ------------------------------------------------------------

def test_as_completed_batch() -> Generator[Any, Any]:
    delays = (0.07, 0.02, 0.06, 0.03, 0.05, 0.04)

    operations = [
        ToThread(delayed_return, delay, str(index))
        for index, delay in enumerate(delays)
    ]

    start = time.monotonic()

    results: list[tuple[str, float]] = list((yield AsCompleted(*operations)))

    elapsed = time.monotonic() - start

    labels: list[str] = [label for label, _ in results]
    completion_times: list[float] = [timestamp for _, timestamp in results]

    check(
        len(results) == len(delays),
        f"Only {len(results)}/{len(delays)} results returned",
    )
    check(
        set(labels) == {str(index) for index in range(len(delays))},
        f"Missing or duplicate results: {labels}",
    )
    check(
        completion_times == sorted(completion_times),
        "Batch results were not returned in completion order",
    )

    record_metric(
        "AsCompleted batch overhead",
        max(0.0, elapsed - max(delays)),
        target=0.0,
        limit=MAX_LATENESS,
    )

    report(
        "AsCompleted batch",
        f"{len(results)} operations, elapsed={ms(elapsed)}",
    )


# ------------------------------------------------------------
# 25. AsCompleted should preserve arbitrary result types
# ------------------------------------------------------------

def test_as_completed_value_types() -> Generator[Any, Any]:
    marker = object()
    dictionary = {"value": 42}
    sequence = [1, 2, 3]

    operations = [
        ToThread(delayed_identity, 0.01, marker),
        ToThread(delayed_identity, 0.02, dictionary),
        ToThread(delayed_identity, 0.03, sequence),
    ]

    results = list((yield AsCompleted(*operations)))

    check(
        len(results) == 3,
        f"Expected 3 results, received {len(results)}",
    )
    check(
        any(result is marker for result in results),
        "Object identity was not preserved",
    )
    check(
        any(result is dictionary for result in results),
        "Dictionary identity was not preserved",
    )
    check(
        any(result is sequence for result in results),
        "List identity was not preserved",
    )

    report("AsCompleted result types", "object identity and mutable values preserved")


# ------------------------------------------------------------
# 26. Callable child Task return value
# ------------------------------------------------------------

def test_child_callable_result() -> Generator[Any, Any]:
    result = yield Task(lambda: 42)

    check(
        result == 42,
        f"Unexpected child callable result: {result!r}",
    )

    report("callable child Task return", f"result={result}")


# ------------------------------------------------------------
# 27. Generator child Task return value
# ------------------------------------------------------------

def returning_generator(value: int) -> Generator[Any, Any, Any]:
    yield Sleep(0.005)
    return value


def test_child_generator_result() -> Generator[Any, Any]:
    result = yield Task(returning_generator(123))

    check(
        result == 123,
        f"Unexpected generator child result: {result!r}",
    )

    report("generator child Task return", f"result={result}")


# ------------------------------------------------------------
# 28. Nested child Task return propagation
# ------------------------------------------------------------

def nested_leaf() -> Generator[Any, Any, int]:
    yield Sleep(0.005)
    return 6


def nested_middle() -> Generator[Any, Any, int]:
    value = yield Task(nested_leaf())
    return value * 7


def test_nested_child_result() -> Generator[Any, Any]:
    result = yield Task(nested_middle())

    check(
        result == 42,
        f"Unexpected nested child result: {result!r}",
    )

    report("nested child Task returns", f"result={result}")


# ------------------------------------------------------------
# 29. Task positional and keyword argument forwarding
# ------------------------------------------------------------

def task_argument_function(first: str, second: str, *, separator: str) -> str:
    return f"{first}{separator}{second}"


def test_task_argument_forwarding() -> Generator[Any, Any]:
    result = yield Task(
        task_argument_function,
        "left",
        "right",
        separator="::",
    )

    check(
        result == "left::right",
        f"Unexpected forwarded arguments: {result!r}",
    )

    report("Task argument forwarding", f"result={result}")


# ------------------------------------------------------------
# 30. Yieldable synchronous callable
# ------------------------------------------------------------

def yieldable_sync_function(value: int, *, multiplier: int) -> int:
    return value * multiplier


def test_yieldable_callable() -> Generator[Any, Any]:
    wrapped = Yieldable(yieldable_sync_function)

    result = yield wrapped(9, multiplier=5)

    check(
        result == 45,
        f"Unexpected Yieldable callable result: {result!r}",
    )

    report("Yieldable synchronous callable", f"result={result}")


# ------------------------------------------------------------
# 31. Yieldable generator function arguments
# ------------------------------------------------------------

def yieldable_generator_function(value: int, *, increment: int) -> Generator[Any, Any, int]:
    yield Sleep(0.005)
    return value + increment


def test_yieldable_generator_arguments() -> Generator[Any, Any]:
    wrapped = Yieldable(yieldable_generator_function)

    result = yield wrapped(10, increment=5)

    check(
        result == 15,
        f"Unexpected Yieldable generator result: {result!r}",
    )

    report("Yieldable generator arguments", f"result={result}")


# ------------------------------------------------------------
# 32. Plain yielded values should resume the generator
# ------------------------------------------------------------

def test_plain_yield_resumption() -> Generator[Any, Any]:
    events: list[tuple[str, float]] = []

    def worker() -> Generator[Any, Any]:
        for index in range(5):
            events.append(("before", index))
            yield None
            events.append(("after", index))

    task = yield Task(worker(), background=True)

    while not task.done:
        yield Sleep(0.001)

    expected = [
        event
        for index in range(5)
        for event in (("before", index), ("after", index))
    ]

    check(
        events == expected,
        f"Unexpected generator execution order: {events!r}",
    )

    report("plain-value yield resumption", "5 yields resumed in order")


# ------------------------------------------------------------
# 33. Generator finally block on natural completion
# ------------------------------------------------------------

def test_natural_generator_cleanup() -> Generator[Any, Any]:
    events: list[str] = []

    def worker() -> Generator[Any, Any]:
        try:
            yield Sleep(0.005)
            yield None
        finally:
            events.append("finally")

    task = yield Task(worker(), background=True)

    while not task.done:
        yield Sleep(0.001)

    check(
        events == ["finally"],
        f"Unexpected cleanup events: {events!r}",
    )

    report("natural generator cleanup", "finally block executed once")


# ------------------------------------------------------------
# 34. Background task should not block its parent
# ------------------------------------------------------------

def delayed_background_worker(events: list[str]) -> Generator[Any, Any]:
    events.append("started")
    yield Sleep(0.05)
    events.append("finished")


def test_background_parent_resumption() -> Generator[Any, Any]:
    events: list[str] = []

    start = time.monotonic()

    task = yield Task(
        delayed_background_worker(events),
        background=True,
    )

    elapsed = time.monotonic() - start

    check(
        elapsed < 0.08,
        f"Parent waited for background task: {ms(elapsed)}",
    )

    yield Sleep(0.08)

    check(
        task.done,
        "Background task did not finish",
    )
    check(
        events == ["started", "finished"],
        f"Unexpected background events: {events!r}",
    )

    report(
        "background parent resumption",
        f"parent resumed in {ms(elapsed)}",
    )


# ------------------------------------------------------------
# 35. Multiple independent background workers
# ------------------------------------------------------------

def background_worker(index: int, observations: list[tuple[int, float]]) -> Generator[Any, Any]:
    for iteration in range(3):
        yield Sleep(0.006)
        observations.append((index, iteration))


def test_multiple_background_workers() -> Generator[Any, Any]:
    count = 6
    observations: list[tuple[int, float]] = []

    for index in range(count):
        yield Task(
            background_worker(index, observations),
            background=True,
        )

    yield Sleep(0.10)

    check(
        len(observations) == count * 3,
        f"Expected {count * 3} events, got {len(observations)}",
    )

    for index in range(count):
        iterations = [
            iteration
            for worker_index, iteration in observations
            if worker_index == index
        ]

        check(
            iterations == [0, 1, 2],
            f"Worker {index} has unexpected events: {iterations}",
        )

    report(
        "multiple background workers",
        f"{count} workers, {len(observations)} events",
    )


# ------------------------------------------------------------
# 36. High task count
# ------------------------------------------------------------

def test_high_task_count() -> Generator[Any, Any]:
    count = 96
    observations: list[tuple[int, float]] = []

    start: float = time.monotonic()

    for index in range(count):
        yield Task(
            short_background_task(index, observations),
            background=True,
        )

    yield Sleep(0.10)

    check(
        len(observations) == count,
        f"Only {len(observations)}/{count} tasks completed",
    )

    completion_times = [
        timestamp
        for _, timestamp in observations
    ]

    elapsed = max(completion_times) - start

    check(
        elapsed < 0.30,
        f"High task-count workload took too long: {ms(elapsed)}",
    )

    report(
        "high task count",
        f"{count} background tasks completed in {ms(elapsed)}",
    )


# ------------------------------------------------------------
# 37. Mixed thread and timer workload
# ------------------------------------------------------------

def test_mixed_thread_timer_load() -> Generator[Any, Any]:
    thread_observations: list[float] = []
    timer_observations: list[float] = []

    start: float = time.monotonic()

    for delay in (0.04, 0.05, 0.06, 0.07):
        yield Task(
            finite_thread_task(delay, thread_observations),
            background=True,
        )

    timer_task = yield Task(
        finite_ticker(5, 0.012, timer_observations),
        background=True,
    )

    yield Sleep(0.12)

    check(
        len(thread_observations) == 4,
        f"Only {len(thread_observations)}/4 thread jobs completed",
    )
    check(
        len(timer_observations) == 5,
        f"Only {len(timer_observations)}/5 timer ticks completed",
    )
    check(
        timer_task.done,
        "Finite timer task did not finish",
    )

    elapsed = max(
        max(thread_observations),
        max(timer_observations),
    ) - start

    overhead = elapsed - 0.07

    record_metric(
        "mixed thread/timer workload",
        overhead,
        target=0,
        limit=0.002,
    )

    report(
        "mixed thread and timer workload",
        f"4 thread jobs, 5 timer ticks, elapsed={ms(elapsed)}",
    )


# ============================================================
# Main
# ============================================================

def main() -> Generator[Any, Any]:
    PASSED.clear()
    METRICS.clear()

    clock = time.get_clock_info("monotonic")

    print("Timer, scheduler and event-loop test suite")
    print(f"Clock resolution: {clock.resolution * 1e9:.3f} ns")
    print()
    from .loop import _loop
    if _loop:
        print(f"Scheduler: {_loop.scheduler.__class__.__name__}")
        print()

    tests = (
        # Original tests
        test_sleep_edges,
        test_sleep_accuracy,
        test_periodic_timing,
        test_multiple_blockers,
        test_concurrent_timers,
        test_thread_wakeups,
        test_socket_events,
        test_as_completed,
        test_thread_exception,
        test_as_completed_exception,
        test_background_stop,
        test_ready_queue_fairness,

        # Additional tests
        test_sleep_deadline_order,
        test_sleep_fanout,
        test_mixed_message_blockers,
        test_socket_writable,
        test_socket_payload,
        test_socket_eof,
        test_socket_multiple_pairs,
        test_thread_return_value,
        test_thread_keyword_arguments,
        test_thread_parallelism,
        test_as_completed_first_result,
        test_as_completed_batch,
        test_as_completed_value_types,
        test_child_callable_result,
        test_child_generator_result,
        test_nested_child_result,
        test_task_argument_forwarding,
        test_yieldable_callable,
        test_yieldable_generator_arguments,
        test_plain_yield_resumption,
        test_natural_generator_cleanup,
        test_background_parent_resumption,
        test_multiple_background_workers,
        test_high_task_count,
        test_mixed_thread_timer_load,
    )

    for test in tests:
        yield Task(test())

    print()
    print(f"Completed {len(PASSED)}/{len(tests)} test groups.")

    check(
        len(PASSED) == len(tests),
        f"Only {len(PASSED)}/{len(tests)} test groups passed",
    )

    print("All tests passed.")
    print_score()

from .scheduler import EEVDFScheduler

if __name__ == "__main__":
    ExecutionLoop(EEVDFScheduler).run(Yieldable(main))
