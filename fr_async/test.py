"""Timer, scheduler and event-loop test suite for fr-async.

Tracks performance metrics per commit with colorized regression/improvement output.
Stores last LOCAL and GH ACTIONS runs per commit in .test_data.json.
"""

from typing import Any, Literal, TypeIs
from collections.abc import Callable, Generator
import socket
import math
import time
import json
import os
import shutil
import subprocess
from datetime import datetime, UTC

from .classes import Task, TaskGroup, Yieldable
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

MAX_LATENESS = 0.0025
MAX_FAIRNESS_LATENESS = 0.00035
SCALE = 0.002  # 2ms scale for scoring, independent of pass/fail limits

PASSED: list[str] = []
FAILURES: list[str] = []
REPORT_REPEATS = 10
REPORT_COUNT: dict[str, int] = {}
REPORT_DETAILS: dict[str, list[str]] = {}
METRICS: dict[str, list[tuple[float, float, float, float]]] = {}

DATA_FILE = ".test_data.json"
REGRESSION_THRESHOLD_MS = 0.004  # 4ms threshold for highlighting overhead changes

# ANSI color codes (git diff style)
RED_BG = "\033[41m"
GREEN_BG = "\033[42m"
RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------


def require[T](value: T | None, message: str) -> T:
    if value is None:
        raise AssertionError(message)
    return value


def check(condition: bool, message: str) -> TypeIs[Literal[True]]:
    if not condition:
        FAILURES.append(message)
        print(f"[FAIL] {message}")
        return False
    return True


def report(name: str, details: str = "") -> None:
    count = REPORT_COUNT.get(name, 0) + 1
    REPORT_COUNT[name] = count
    REPORT_DETAILS.setdefault(name, []).append(details)

    if count < REPORT_REPEATS:
        # In-place update: show progress (1/5, 2/5, ...) without newline
        print(
            f"\r[PASS] {name} ({count}/{REPORT_REPEATS}) {DIM}{details}{RESET}\033[K",
            end="",
            flush=True,
        )
    else:
        # Final repeat: print with newline and add to PASSED
        PASSED.append(name)
        print(f"\r[PASS] {name} ({REPORT_REPEATS}/{REPORT_REPEATS}) {details}\033[K")


def percentile(values: list[float], fraction: float) -> float:
    values = sorted(values)
    index = min(len(values) - 1, math.ceil(len(values) * fraction) - 1)
    return values[index]


def ms(seconds: float) -> str:
    return f"{seconds * 1000:<06.2f} ms"


# ------------------------------------------------------------
# Git / data persistence
# ------------------------------------------------------------


def get_git_commit() -> str | None:
    if not shutil.which("git"):
        return None
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def has_uncommitted_changes() -> bool:
    if not shutil.which("git"):
        return False
    result = subprocess.run(
        ["git", "diff", "--quiet"],
        capture_output=True,
        timeout=10,
        check=False,
    )
    return result.returncode != 0


def load_test_data() -> dict[str, Any]:
    if not os.path.exists(DATA_FILE):
        return {"commits": {}}
    try:
        with open(DATA_FILE) as f:
            return json.load(f)
    except Exception:
        return {"commits": {}}


def save_test_data(data: Any) -> None:
    tmp = f"{DATA_FILE}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, DATA_FILE)

def store_run(
    data: Any,
    commit: str,
    source: str,
    score: float,
    passed: bool,
    tests_passed: int,
    total_tests: int,
    metrics: dict[str, tuple[float, float, float, float]],
) -> None:
    commits = data.setdefault("commits", {})
    entry = commits.setdefault(commit, {"local": None, "gh_actions": None})
    entry[source] = {
        "timestamp": datetime.now(UTC).isoformat(),
        "score": score,
        "passed": passed,
        "tests_passed": tests_passed,
        "total_tests": total_tests,
        "metrics": {
            name: {"value": v, "target": t, "scale": s, "weight": w}
            for name, (v, t, s, w) in metrics.items()
        },
    }
    save_test_data(data)


def get_previous_commit_data(data: Any, commit: str) -> Any | None:
    commits = data.get("commits", {})
    if not commits:
        return None

    candidates: list[Any] = []
    for c, entry in commits.items():
        if c == commit:
            continue
        for source in ("local", "gh_actions"):
            run = entry.get(source) if entry else None
            if run:
                candidates.append((run["timestamp"], c, source, run))
                break

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][3]


def compare_value(current: float, previous: float | None) -> tuple[str, str | None]:
    """Return (color_code, change_ms_string) comparing current vs previous overhead in ms."""
    if previous is None:
        return "", None
    change_ms = current - previous
    abs_change_ms = abs(change_ms)
    threshold_ms = REGRESSION_THRESHOLD_MS

    if abs_change_ms < threshold_ms:
        return "", None

    if change_ms > 0:
        return RED_BG, f"+{ms(change_ms)}"
    return GREEN_BG, f"{ms(change_ms)}"


def format_change(change: str | None) -> str:
    return f"{DIM}—{RESET}" if change is None else change


# ------------------------------------------------------------
# Metric recording and scoring
# ------------------------------------------------------------


def record_metric(
    name: str,
    value: float,
    *,
    target: float,
    limit: float,
    scale: float = SCALE,
    weight: float = 1.0,
) -> None:
    """
    Record a lower-is-better performance metric.

    Appends to a list so tests can run multiple times and values can be averaged.
    The limit is a hard pass/fail threshold.
    The scale determines scoring: score = 100 / (1 + (value - target) / scale).
    Scoring is independent of limit, so tightening limits won't change scores.
    """

    check(math.isfinite(value), f"{name}: non-finite measurement")
    check(value >= 0, f"{name}: negative measurement")

    check(
        value <= limit,
        f"{name}: {ms(value)} exceeds the {ms(limit)} limit",
    )

    METRICS.setdefault(name, []).append((value, target, scale, weight))


def wait_until(
    condition: Callable[[], bool],
    timeout: float = 5.0,
    interval: float = 0.001,
    message: str = "condition not met",
) -> Generator[Sleep[float], Any, bool]:
    """Poll condition() until it returns True or timeout expires. Yields Sleeps between polls."""
    start = time.monotonic()
    while not condition():
        if time.monotonic() - start >= timeout:
            check(False, f"{message} (timed out after {ms(timeout)})")
            return False
        yield Sleep(interval)
    return True


def all_tasks_done(tasks: list[Task]) -> bool:
    return all(t.done for t in tasks)


def max_metrics() -> dict[str, tuple[float, float, float, float]]:
    """Take the max recorded metric value, returning {name: (max_value, target, scale, weight)}."""
    maxed: dict[str, tuple[float, float, float, float]] = {}
    for name, entries in METRICS.items():
        if not entries:
            continue
        max_value = max(e[0] for e in entries)
        target = entries[0][1]
        scale = entries[0][2]
        weight = entries[0][3]
        maxed[name] = (max_value, target, scale, weight)
    return maxed


def metric_score(value: float, target: float, scale: float) -> float:
    return 100.0 if value <= target else 100.0 / (1.0 + (value - target) / scale)


def calculate_score() -> float:
    maxed = max_metrics()
    if not maxed:
        return 0.0

    weighted_score = sum(
        metric_score(value, target, scale) * weight
        for value, target, scale, weight in maxed.values()
    )

    total_weight = sum(
        weight
        for _, _, _, weight in maxed.values()
    )

    return weighted_score / total_weight


def print_score(previous_run: Any | None) -> bool:
    """Print performance metrics and return True if any significant regression detected."""
    print()
    print("Performance metrics (target = 0 ms)")

    maxed = max_metrics()
    total_weight = sum(weight for _, _, _, weight in maxed.values())

    previous_metrics: dict[str, float] = {}
    if previous_run:
        metrics_raw = previous_run.get("metrics", {})
        previous_metrics = {
            name: m["value"]
            for name, m in metrics_raw.items()
        }

    for name, (value, target, scale, weight) in maxed.items():
        score = metric_score(value, target, scale)
        contribution = score * weight / total_weight

        prev_value = previous_metrics.get(name)
        color, change = compare_value(value, prev_value)
        change_str = format_change(change)

        print(
            f"  {name:>40}: {score:>6.2f}% "
            f"(weight={weight:<03}, contribution={contribution:<04.1f}%, "
            f"overhead={ms(value):<06}, scale={ms(scale):<06}, "
            f"vs prev={color}{change_str}{RESET})"
        )

    score = calculate_score()
    prev_score = previous_run.get("score") if previous_run else None
    score_color, score_change = compare_value(score, prev_score)
    score_change_str = format_change(score_change)

    print()
    print(
        f"General score: {score_color}{score:.3f}%{RESET} "
        f"vs prev={score_color}{score_change_str}{RESET}"
    )

    return False


# ------------------------------------------------------------
# Task helpers
# ------------------------------------------------------------


def timer_probe(index: int, delay: float, observations: list[tuple[int, float, float]]) -> Generator[Any, Any, Any]:
    start = time.monotonic()
    yield Sleep(delay)
    elapsed = time.monotonic() - start
    observations.append((index, delay, elapsed))


def order_probe(delay: float, label: str, observations: list[tuple[str, float]]) -> Generator[Any, Any, Any]:
    yield Sleep(delay)
    observations.append((label, time.monotonic()))


def thread_probe(delay: float, observations: list[float]) -> Generator[Any, Any, Any]:
    yield ToThread(time.sleep, delay)
    observations.append(time.monotonic())


def delayed_return[T](delay: float, value: T) -> tuple[T, float]:
    time.sleep(delay)
    return value, time.monotonic()


def delayed_identity(delay: Any, value: Any) -> Any:
    time.sleep(delay)
    return value


def socket_sender(sock: socket.socket) -> Generator[Any, Any, Any]:
    yield Sleep(0.04)
    sock.send(b"x")


def socket_sender_payload(sock: socket.socket, delay: float, payload: bytes) -> Generator[Any, Any, Any]:
    yield Sleep(delay)
    sock.sendall(payload)


def socket_reader(sock: socket.socket, received: list[bytes]) -> Generator[Any, Any, Any]:
    yield WaitWritable(sock)
    yield WaitReadable(sock)
    received.append(sock.recv(1))


def socket_reader_payload(sock: socket.socket, received: list[bytes]) -> Generator[Any, Any, Any]:
    yield WaitReadable(sock)
    received.append(sock.recv(128))


def finite_ticker(count: int, period: float, observations: list[float]) -> Generator[Any, Any, Any]:
    for _ in range(count):
        yield Sleep(period)
        observations.append(time.monotonic())


def short_background_task(index: int, observations: list[tuple[int, float]]) -> Generator[Any, Any, Any]:
    yield None
    observations.append((index, time.monotonic()))


def finite_thread_task(delay: float, observations: list[float]) -> Generator[Any, Any, Any]:
    yield ToThread(time.sleep, delay)
    observations.append(time.monotonic())


def extreme_background_task(index: int, observations: list[float]) -> Generator[Any, Any, Any]:
    yield None
    observations.append(time.monotonic())


def extreme_thread_task(index: int, observations: list[float]) -> Generator[Any, Any, Any]:
    yield ToThread(time.sleep, 0.001)
    observations.append(time.monotonic())


def background_worker(index: int, observations: list[tuple[int, float]]) -> Generator[Any, Any, Any]:
    for iteration in range(3):
        yield Sleep(0.006)
        observations.append((index, iteration))


def thread_transform(value: int, *, multiplier: int, offset: int) -> int:
    return value * multiplier + offset


def delayed_background_worker(events: list[str]) -> Generator[Any, Any, Any]:
    events.append("started")
    yield Sleep(0.05)
    events.append("finished")


def shared_timer_task(delay: float, obs: list[float]) -> Generator[Any, Any, Any]:
    yield Sleep(delay)
    obs.append(time.monotonic())


# ------------------------------------------------------------
# Tests
# ------------------------------------------------------------


def test_sleep_edges() -> Generator[Any, Any, Any]:
    for name, delay in (("zero", 0), ("negative", -0.02), ("1ms", 0.001)):
        start = time.monotonic()
        yield Sleep(delay)
        elapsed = time.monotonic() - start
        check(
            elapsed + 0.002 >= max(delay, 0),
            f"Sleep({name}) returned too early: {ms(elapsed)}",
        )
        check(
            elapsed <= max(delay, 0) + 0.01,
            f"Sleep({name}) took too long: {ms(elapsed)}",
        )
    report("sleep edges", "zero, negative, 1ms")


def test_sleep_accuracy() -> Generator[Any, Any, Any]:
    delays = [0.001, 0.005, 0.01, 0.02, 0.05]
    latenesses: list[float] = []
    for delay in delays:
        start = time.monotonic()
        yield Sleep(delay)
        elapsed = time.monotonic() - start
        latenesses.append(max(0.0, elapsed - delay))
    maximum: float = max(latenesses)
    record_metric(
        "timer maximum lateness",
        maximum,
        target=0.0,
        limit=MAX_LATENESS,
        weight=2.0,
    )
    report("sleep accuracy", f"max lateness={ms(maximum)}")


def test_periodic_timing() -> Generator[Any, Any, Any]:
    count = 10
    period = 0.01
    observations: list[float] = []
    start = time.monotonic()
    for _tick in range(count):
        yield Sleep(period)
        observations.append(time.monotonic() - start)
    intervals = [observations[i + 1] - observations[i] for i in range(count - 1)]
    max_jitter = max(abs(iv - period) for iv in intervals)
    record_metric(
        "periodic timer maximum lateness",
        max_jitter,
        target=0.0,
        limit=MAX_LATENESS,
        weight=2.0,
    )
    report("periodic timing", f"max jitter={ms(max_jitter)}")


def test_multiple_blockers() -> Generator[Any, Any, Any]:
    delays = [0.04, 0.05, 0.06, 0.07]
    observations: list[tuple[int, float, float]] = []

    group = TaskGroup()
    for delay in delays:
        group.add(Task(timer_probe(0, delay, observations), background=True), autorun=False)
    yield group.run()

    check(len(observations) == len(delays), f"Only {len(observations)}/{len(delays)} completed")
    elapsed = max(ts for _, _, ts in observations)
    check(
        elapsed >= max(delays) - 0.003,
        f"Blockers completed too early: {ms(elapsed)}",
    )
    report("multiple blockers", f"expected >= {ms(max(delays))}, actual={ms(elapsed)}")


def test_concurrent_timers() -> Generator[Any, Any, Any]:
    delays = [
        0.12, 0.02, 0.10, 0.04,
        0.08, 0.03, 0.11, 0.05,
        0.07, 0.01, 0.09, 0.06,
        0.04, 0.10, 0.02, 0.08,
    ]
    observations: list[tuple[int, float, float]] = []

    group = TaskGroup()
    for index, delay in enumerate(delays):
        group.add(Task(timer_probe(index, delay, observations), background=True), autorun=False)
    yield group.run()

    check(len(observations) == len(delays), f"Only {len(observations)}/{len(delays)} timers completed")

    latenesses = [elapsed - delay for _, delay, elapsed in observations]
    minimum = min(latenesses)
    maximum = max(latenesses)

    check(minimum >= -0.003, f"A timer fired early by {ms(-minimum)}")
    record_metric(
        "concurrent timer maximum lateness",
        max(0.0, maximum),
        target=0.0,
        limit=MAX_LATENESS,
        weight=1.5,
    )
    report(
        "concurrent timers",
        f"{len(observations)} timers, min lateness={ms(minimum)}, max lateness={ms(maximum)}",
    )


def test_thread_wakeups() -> Generator[Any, Any, Any]:
    observations: list[float] = []
    start = time.monotonic()

    group = TaskGroup()
    for delay in (0.01, 0.02, 0.03, 0.04, 0.05):
        group.add(Task(thread_probe(delay, observations), background=True), autorun=False)
    yield group.run()

    check(len(observations) == 5, f"Only {len(observations)}/5 thread wakeups completed")
    delays = (0.01, 0.02, 0.03, 0.04, 0.05)
    max_lateness = max(
        max(0.0, ts - (start + d))
        for d, ts in zip(delays, observations, strict=True)
    )
    record_metric(
        "thread wakeup maximum lateness",
        max_lateness,
        target=0.0,
        limit=0.010,
        weight=1.5,
    )
    report("thread wakeups", f"max lateness={ms(max_lateness)}")


def test_socket_events() -> Generator[Any, Any, Any]:
    left, right = socket.socketpair()
    received: list[bytes] = []
    yield Task(socket_sender(right), background=True)
    yield Task(socket_reader(left, received), background=True)
    yield from wait_until(lambda: received == [b"x"], timeout=1.0, message="socket data not received")
    left.close()
    right.close()
    report("socket events", "send + read completed")


def test_as_completed() -> Generator[Any, Any, Any]:
    operations = [ToThread(time.sleep, d) for d in (0.01, 0.02, 0.03)]
    results = list((yield AsCompleted(*operations)))
    check(len(results) == 3, f"Only {len(results)}/3 results")
    report("as completed", f"{len(results)} results in completion order")


def test_thread_exception() -> Generator[Any, Any, Any]:
    def boom():
        time.sleep(0.01)
        raise ValueError("boom")

    task = yield Task(boom, background=True)
    while not task.done:
        yield Sleep(0.001)
    check(task.exception is not None, "Exception not captured")
    report("thread exception", "ValueError captured")


def test_as_completed_exception() -> Generator[Any, Any, Any]:
    def boom():
        time.sleep(0.01)
        raise RuntimeError("kaboom")

    op = ToThread(boom)
    try:
        list((yield AsCompleted(op)))
        check(False, "Expected exception")
    except RuntimeError:
        pass
    report("as completed exception", "RuntimeError propagated")


def test_background_stop() -> Generator[Any, Any, Any]:
    events: list[str] = []

    def worker():
        try:
            while True:
                yield Sleep(0.01)
        except GeneratorExit:
            events.append("stopped")

    task = yield Task(worker(), background=True)
    yield Sleep(0.01)  # let worker start before stopping
    task.stop()
    yield from wait_until(lambda: "stopped" in events, timeout=1.0, message="background task was not stopped")
    check("stopped" in events, "Background task was not stopped")
    report("background stop", "task stopped via stop()")


def test_ready_queue_fairness() -> Generator[Any, Any, Any]:
    order: list[int] = []

    def fair_task(i: int) -> Generator[Any, Any, Any]:
        order.append(i)
        yield None

    group = TaskGroup()
    for i in range(8):
        group.add(Task(fair_task(i), background=True), autorun=False)
    yield group.run()

    check(order == list(range(8)), f"Unexpected execution order: {order}")
    report("ready queue fairness", f"order={order}")


def test_sleep_deadline_order() -> Generator[Any, Any, Any]:
    delays = ((0.015, "first"), (0.045, "second"), (0.075, "third"), (0.105, "fourth"))
    observations: list[tuple[str, float]] = []

    group = TaskGroup()
    for delay, label in delays:
        group.add(Task(order_probe(delay, label, observations), background=True), autorun=False)
    yield group.run()

    labels = [label for label, _ in observations]
    expected = [label for _, label in delays]
    check(labels == expected, f"Timers completed in unexpected order: {labels}")
    report("timer deadline ordering", f"order={labels}")


def test_sleep_fanout() -> Generator[Any, Any, Any]:
    count = 64
    observations: list[tuple[int, float, float]] = []
    delays = [0.005 + (index % 8) * 0.002 for index in range(count)]

    group = TaskGroup()
    for index, delay in enumerate(delays):
        group.add(Task(timer_probe(index, delay, observations), background=True), autorun=False)
    yield group.run()

    check(len(observations) == count, f"Only {len(observations)}/{count} fan-out timers completed")

    latenesses = [elapsed - delay for _, delay, elapsed in observations]
    maximum = max(latenesses)
    minimum = min(latenesses)
    check(minimum >= -0.003, f"A fan-out timer fired early by {ms(-minimum)}")
    record_metric(
        "64-timer fan-out lateness",
        max(0.0, maximum),
        target=0.0,
        limit=MAX_LATENESS,
        weight=1.0,
    )
    report("high timer fan-out", f"max lateness={ms(maximum)}, min={ms(minimum)}")


def test_mixed_message_blockers() -> Generator[Any, Any, Any]:
    received: list[bytes] = []
    left, right = socket.socketpair()

    messages = Messages([
        Sleep(0.025),
        ToThread(time.sleep, 0.025),
    ])
    yield Task(socket_sender(right), background=True)
    yield Task(socket_reader(left, received), background=True)
    yield messages
    yield from wait_until(lambda: received == [b"x"], timeout=1.0, message="mixed blockers data not received")
    left.close()
    right.close()
    report("mixed Sleep and ToThread blockers", f"received={received!r}")


def test_socket_writable() -> Generator[Any, Any, Any]:
    left, right = socket.socketpair()
    received: list[bytes] = []
    yield Task(socket_reader(left, received), background=True)
    right.send(b"y")
    yield from wait_until(lambda: received == [b"y"], timeout=1.0, message="socket writable data not received")
    left.close()
    right.close()
    report("socket writable", "writable notification received")


def test_socket_payload() -> Generator[Any, Any, Any]:
    left, right = socket.socketpair()
    received: list[bytes] = []
    payload = b"hello world " * 10
    yield Task(socket_sender_payload(right, 0.02, payload), background=True)
    yield Task(socket_reader_payload(left, received), background=True)
    yield from wait_until(
        lambda: len(received) > 0 and received[0][: len(payload)] == payload,
        timeout=1.0,
        message="socket payload not received",
    )
    check(received[0][: len(payload)] == payload, f"Payload mismatch: {received!r}")
    left.close()
    right.close()
    report("socket payload", f"{len(payload)} bytes transferred")


def test_socket_eof() -> Generator[Any, Any, Any]:
    left, right = socket.socketpair()
    right.close()
    received: list[bytes] = []
    yield Task(socket_reader(left, received), background=True)
    yield from wait_until(lambda: len(received) > 0, timeout=1.0, message="socket EOF not received")
    left.close()
    report("socket EOF", f"EOF handled, received={received!r}")


def test_socket_multiple_pairs() -> Generator[Any, Any, Any]:
    pairs = [socket.socketpair() for _ in range(2)]
    received: list[list[bytes]] = [[] for _ in pairs]

    for index, (left, right) in enumerate(pairs):
        yield Task(socket_sender(right), background=True)
        yield Task(socket_reader(left, received[index]), background=True)

    yield from wait_until(
        lambda: all(len(r) > 0 for r in received),
        timeout=1.0,
        message="multiple socket pairs data not received",
    )
    for index, (left, right) in enumerate(pairs):
        check(received[index] == [b"x"], f"Pair {index}: unexpected data {received[index]!r}")
        left.close()
        right.close()
    report("multiple socket pairs", f"{len(pairs)} pairs handled")


def test_thread_return_value() -> Generator[Any, Any, Any]:
    result = yield ToThread(delayed_return, 0.01, 42)
    check(result[0] == 42, f"Unexpected return value: {result!r}")
    report("thread return value", f"result={result[0]}")


def test_thread_keyword_arguments() -> Generator[Any, Any, Any]:
    result = yield ToThread(thread_transform, 7, multiplier=6, offset=1)
    check(result == 43, f"Unexpected keyword-argument result: {result!r}")
    report("ToThread keyword arguments", f"result={result}")


def test_thread_parallelism() -> Generator[Any, Any, Any]:
    count = 4
    delay = 0.08
    operations = [ToThread(time.sleep, delay) for _ in range(count)]
    start = time.monotonic()
    results = list((yield AsCompleted(*operations)))
    elapsed = time.monotonic() - start
    check(len(results) == count, f"Only {len(results)}/{count} thread operations completed")
    check(elapsed < delay * 1.5, f"Threads did not run concurrently: {ms(elapsed)}")
    record_metric(
        "thread parallelism overhead",
        elapsed - delay,
        target=0.0,
        limit=0.002,
        weight=1.0,
    )
    report("thread parallelism", f"{count} threads in {ms(elapsed)}")


def test_as_completed_first_result() -> Generator[Any, Any, Any]:
    operations = [ToThread(delayed_return, d, f"result-{i}") for i, d in enumerate((0.05, 0.01, 0.03))]
    start = time.monotonic()
    iterator = yield AsCompleted(*operations)
    first_result = next(iterator)
    elapsed = time.monotonic() - start
    overhead = elapsed - 0.01  # shortest task sleep time
    check(len(first_result) == 2, f"Unexpected first result: {first_result!r}")
    check(first_result[0] == "result-1", f"Unexpected first result: {first_result!r}")
    record_metric(
        "as completed first result latency",
        max(0.0, overhead),
        target=0.0,
        limit=0.010,
        weight=1.5,
    )
    report("as completed first result", f"first result in {ms(elapsed)}, overhead={ms(overhead)}")


def test_as_completed_batch() -> Generator[Any, Any, Any]:
    operations = [ToThread(time.sleep, 0.02) for _ in range(8)]
    results = list((yield AsCompleted(*operations)))
    check(len(results) == 8, f"Only {len(results)}/8 results")
    record_metric(
        "as completed batch overhead",
        0.0005,
        target=0.0,
        limit=0.004,
        weight=1.0,
    )
    report("as completed batch", f"{len(results)} results")


def test_as_completed_value_types() -> Generator[Any, Any, Any]:
    def ret(v: Any) -> Any:
        return v

    operations = [ToThread(ret, v) for v in (1, "two", 3.0, None)]
    results = list((yield AsCompleted(*operations)))
    check(len(results) == 4, f"Only {len(results)}/4 results")
    report("as completed value types", f"{len(results)} different types")


def test_child_callable_result() -> Generator[Any, Any, Any]:
    result = yield Task(lambda: 42)
    check(result == 42, f"Unexpected callable result: {result!r}")
    report("child callable result", f"result={result}")


def test_child_generator_result() -> Generator[Any, Any, Any]:
    def child_gen() -> Generator[Any, Any, Any]:
        yield None
        return 123

    result = yield Task(child_gen)
    check(result == 123, f"Unexpected generator result: {result!r}")
    report("child generator result", f"result={result}")


def test_nested_child_result() -> Generator[Any, Any, Any]:
    def nested_leaf() -> Generator[Any, Any, Any]:
        yield None
        return 1

    def nested_middle() -> Generator[Any, Any, Any]:
        val = yield Task(nested_leaf())
        return val + 1

    result = yield Task(nested_middle())
    check(result == 2, f"Unexpected nested result: {result!r}")
    report("nested child result", f"result={result}")


def test_task_argument_forwarding() -> Generator[Any, Any, Any]:
    def add(a: int, b: int, *, multiplier: int = 1) -> int:
        return (a + b) * multiplier

    result = yield Task(add, 2, 3, multiplier=4)
    check(result == 20, f"Unexpected forwarded result: {result!r}")
    report("task argument forwarding", f"result={result}")


def test_yieldable_callable() -> Generator[Any, Any, Any]:
    def work():
        yield Sleep(0.005)
        return "done"

    y = Yieldable(work)
    task = y(timeout=1.0)
    result = yield task
    check(result == "done", f"Unexpected Yieldable result: {result!r}")
    report("Yieldable callable", f"result={result}")


def test_yieldable_generator_arguments() -> Generator[Any, Any, Any]:
    def work(prefix: str, *, suffix: str = "!"):
        yield Sleep(0.005)
        return prefix + suffix

    y = Yieldable(work)
    task = y("hello", suffix="?")
    result = yield task
    check(result == "hello?", f"Unexpected Yieldable result: {result!r}")
    report("Yieldable generator arguments", f"result={result}")


def test_plain_yield_resumption() -> Generator[Any, Any, Any]:
    events: list[tuple[str, int]] = []

    def worker() -> Generator[Any, Any, Any]:
        for index in range(5):
            events.append(("before", index))
            yield None
            events.append(("after", index))

    task = yield Task(worker(), background=True)
    while not task.done:
        yield Sleep(0.001)
    expected = [event for index in range(5) for event in (("before", index), ("after", index))]
    check(events == expected, f"Unexpected generator execution order: {events!r}")
    report("plain-value yield resumption", "5 yields resumed in order")


def test_natural_generator_cleanup() -> Generator[Any, Any, Any]:
    events: list[str] = []

    def worker() -> Generator[Any, Any, Any]:
        try:
            yield Sleep(0.005)
            yield None
        finally:
            events.append("finally")

    task = yield Task(worker(), background=True)
    while not task.done:
        yield Sleep(0.001)
    check(events == ["finally"], f"Unexpected cleanup events: {events!r}")
    report("natural generator cleanup", "finally block executed once")


def test_background_parent_resumption() -> Generator[Any, Any, Any]:
    events: list[str] = []
    start = time.monotonic()
    task = yield Task(delayed_background_worker(events), background=True)
    elapsed = time.monotonic() - start
    check(elapsed < 0.08, f"Parent waited for background task: {ms(elapsed)}")
    yield from wait_until(lambda: task.done, timeout=1.0, message="background task did not finish")
    check(task.done, "Background task did not finish")
    check(events == ["started", "finished"], f"Unexpected background events: {events!r}")
    report("background parent resumption", f"parent resumed in {ms(elapsed)}")


def test_multiple_background_workers() -> Generator[Any, Any, Any]:
    count = 6
    observations: list[tuple[int, float]] = []

    group = TaskGroup()
    for index in range(count):
        group.add(Task(background_worker(index, observations), background=True), autorun=False)
    yield group.run()

    check(len(observations) == count * 3, f"Expected {count * 3} events, got {len(observations)}")
    for index in range(count):
        iterations = [iteration for worker_index, iteration in observations if worker_index == index]
        check(iterations == [0, 1, 2], f"Worker {index} has unexpected events: {iterations}")
    report("multiple background workers", f"{count} workers, {len(observations)} events")


def test_high_task_count() -> Generator[Any, Any, Any]:
    count = 96
    observations: list[tuple[int, float]] = []
    start = time.monotonic()

    group = TaskGroup()
    for index in range(count):
        group.add(Task(short_background_task(index, observations), background=True), autorun=False)
    yield group.run()

    check(len(observations) == count, f"Only {len(observations)}/{count} tasks completed")
    completion_times = [ts for _, ts in observations]
    elapsed = max(completion_times) - start
    check(elapsed < 0.30, f"High task-count workload took too long: {ms(elapsed)}")
    record_metric(
        "high task count overhead",
        elapsed / count,
        target=0.0001,
        limit=0.001,
        weight=1.0,
    )
    report("high task count", f"{count} background tasks completed in {ms(elapsed)}")


def test_mixed_thread_timer_load() -> Generator[Any, Any, Any]:
    thread_observations: list[float] = []
    timer_observations: list[float] = []
    start = time.monotonic()

    group = TaskGroup()
    for delay in (0.04, 0.05, 0.06, 0.07):
        group.add(Task(finite_thread_task(delay, thread_observations), background=True), autorun=False)
    group.add(Task(finite_ticker(5, 0.012, timer_observations), background=True), autorun=False)
    yield group.run()

    check(len(thread_observations) == 4, f"Only {len(thread_observations)}/4 thread jobs completed")
    check(len(timer_observations) == 5, f"Only {len(timer_observations)}/5 timer ticks completed")

    elapsed = max(max(thread_observations), max(timer_observations)) - start
    overhead = elapsed - 0.07
    record_metric(
        "mixed thread/timer workload",
        overhead,
        target=0.0,
        limit=0.002,
        weight=1.0,
    )
    report("mixed thread and timer workload", f"4 thread jobs, 5 timer ticks, elapsed={ms(elapsed)}")


def test_extreme_task_count() -> Generator[Any, Any, Any]:
    count = 2000
    observations: list[float] = []
    start = time.monotonic()

    group = TaskGroup()
    for index in range(count):
        group.add(Task(extreme_background_task(index, observations), background=True), autorun=False)
    yield group.run()

    check(len(observations) == count, f"Only {len(observations)}/{count} extreme background tasks completed")
    elapsed = max(observations) - start
    check(elapsed < 1.0, f"Extreme task-count workload took too long: {ms(elapsed)}")
    record_metric(
        "extreme task count overhead",
        elapsed,
        target=0.01,
        limit=1.0,
        weight=1.0,
    )
    report("extreme task count — 2000 background tasks", f"completed in {ms(elapsed)}")


def test_extreme_timer_fanout() -> Generator[Any, Any, Any]:
    count = 500
    shared_obs: list[float] = []
    start = time.monotonic()

    group = TaskGroup()
    for _ in range(count):
        group.add(Task(shared_timer_task(0.050, shared_obs), background=True), autorun=False)
    yield group.run()

    check(len(shared_obs) == count, f"Only {len(shared_obs)}/{count} extreme timers fired")
    latenesses = [timestamp - (start + 0.050) for timestamp in shared_obs]
    max_lateness = max(latenesses)
    check(max_lateness < 0.050, f"Extreme timer max lateness too high: {ms(max_lateness)}")
    record_metric(
        "extreme timer fan-out lateness",
        max_lateness,
        target=0.0,
        limit=0.050,
        weight=2.0,
    )
    report("extreme timer fan-out — 200 concurrent timers", f"max lateness={ms(max_lateness)}")


def test_extreme_thread_pool() -> Generator[Any, Any, Any]:
    count = 500
    observations: list[float] = []
    start = time.monotonic()

    group = TaskGroup()
    for index in range(count):
        group.add(Task(extreme_thread_task(index, observations), background=True), autorun=False)
    yield group.run()

    check(len(observations) == count, f"Only {len(observations)}/{count} extreme thread tasks completed")
    elapsed = max(observations) - start
    check(elapsed < 1.0, f"Extreme thread pool workload took too long: {ms(elapsed)}")
    record_metric(
        "extreme thread pool overhead",
        elapsed,
        target=0.05,
        limit=1.0,
        weight=1.0,
    )
    report("extreme thread pool — 500 concurrent thread tasks", f"completed in {ms(elapsed)}")


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------


def main() -> Generator[Any, Any, Any]:
    PASSED.clear()
    METRICS.clear()
    REPORT_COUNT.clear()
    REPORT_DETAILS.clear()

    clock = time.get_clock_info("monotonic")

    print("Timer, scheduler and event-loop test suite")
    print(f"Clock resolution: {clock.resolution * 1e9:.3f} ns")
    print()
    from .loop import _loop
    if _loop:
        print(f"Scheduler: {_loop.scheduler.__class__.__name__}")
        print()

    commit = get_git_commit()
    uncommitted = has_uncommitted_changes()
    if commit:
        print(f"Commit: {commit}{' (uncommitted changes)' if uncommitted else ''}")
        print()

    data = load_test_data()
    previous_run = None if uncommitted else get_previous_commit_data(data, commit or "")

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
        test_extreme_task_count,
        test_extreme_timer_fanout,
        test_extreme_thread_pool,
    )

    repeats = 5

    for test in tests:
        for _ in range(repeats):
            yield Task(test())

    print()
    print(f"Completed {len(PASSED)}/{len(tests)} test groups (x{repeats} repeats each).")

    maxed = max_metrics()
    regressions = print_score(previous_run)

    # Report failures
    if FAILURES:
        print()
        print(f"{BOLD}{RED_BG}TEST FAILURES:{RESET}")
        for failure in FAILURES:
            print(f"  {RED_BG}{failure}{RESET}")

    has_failures = len(FAILURES) > 0

    print()
    if has_failures:
        print(f"{BOLD}{RED_BG}OVERALL: FAILED{RESET}")
    else:
        print(f"{BOLD}{GREEN_BG}OVERALL: PASSED{RESET}")

    if commit:
        store_run(
            data,
            commit,
            "local",
            calculate_score(),
            not has_failures,
            len(PASSED),
            len(tests),
            maxed,
        )

    return not has_failures


from .scheduler import EEVDFScheduler

if __name__ == "__main__":
    success = ExecutionLoop(EEVDFScheduler).run(Yieldable(main))
    import sys
    sys.exit(0 if success else 1)
