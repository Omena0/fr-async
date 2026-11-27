"""A small worker pool that grows with queued work and retires idle workers."""

from concurrent.futures import Future
from collections import deque
from threading import Condition, Thread, current_thread
from typing import Any, Callable
import os


class DynamicWorkerPool:
    """Run callables on a pool that scales between its minimum and maximum size."""

    __slots__ = (
        "min_workers", "max_workers", "idle_timeout", "_condition", "_work",
        "_threads", "_idle", "_active", "_shutdown", "_thread_name_prefix",
    )

    def __init__(
        self,
        *,
        min_workers: int = 1,
        max_workers: int | None = None,
        idle_timeout: float = 1.0,
        thread_name_prefix: str = "fr-async-worker",
    ) -> None:
        if max_workers is None:
            max_workers = max(1, os.process_cpu_count() or os.cpu_count() or 1)
        if min_workers < 0:
            raise ValueError("min_workers must be non-negative")
        if max_workers < 1 or min_workers > max_workers:
            raise ValueError("max_workers must be positive and at least min_workers")
        if idle_timeout <= 0:
            raise ValueError("idle_timeout must be positive")

        self.min_workers = min_workers
        self.max_workers = max_workers
        self.idle_timeout = idle_timeout
        self._condition = Condition()
        self._work: deque[tuple[Future[Any], Callable[..., Any], tuple[Any, ...]]] = deque()
        self._threads: set[Thread] = set()
        self._idle = 0
        self._active = 0
        self._shutdown = False
        self._thread_name_prefix = thread_name_prefix

        with self._condition:
            for _ in range(min_workers):
                self._start_worker_locked()

    @property
    def worker_count(self) -> int:
        with self._condition:
            return len(self._threads)

    @property
    def queued_count(self) -> int:
        with self._condition:
            return len(self._work)

    def submit(self, function: Callable[..., Any], /, *args: Any) -> Future[Any]:
        future: Future[Any] = Future()
        with self._condition:
            if self._shutdown:
                raise RuntimeError("cannot submit work after pool shutdown")

            self._work.append((future, function, args))
            desired = min(
                self.max_workers,
                max(self.min_workers, self._active + len(self._work)),
            )
            while len(self._threads) < desired:
                self._start_worker_locked()
            self._condition.notify_all()
        return future

    def _start_worker_locked(self) -> None:
        thread = Thread(
            target=self._worker,
            name=f"{self._thread_name_prefix}-{len(self._threads) + 1}",
            daemon=True,
        )
        self._threads.add(thread)
        thread.start()

    def _worker(self) -> None:
        thread = current_thread()
        while True:
            with self._condition:
                while not self._work and not self._shutdown:
                    self._idle += 1
                    self._condition.wait(self.idle_timeout)
                    self._idle -= 1
                    if (
                        not self._work
                        and not self._shutdown
                        and len(self._threads) > self.min_workers
                    ):
                        self._threads.discard(thread)
                        self._condition.notify_all()
                        return

                if self._shutdown and not self._work:
                    self._threads.discard(thread)
                    self._condition.notify_all()
                    return

                future, function, args = self._work.popleft()
                self._active += 1

            if not future.set_running_or_notify_cancel():
                with self._condition:
                    self._active -= 1
                continue

            try:
                result = function(*args)
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)
            finally:
                with self._condition:
                    self._active -= 1

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        with self._condition:
            if not self._shutdown:
                self._shutdown = True
                if cancel_futures:
                    while self._work:
                        future, _function, _args = self._work.popleft()
                        future.cancel()
                self._condition.notify_all()
            threads = tuple(self._threads)

        if wait:
            for thread in threads:
                if thread is not current_thread():
                    thread.join()
