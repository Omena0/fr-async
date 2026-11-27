from concurrent.futures import ThreadPoolExecutor, Future
from typing import Any, Generator, Type, cast
from .exceptions import NoEventLoopException, TaskTimeoutError
from contextlib import contextmanager
from selectors import SelectorKey
from collections import deque
from queue import SimpleQueue
import selectors
import itertools
import socket
import heapq
import time

lazy from .messages import Message, BackgroundMessage, MessageWithSocket, Messages, AsCompleted, Accept
lazy from .classes import Task, Yieldable
lazy from .scheduler import BaseScheduler
lazy from .utils import log

class _DequeScheduler(BaseScheduler):
    def __init__(self):
        self.tasks: deque[Task] = deque()
        self._queued: set[Task] = set()

    def add(self, task, *, immediate=False):
        if task in self._queued:
            if immediate:
                self.tasks.remove(task)
                self.tasks.appendleft(task)
            return

        self._queued.add(task)
        if immediate:
            self.tasks.appendleft(task)
        else:
            self.tasks.append(task)

    def pop(self) -> Task:
        task = self.tasks.popleft()
        self._queued.remove(task)
        return task

    def remove(self, task):
        self.tasks.remove(task)
        self._queued.remove(task)

    def account(self, task, runtime):
        pass

    def __contains__(self, task: Task) -> bool:
        return task in self._queued

    def __bool__(self) -> bool:
        return bool(self.tasks)

    def __len__(self) -> int:
        return len(self.tasks)

_NOTREADY = object()

def _background_message[T](message: BackgroundMessage) -> Generator[BackgroundMessage, Any, Any]:
    message.background = False
    try:
        return (yield message)
    finally:
        message.background = True

_loop: ExecutionLoop | None = None
class ExecutionLoop:
    def __init__(self, scheduler: Type[BaseScheduler] | None = None):
        self.scheduler: BaseScheduler = (scheduler or _DequeScheduler)()
        self._run_frames: list[dict[str, float]] = []

        self.selector = selectors.DefaultSelector()
        self.running = False

        self.pending = 0

        self.running_tasks: set[Task] = set()

        self.timers: list[tuple[float, int, Message | None, Task]] = []
        self._timer_id = itertools.count()

        self.executor = ThreadPoolExecutor()
        self.completed: SimpleQueue[tuple[Message, Task, Future[Any]]] = SimpleQueue()
        self.abandoned_completions: dict[tuple[Message, Task], int] = {}
        self._has_timed_out_work = False

        self.wakeup_r, self.wakeup_w = socket.socketpair()
        self.wakeup_r.setblocking(False)
        self.wakeup_w.setblocking(False)

        self.selector.register(self.wakeup_r, selectors.EVENT_READ, None)

        global _loop
        _loop = self

    def process_timers(self):
        now = time.monotonic()

        while self.timers and self.timers[0][0] <= now:
            deadline, _sequence, message, task = heapq.heappop(self.timers)

            if message is None and task._timeout_deadline != deadline:
                continue

            self.pending -= 1

            if message is None:
                task._timeout_deadline = None
                self._expire_task(task)
                continue

            if task.is_ready:
                self.schedule(task)

    def _discard_stale_timeout_timers(self) -> None:
        while self.timers:
            deadline, _sequence, message, task = self.timers[0]
            if message is not None or task._timeout_deadline == deadline:
                return
            heapq.heappop(self.timers)

    def _start_task_timeout(self, task: Task) -> None:
        if task.timeout is None or task._timeout_started or task.done:
            return

        task._timeout_started = True
        deadline = time.monotonic() + task.timeout
        task._timeout_deadline = deadline
        heapq.heappush(
            self.timers,
            (deadline, next(self._timer_id), None, task),
        )
        self.pending += 1

    def _clear_task_timeout(self, task: Task) -> None:
        if task._timeout_deadline is None:
            return

        task._timeout_deadline = None
        self.pending -= 1

    def _expire_task(self, task: Task) -> None:
        if task.done or task._timeout_expired:
            return

        task._timeout_expired = True
        self._cancel_task_waits(task)
        task.throw_value(TaskTimeoutError(f'{task!r} exceeded its {task.timeout}s timeout'))
        self.schedule(task, immediate=True)

    def _cancel_task_waits(self, task: Task) -> None:
        for message in tuple(task.wait_for):
            self._remove_timer_wait(message, task)
            self._remove_socket_wait(message)
            self._remove_join_wait(message, task)

            remaining = message._pending_count
            if remaining:
                if message._reg_type in ('execute', 'as_completed'):
                    key = (message, task)
                    self.abandoned_completions[key] = self.abandoned_completions.get(key, 0) + remaining
                    self._has_timed_out_work = True
                self.pending -= remaining
                message._pending_count = 0

            message.resolve([])

        task.wait_for.clear()

    def _remove_timer_wait(self, message: Message, task: Task) -> None:
        kept = [entry for entry in self.timers if not (entry[2] is message and entry[3] is task)]
        removed = len(self.timers) - len(kept)
        if removed:
            self.timers = kept
            heapq.heapify(self.timers)

    def _remove_socket_wait(self, message: Message) -> None:
        if not isinstance(message, MessageWithSocket):
            return

        try:
            key = self.selector.get_key(message.socket)
        except KeyError:
            return

        data = key.data
        if not isinstance(data, dict) or message not in data:
            return
        data = dict(cast(dict[Any, Any], data))
        del data[message]
        if data:
            self.selector.modify(key.fileobj, key.events, data)
        else:
            self.selector.unregister(key.fileobj)

    @staticmethod
    def _remove_join_wait(message: Message, task: Task) -> None:
        targets = getattr(message, 'targets', ())
        target = getattr(message, 'target', None)
        if target is not None:
            targets = (*targets, target)
        for joined_task in targets:
            if hasattr(joined_task, '_joiners'):
                joined_task._joiners[:] = [
                    joiner for joiner in joined_task._joiners
                    if joiner != (message, task)
                ]

    def process_event(self, key: SelectorKey, event: int):
        # Thread
        if key.fileobj is self.wakeup_r:
            self.process_completed()
            return

        # Socket
        for message, task in list(key.data.items()):
            message.resolve(event)

            if message.is_resolved():
                self.pending -= 1
                del key.data[message]

                if isinstance(message, Accept):
                    task.return_value(message.result)

                if task.is_ready:
                    self.schedule(task)

        if not key.data:
            self.selector.unregister(key.fileobj)

    def poll(self):
        if self.scheduler:
            return

        self._discard_stale_timeout_timers()
        timeout = None
        if self.timers:
            timeout = max(0, self.timers[0][0] - time.monotonic())

        events = self.selector.select(timeout)

        self.process_timers()

        if self.scheduler:
            return

        for key, event in events:
            self.process_event(key, event)

    @contextmanager
    def pause_accounting(self):
        frames = tuple(self._run_frames)
        start = time.monotonic()

        try:
            yield
        finally:
            elapsed = time.monotonic() - start

            for frame in frames:
                frame["paused"] += elapsed

    def run_one(self):
        task = self.scheduler.pop()
        self._start_task_timeout(task)

        frame = {"paused": 0.0}
        self._run_frames.append(frame)
        self.running_tasks.add(task)

        start = time.monotonic()

        try:
            result = self.run_task(task)
        except BaseException as exc:
            task.done = True
            task.exception = exc
            task.result = None
            result = _NOTREADY
        finally:
            runtime = max(
                0.0,
                time.monotonic() - start - frame["paused"],
            )

            self.running_tasks.remove(task)
            self._run_frames.pop()

            self.scheduler.account(task, runtime)

        deadline = task._timeout_deadline
        timed_out_now = deadline is not None and time.monotonic() >= deadline
        if timed_out_now and not task._timeout_expired:
            self._clear_task_timeout(task)
            timeout_error = TaskTimeoutError(f'{task!r} exceeded its {task.timeout}s timeout')
            if task.done:
                task._timeout_expired = True
                task.exception = timeout_error
                task.result = None
            else:
                self._expire_task(task)
                self.schedule(task, immediate=True)
                result = _NOTREADY
        elif task._timeout_expired:
            timeout_error = TaskTimeoutError(f'{task!r} exceeded its {task.timeout}s timeout')
            if task.done:
                task.exception = timeout_error
                task.result = None
            else:
                self.schedule(task, immediate=True)
                result = _NOTREADY

        if task.done:
            self._clear_task_timeout(task)

        parent = task.parent

        if task.done and task._joiners:
            for message, waiter in task._joiners:
                self.pending -= 1
                if isinstance(message, BackgroundMessage):
                    message.task_completed(task, waiter)
                else:
                    message.resolve([])
                    waiter.return_value(task.result)
                if waiter.is_ready:
                    self.schedule(waiter)
            task._joiners.clear()

        if parent is task and task.done:
            if task.exception is not None:
                raise task.exception
            return

        if task.exception is not None:
            if parent is not None:
                parent.throw_value(task.exception)
                self.schedule(parent, parent.parent, immediate=True)
            return

        if result is _NOTREADY:
            return

        if isinstance(result, BackgroundMessage) and result.background:
            result = Task(_background_message(result), background=True, immediate=True)

        if isinstance(result, Task) and not task.done:
            log(f'{result.immediate=}')
            log(f'{result.background=}')
            if result.background:
                self.schedule(cast(Task[Any], result), immediate=result.immediate)
                task.return_value(result)
                self.schedule(task, task.parent, immediate=True)
                return
            else:
                self.schedule(cast(Task[Any], result), task, immediate=result.immediate)
            return

        if isinstance(result, Message):
            self.handle_message(task, result)
            return

        if isinstance(result, Messages):
            for message in result.messages:
                self.handle_message(task, message)
            return

        if parent is not None:
            parent.return_value(result)
            log('Rescheduling immediately')
            self.schedule(parent, parent.parent, immediate=True)
        elif not task.done:
            self.schedule(task)

    def run(self, initial: Yieldable | Task | None = None):
        try:
            self.running = True

            if initial is not None:
                if isinstance(initial, Yieldable):
                    initial = initial()

                self.schedule(initial, initial)

            while self.running:
                self.process_timers()

                if self.scheduler:
                    self.run_one()
                else:
                    if not self.pending:
                        break
                    self.poll()

        finally:
            self.shutdown()

    def handle_message(self, task: Task, message: Message):
        if message._type == 'register':
            message.register(task)

        elif message._type == 'execute':
            future = self.executor.submit(message.execute)
            future.add_done_callback(
                lambda future, message=message, task=task:
                    self.thread_completed(message, task, future)
            )

        task.wait_for.append(message)
        self.pending += message._pending_count

    def run_task(self, task: Task):
        log(f'\nRunning: {task}')

        if not task.is_ready:
            log(f"Task is not ready, waiting for: {task.wait_for[0]}.")
            return _NOTREADY

        result = task()
        log(f'{result=}')
        log(f'{task.parent=}')

        return result

    def schedule(self, task: Task, parent: Task | None = None, *, immediate=False):
        if task in self.running_tasks:
            return

        if parent is not None:
            task.parent = parent

        self.scheduler.add(task, immediate=immediate)

    def put_off(self, task: Task):
        if task in self.scheduler:
            self.scheduler.remove(task)

        self.scheduler.add(task)

    def process_completed(self):
        while True:
            try:
                self.wakeup_r.recv(4096)
            except BlockingIOError:
                break

        while not self.completed.empty():
            message, task, future = self.completed.get()

            abandoned_key = (message, task)
            abandoned_count = self.abandoned_completions.get(abandoned_key, 0)
            if abandoned_count:
                if abandoned_count == 1:
                    del self.abandoned_completions[abandoned_key]
                else:
                    self.abandoned_completions[abandoned_key] = abandoned_count - 1
                continue

            if message._reg_type == 'as_completed':
                self.pending -= 1
                message._pending_count -= 1

                message = cast(AsCompleted[Any], message)

                if message.receive(task, future):
                    task.return_value(iter(message))

                    if task.is_ready:
                        self.schedule(task)

                continue

            try:
                task.return_value(future.result())
            except BaseException as exc:
                task.throw_value(exc)

            message.resolve([])
            self.pending -= 1
            message._pending_count -= 1

            if task.is_ready:
                self.schedule(task)

    def thread_completed(self, message: Message, task: Task, future: Future[Any]):
        self.completed.put((message, task, future))

        try:
            self.wakeup_w.send(b'\0')
        except BlockingIOError:
            pass

    def shutdown(self):
        self.executor.shutdown(wait=not self._has_timed_out_work, cancel_futures=self._has_timed_out_work)

def getEventLoop():
    if not _loop:
        raise NoEventLoopException('No event loop started')

    return _loop
