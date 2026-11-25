from concurrent.futures import ThreadPoolExecutor, Future
from typing import Any, Generator, Type, cast
from .exceptions import NoEventLoopException
from contextlib import contextmanager
from selectors import SelectorKey
from collections import deque
from queue import SimpleQueue
import selectors
import itertools
import socket
import heapq
import time

lazy from .messages import Message, BackgroundMessage, Messages, AsCompleted
lazy from .classes import Task, Yieldable
lazy from .scheduler import BaseScheduler
lazy from .utils import log

class _DequeScheduler(BaseScheduler):
    def __init__(self):
        self.tasks: deque[Task] = deque()

    def add(self, task, *, immediate=False):
        if task in self.tasks:
            if immediate:
                self.tasks.remove(task)
                self.tasks.appendleft(task)
            return

        if immediate:
            self.tasks.appendleft(task)
        else:
            self.tasks.append(task)

    def pop(self) -> Task:
        return self.tasks.popleft()

    def remove(self, task):
        self.tasks.remove(task)

    def account(self, task, runtime):
        pass

    def __contains__(self, task: Task) -> bool:
        return task in self.tasks

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

        self.timers: list[tuple[float, int, Message, Task]] = []
        self._timer_id = itertools.count()

        self.executor = ThreadPoolExecutor()
        self.completed: SimpleQueue[tuple[Message, Task, Future[Any]]] = SimpleQueue()

        self.wakeup_r, self.wakeup_w = socket.socketpair()
        self.wakeup_r.setblocking(False)
        self.wakeup_w.setblocking(False)

        self.selector.register(self.wakeup_r, selectors.EVENT_READ, None)

        global _loop
        _loop = self

    def process_timers(self):
        now = time.monotonic()

        while self.timers and self.timers[0][0] <= now:
            _deadline, _sequence, _msg, task = heapq.heappop(self.timers)
            self.pending -= 1

            if task.is_ready:
                self.schedule(task)

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

                if task.is_ready:
                    self.schedule(task)

        if not key.data:
            self.selector.unregister(key.fileobj)

    def poll(self):
        if self.scheduler:
            return

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

        frame = {"paused": 0.0}
        self._run_frames.append(frame)
        self.running_tasks.add(task)

        start = time.monotonic()

        try:
            result = self.run_task(task)
        finally:
            runtime = max(
                0.0,
                time.monotonic() - start - frame["paused"],
            )

            self.running_tasks.remove(task)
            self._run_frames.pop()

            self.scheduler.account(task, runtime)

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
            return

        if result is _NOTREADY:
            return

        if isinstance(result, BackgroundMessage) and result.background:
            result = Task(_background_message(result), background=True, immediate=True)

        if isinstance(result, Task) and not task.done:
            log(f'{result.immediate=}')
            log(f'{result.background=}')
            if result.background:
                self.schedule(result, immediate=result.immediate)
                task.return_value(result)
                self.schedule(task, task.parent, immediate=True)
                return
            else:
                self.schedule(result, task, immediate=result.immediate)
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
                    if not self.timers and not self.pending:
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

            if message._reg_type == 'as_completed':
                self.pending -= 1

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

            if task.is_ready:
                self.schedule(task)

    def thread_completed(self, message: Message, task: Task, future: Future[Any]):
        self.completed.put((message, task, future))

        try:
            self.wakeup_w.send(b'\0')
        except BlockingIOError:
            pass

    def shutdown(self):
        self.executor.shutdown(wait=True)

def getEventLoop():
    if not _loop:
        raise NoEventLoopException('No event loop started')

    return _loop
