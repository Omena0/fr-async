from collections.abc import Callable, Generator, Iterable
from typing import IO, Any, final, overload, Literal
from concurrent.futures import Future
from collections import deque
from subprocess import Popen
import subprocess
import selectors
import socket
import heapq
import time

lazy from .typing import LiteralType, ReturnsAfter, YieldTypeIs
lazy from .loop import ExecutionLoop, getEventLoop
lazy from .exceptions import NoEventLoopException
lazy from .classes import Task
lazy from .utils import log

# Check if a set of blockers are resolved
def check_resolved(blockers: deque[Message]) -> bool:
    'Remove resolved blockers and report whether the queue is empty.'
    while blockers:
        if not blockers[0].is_resolved():
            return False
        blockers.popleft()

    # Queue drained
    return True

# Select events
def register_event(self: MessageWithSocket, task: Task, event: int):
    'Register a socket message with the selector for the requested event.'
    _loop = getEventLoop()

    try:
        old = _loop.selector.get_key(self.socket).data
    except KeyError:
        _loop.selector.register(self.socket, event, {self: task})
    else:
        _loop.selector.modify(self.socket, event, old | {self: task})

# Event loop messaging base classes
class Message:
    """Base class for values yielded to the event loop to request an operation."""
    __slots__ = ('_pending_count', '_resolved', 'timeout')

    _type = 'None'
    _reg_type = 'None'

    def __init__(self, *, timeout: float | None = None) -> None:
        'Initialize the message as unresolved, with an optional per-message timeout.'
        self._resolved = False
        self._pending_count = 1
        self.timeout: float | None = timeout

    def __repr__(self) -> str:
        'Return a compact representation of the message and its public fields.'
        fields: dict[str, Any] = {}
        for cls in reversed(type(self).__mro__):
            slots = cls.__dict__.get('__slots__', ())
            if isinstance(slots, str):
                slots = (slots,)
            for name in slots:
                if not name.startswith('_') and hasattr(self, name):
                    fields[name] = getattr(self, name)

        if hasattr(self, '__dict__'):
            fields.update(vars(self))

        values = ','.join(
            f'{name}={value if len(str(value)) < 20 else "..."}'
            for name, value in fields.items()
        )
        return f'{self.__class__.__name__}({values})'

    def is_resolved(self) -> bool:
        """Return whether the event loop has completed this message."""
        return self._resolved

    def resolve(self, events: list[Any]) -> None:
        'Mark the message as resolved.'
        self._resolved = True
        log(f"{self.__class__.__name__} resolved: {events}")

    def register(self, task: Task) -> Any:
        """Register this message with the event loop for a task."""

    def execute(self) -> Any:
        """Perform the blocking operation represented by this message."""

class BackgroundMessage(Message):
    'A message that can run in a detached task when background mode is enabled.'
    __slots__ = ('background', 'timeout')

    def __init__(self, *, background: bool = False, timeout: float | None = None) -> None:
        'Initialize the message and set its background mode and optional timeout.'
        super().__init__(timeout=timeout)
        self.background = background

    def task_completed(self, target: Task, waiter: Task) -> None:
        """Resolve a task joiner with the completed task's result."""
        self.resolve([])
        if target.exception is not None:
            waiter.throw_value(target.exception)
        else:
            waiter.return_value(target.result)

@final # Typo prevention 8000
class Messages:
    'A collection of messages that a task waits for together.'
    __slots__ = ('messages',)

    def __init__(self, messages: Iterable[Message]):
        'Store the messages to register for a task.'
        self.messages = messages

# Sleep
class Sleep[T: float](BackgroundMessage, YieldTypeIs[ReturnsAfter[T, None]]):
    'Wait until a duration has elapsed.'
    __slots__ = ('until',)

    _type = 'register'
    _reg_type = 'timer'
    def __init__(self, duration: LiteralType[T], *, background: bool = False, timeout: float | None = None):
        'Set the sleep deadline, optional background mode, and optional per-message timeout.'
        super().__init__(background=background, timeout=timeout)
        self.until: float = time.monotonic() + duration

    def is_resolved(self) -> bool:
        'Return whether the sleep deadline has passed.'
        return self._resolved or time.monotonic() >= self.until

    def register(self, task: Task):
        'Add this deadline to the event loop timer queue.'
        from .loop import _loop
        if not _loop:
                raise NoEventLoopException("No event loop started.")

        heapq.heappush(
            _loop.timers,
            (self.until, next(_loop._timer_id), self, task)
        )

# Sockets
class MessageWithSocket(BackgroundMessage):
    'Base class for messages that have a socket.'
    __slots__ = ('socket',)

    socket: socket.socket

class WaitReadable(MessageWithSocket, YieldTypeIs[ReturnsAfter[None]]):
    'Wait until a socket can be read without blocking.'
    __slots__ = ()

    _type = 'register'
    _reg_type = 'socket'
    def __init__(self, sock: socket.socket, *, background: bool = False, timeout: float | None = None):
        'Create a message that waits for socket read readiness.'
        super().__init__(background=background, timeout=timeout)
        self.socket = sock

    def register(self, task):
        'Register the socket for read readiness.'
        register_event(self, task, selectors.EVENT_READ)

class WaitWritable(MessageWithSocket, YieldTypeIs[ReturnsAfter[None]]):
    'Wait until a socket can be written without blocking.'
    __slots__ = ()

    _type = 'register'
    _reg_type = 'socket'
    def __init__(self, sock: socket.socket, *, background: bool = False, timeout: float | None = None):
        'Create a message that waits for socket write readiness.'
        super().__init__(background=background, timeout=timeout)
        self.socket = sock

    def register(self, task):
        'Register the socket for write readiness.'
        register_event(self, task, selectors.EVENT_WRITE)

class Accept(MessageWithSocket, YieldTypeIs[ReturnsAfter[None, list[tuple[socket.socket, tuple[str, int]]]]]):
    """Accept all currently queued client connections without blocking the loop."""
    __slots__ = ('result',)

    _type = 'register'
    _reg_type = 'socket'

    def __init__(self, sock: socket.socket, *, background: bool = False, timeout: float | None = None):
        """Create an accept message for a listening socket."""
        super().__init__(background=background, timeout=timeout)
        self.socket = sock
        self.result: list[tuple[socket.socket, tuple[str, int]]] = []

    def register(self, task: Task) -> None:
        """Make the listener nonblocking and register it for readability."""
        self.socket.setblocking(False)
        register_event(self, task, selectors.EVENT_READ)

    def resolve(self, events: list[Any]) -> None:
        """Drain the ready accept queue and resolve with accepted sockets."""
        try:
            while True:
                client, address = self.socket.accept()
                client.setblocking(False)
                self.result.append((client, address))
        except BlockingIOError:
            if not self.result:
                return
        if self.result:
            super().resolve(events)

# Send to thread (for blocking tasks)
class ToThread[T, **P](BackgroundMessage, YieldTypeIs[T]):
    'Run a callable in the event loop thread pool and return its result.'
    __slots__ = ('args', 'func', 'future', 'kwargs', 'result')

    _type = 'register'
    @overload
    def __init__(self, func: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> None:
        """Typing overload for calls matching the wrapped callable."""
    @overload
    def __init__(self, func: Callable[..., T], /, *args: Any, background: bool = False, **kwargs: Any) -> None:
        """Typing overload for calls with a background option."""
    def __init__(self, func: Callable[..., T], /, *args: Any, background: bool = False, timeout: float | None = None, **kwargs: Any) -> None:
        'Store the callable, arguments, background mode, and optional per-message timeout.'
        super().__init__(background=background, timeout=timeout)
        self.func: Callable[P, T] = func
        self.args: tuple[Any, ...] = args
        self.kwargs: dict[str, Any] = kwargs
        self.future: Future[T] | None = None
        self.result: T | None = None

    def register(self, task: Task):
        'Submit the callable to the event loop thread pool.'
        _loop: ExecutionLoop = getEventLoop()

        self.future = _loop.executor.submit(
            self.func, *self.args, **self.kwargs
        )
        self.future.add_done_callback(
            lambda future, _loop=_loop: _loop.thread_completed(self, task, future)
        )

class AsCompleted[T](BackgroundMessage, YieldTypeIs[ReturnsAfter[None, Generator[T]]]):
    'Run thread operations and/or task groups, yielding results as they complete.'
    __slots__ = ('completed', 'loop', 'operations', 'targets')

    _type = 'register'
    _reg_type = 'as_completed'

    def __init__(self, *items: ToThread[T, Any] | Task[T], background: bool = False, timeout: float | None = None):
        'Store thread operations and task targets, initialize completion tracking.'
        super().__init__(background=background, timeout=timeout)
        self.operations: tuple[ToThread[T, Any], ...] = tuple(
            i for i in items if isinstance(i, ToThread)
        )
        self.targets: tuple[Task[T], ...] = tuple(
            i for i in items if isinstance(i, Task)
        )
        self.completed: deque[tuple[Any, BaseException | None]] = deque()
        self._pending_count = len(self.operations) + sum(not t.done for t in self.targets)
        self.loop: ExecutionLoop | None = None

    def register(self, task: Task):
        'Submit all operations to the thread pool and register all task targets as joiners.'
        _loop: ExecutionLoop = getEventLoop()
        self.loop = _loop

        if not self.operations and not self.targets:
            task.return_value(iter(self))
            self.resolve([])
            return

        for operation in self.operations:
            future: Future[Any] = _loop.executor.submit(
                operation.func, *operation.args, **operation.kwargs
            )
            future.add_done_callback(
                lambda future, message=self, task=task, loop=_loop:
                    loop.thread_completed(message, task, future)
            )

        for target in self.targets:
            if target.done:
                self.completed.append((target.result, target.exception))
            else:
                target._joiners.append((self, task))

        if self.completed or not self._pending_count:
            task.return_value(iter(self))
            self.resolve([])

    def receive(self, future: Future[Any]) -> bool:
        'Record one thread future result and report whether iteration can begin.'
        try:
            self.completed.append((future.result(), None))
        except BaseException as exc:
            self.completed.append((None, exc))
        self._pending_count -= 1
        # The first result makes the message ready.
        if not self.is_resolved():
            self.resolve([])
            return True
        return False

    def task_completed(self, target: Task, waiter: Task) -> None:
        'Record one task completion result from a task joiner.'
        self.completed.append((target.result, target.exception))
        self._pending_count -= 1
        if not self.is_resolved():
            self.resolve([])

    def __iter__(self) -> Generator[Any, Any]:
        'Yield results in completion order, raising any operation error.'
        if self.loop is None:
            raise NoEventLoopException("No event loop started.")

        remaining = len(self.operations) + len(self.targets)
        while remaining:
            with self.loop.pause_accounting():
                while not self.completed:
                    if self.loop.scheduler:
                        self.loop.run_one()
                    else:
                        self.loop.poll()

            remaining -= 1
            result, exception = self.completed.popleft()

            if exception is not None:
                raise exception

            yield result

class TaskJoin[T](BackgroundMessage, YieldTypeIs[ReturnsAfter[None, T]]):
    'Wait for a task to finish and return its result.'
    __slots__ = ('target',)

    _type = 'register'

    def __init__(self, target: Task[T], *, background: bool = False, timeout: float | None = None):
        'Create a message that waits for the target task.'
        super().__init__(background=background, timeout=timeout)
        self.target = target

    def register(self, task: Task):
        'Return an existing result or register this task as a joiner.'
        if self.target.done:
            if self.target.exception is not None:
                task.throw_value(self.target.exception)
            else:
                task.return_value(self.target.result)
            self.resolve([])
            self._pending_count = 0
        else:
            self.target._joiners.append((self, task))

class Collect[T=Any](BackgroundMessage, YieldTypeIs[list[T] | Generator[T]]):
    'Wait for multiple tasks/messages and return results, ordered or by completion.'
    __slots__ = ('_pending_count', 'completed', 'indices', 'items', 'loop', 'mode', 'results')

    _type = 'register'

    def __init__(self, *items: Task[T] | Message, mode: Literal['ordered', 'completed'] = 'ordered', background: bool = False, timeout: float | None = None):
        'Initialize ordered or completion-ordered result collection for any mix of tasks and messages.'
        super().__init__(background=background, timeout=timeout)
        self.items: tuple[Task[T] | Message, ...] = items
        self.mode = mode
        self.results: list[T | BaseException | None] = [None] * len(items)
        self.indices = {id(item): index for index, item in enumerate(items)}
        self.completed: deque[tuple[Any, BaseException | None]] = deque()
        self.loop: ExecutionLoop | None = None
        self._pending_count = sum(
            0 if isinstance(item, Task) and item.done else
            0 if isinstance(item, Message) and item.is_resolved() else
            1
            for item in items
        )

    def register(self, task: Task[T]):
        'Register all items; existing results are captured immediately, pending ones await resolution.'
        self.loop = getEventLoop()

        for index, item in enumerate(self.items):
            if isinstance(item, Task):
                if item.done:
                    self.results[index] = item.exception if item.exception is not None else item.result
                else:
                    item._joiners.append((self, task))
            # item is a Message
            elif item.is_resolved():
                self.results[index] = None  # messages don't carry a result directly
            else:
                item.register(task)

        if not self._pending_count:
            if self.mode == 'completed':
                task.return_value(iter(self))
            else:
                task.return_value(self.results)
            self.resolve([])

    def task_completed(self, target: Task[T], waiter: Task) -> None:
        'Store a completed task result and wake the waiter when all items finish.'
        index = self.indices.get(id(target))
        if index is not None:
            self.results[index] = target.exception if target.exception is not None else target.result
        self._pending_count -= 1
        if self.mode == 'completed':
            self.completed.append((target.result, target.exception))
            if not self.is_resolved():
                self.resolve([])
                waiter.return_value(iter(self))
        elif not self._pending_count:
            self.resolve([])
            waiter.return_value(self.results)

    def __iter__(self) -> Generator[T]:
        'Yield results in completion order (mode="completed") while driving the event loop.'
        if self.loop is None:
            raise NoEventLoopException('No event loop started.')
        remaining = len(self.items)
        while remaining:
            with self.loop.pause_accounting():
                while not self.completed:
                    if self.loop.scheduler:
                        self.loop.run_one()
                    else:
                        self.loop.poll()
            remaining -= 1
            result, exception = self.completed.popleft()
            if exception is not None:
                raise exception
            yield result

class Exec(BackgroundMessage, YieldTypeIs[Popen[bytes]]):
    'Run a subprocess in the event loop thread pool, returning a Popen object.'
    __slots__ = ('args', 'path', 'proc')

    _type = 'execute'

    def __init__(self, path: str, *args: str, background: bool = False, timeout: float | None = None):
        'Store the command and arguments, optional background mode and per-message timeout.'
        super().__init__(background=background, timeout=timeout)
        self.path = path
        self.args = args
        self.proc: subprocess.Popen[bytes] | None = None

    def execute(self) -> subprocess.Popen[bytes]:
        'Launch the subprocess (blocking, runs in the thread pool).'
        self.proc = subprocess.Popen(
            [self.path, *self.args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return self.proc

# File IO
def _validateFileMode[T: (str, bytes)](self: FileWrite[T]) -> bool:
    'Return whether the file mode matches the data type.'
    return ('b' in self.file.mode) == isinstance(self.data, bytes)

class FileRead[T: (str, bytes)](BackgroundMessage, YieldTypeIs[T]):
    'Read data from a file in the event loop thread pool.'
    __slots__ = ('file', 'n')

    _type = 'execute'
    def __init__(self, file: IO[T], n: int = -1, *, background: bool = False, timeout: float | None = None):
        'Store the file, read size, background mode, and optional per-message timeout.'
        super().__init__(background=background, timeout=timeout)
        self.file: IO[T] = file
        self.n: int = n

    def execute(self) -> T:
        'Read up to the configured number of values from the file.'
        return self.file.read(self.n)

class FileWrite[T: (str, bytes)](BackgroundMessage, YieldTypeIs[int]):
    'Write data to a file in the event loop thread pool.'
    __slots__ = ('data', 'file')

    _type = 'execute'
    def __init__(self, file: IO[T], data: T, *, background: bool = False, timeout: float | None = None):
        'Store the file and data, validating text or binary mode, and optional per-message timeout.'
        super().__init__(background=background, timeout=timeout)
        self.file: IO[T] = file
        self.data = data

        if not _validateFileMode(self):
            raise ValueError('File mode and data type do not match.')

    def execute(self) -> int:
        'Write the configured data and return the number of values written.'
        return self.file.write(self.data)


__all__ = [
    'Accept',
    'AsCompleted',
    'BackgroundMessage',
    'Collect',
    'Exec',
    'FileRead',
    'FileWrite',
    'Message',
    'MessageWithSocket',
    'Messages',
    'Sleep',
    'TaskJoin',
    'ToThread',
    'WaitReadable',
    'WaitWritable',
]
