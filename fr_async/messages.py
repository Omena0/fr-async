from ast import Return
from typing import Iterable, Callable, IO, Literal, Tuple, final, Any, Generator, List, overload
from concurrent.futures import Future
from collections import deque
import selectors
import socket
import heapq
import time

lazy from .loop import getEventLoop, ExecutionLoop
lazy from .typing import LiteralType, ReturnsAfter, YieldTypeIs
lazy from .exceptions import NoEventLoopException
lazy from .classes import Task
lazy from .utils import log

# Check if a set of blockers are resolved
def check_resolved(blockers: deque[Message]) -> bool:
    'Remove resolved blockers and report whether the queue is empty.'
    for blocker in blockers.copy():
        if blocker.is_resolved(): # Resolved
            blockers.remove(blocker)
            continue

        # Not resolved
        return False

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
    _type = 'None'
    _reg_type = 'None'
    _pending_count = 1
    def __init__(self) -> None:
        'Initialize the message as unresolved.'
        self._resolved = False

    def __repr__(self) -> str:
        'Return a compact representation of the message and its public fields.'
        return (f"{self.__class__.__name__}("
                    f"{','.join([
                        f'{k}={v if len(str(v))<20 else '...'}'
                        for k,v in self.__dict__.items()
                        if not k.startswith('_')
                    ])})"
                )

    def is_resolved(self) -> bool:
        """Return whether the event loop has completed this message."""
        return self._resolved

    def resolve(self, events: list[Any]) -> None:
        'Mark the message as resolved.'
        self._resolved = True
        log(f"{self.__class__.__name__} resolved: {events}")

    def register(self, task: Task) -> Any:
        """Register this message with the event loop for a task."""
        ...

    def execute(self) -> Any:
        """Perform the blocking operation represented by this message."""
        ...

class BackgroundMessage(Message):
    'A message that can run in a detached task when background mode is enabled.'
    def __init__(self, *, background: bool = False):
        'Initialize the message and set its background mode.'
        super().__init__()
        self.background = background

    def task_completed(self, target: Task, waiter: Task) -> None:
        """Resolve a task joiner with the completed task's result."""
        self.resolve([])
        waiter.return_value(target.result)

@final # Typo prevention 8000
class Messages:
    'A collection of messages that a task waits for together.'
    def __init__(self, messages: Iterable[Message]):
        'Store the messages to register for a task.'
        self.messages = messages

# Sleep
class Sleep[T: float](BackgroundMessage, YieldTypeIs[ReturnsAfter[T, None]]):
    'Wait until a duration has elapsed.'
    _type = 'register'
    _reg_type = 'timer'
    def __init__(self, duration: LiteralType[T], *, background: bool = False):
        'Set the sleep deadline and optional background mode.'
        super().__init__(background=background)
        self.until: float = time.monotonic() + duration

    def is_resolved(self) -> bool:
        'Return whether the sleep deadline has passed.'
        return time.monotonic() >= self.until

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
    socket: socket.socket

class WaitReadable(MessageWithSocket, YieldTypeIs[ReturnsAfter[None]]):
    'Wait until a socket can be read without blocking.'
    _type = 'register'
    _reg_type = 'socket'
    def __init__(self, sock: socket.socket, *, background: bool = False):
        'Create a message that waits for socket read readiness.'
        super().__init__(background=background)
        self.socket = sock

    def register(self, task):
        'Register the socket for read readiness.'
        register_event(self, task, selectors.EVENT_READ)

class WaitWritable(MessageWithSocket, YieldTypeIs[ReturnsAfter[None]]):
    'Wait until a socket can be written without blocking.'
    _type = 'register'
    _reg_type = 'socket'
    def __init__(self, sock: socket.socket, *, background: bool = False):
        'Create a message that waits for socket write readiness.'
        super().__init__(background=background)
        self.socket = sock

    def register(self, task):
        'Register the socket for write readiness.'
        register_event(self, task, selectors.EVENT_WRITE)

class Accept(MessageWithSocket, YieldTypeIs[ReturnsAfter[None, tuple[socket.socket, tuple[str, int]]]]):
    """Accept one client connection without blocking the event loop."""
    _type = 'register'
    _reg_type = 'socket'

    def __init__(self, sock: socket.socket, *, background: bool = False):
        """Create an accept message for a listening socket."""
        super().__init__(background=background)
        self.socket = sock
        self.result: tuple[socket.socket, Any] | None = None

    def register(self, task: Task) -> None:
        """Make the listener nonblocking and register it for readability."""
        self.socket.setblocking(False)
        register_event(self, task, selectors.EVENT_READ)

    def resolve(self, events: list[Any]) -> None:
        """Accept a ready connection and resolve with its socket and address."""
        try:
            client, address = self.socket.accept()
        except BlockingIOError:
            return
        client.setblocking(False)
        self.result = (client, address)
        super().resolve(events)

# Send to thread (for blocking tasks)
class ToThread[T, **P](BackgroundMessage, YieldTypeIs[T]):
    'Run a callable in the event loop thread pool and return its result.'
    _type = 'register'
    @overload
    def __init__(self, func: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs) -> None:
        """Typing overload for calls matching the wrapped callable."""
        ...
    @overload
    def __init__(self, func: Callable[..., T], /, *args: Any, background: bool = False, **kwargs: Any) -> None:
        """Typing overload for calls with a background option."""
        ...
    def __init__(self, func: Callable[..., T], /, *args: Any, background: bool = False, **kwargs: Any) -> None:
        'Store the callable, arguments, and background mode.'
        super().__init__(background=background)
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
    'Run thread operations and yield results as they complete.'
    _type = 'register'
    _reg_type = 'as_completed'

    def __init__(self, *operations: ToThread[T, Any], background: bool = False):
        'Store operations and initialize completion tracking.'
        super().__init__(background=background)
        self.operations: tuple[ToThread[T, Any], ...] = operations
        self.completed: deque[Tuple[Any, BaseException | None]] = deque()
        self._pending_count: int = len(operations)
        self.loop: ExecutionLoop | None = None

    def register(self, task: Task):
        'Submit all operations to the thread pool.'
        _loop: ExecutionLoop = getEventLoop()
        self.loop = _loop

        if not self.operations:
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

    def receive(self, task: Task, future: Future[Any]) -> bool:
        'Record one future result and report whether iteration can begin.'
        try:
            self.completed.append((future.result(), None))
        except BaseException as exc:
            self.completed.append((None, exc))

        # The first result makes the message ready.
        if not self.is_resolved():
            self.resolve([])
            return True

        return False

    def __iter__(self) -> Generator[Any, Any, None]:
        'Yield results in completion order, raising any operation error.'
        if self.loop is None:
            raise NoEventLoopException("No event loop started.")

        for _ in self.operations:
            with self.loop.pause_accounting():
                while not self.completed:
                    if self.loop.scheduler:
                        self.loop.run_one()
                    else:
                        self.loop.poll()

            result, exception = self.completed.popleft()

            if exception is not None:
                raise exception

            yield result

class TaskJoin[T](BackgroundMessage, YieldTypeIs[ReturnsAfter[None, T]]):
    'Wait for a task to finish and return its result.'
    _type = 'register'

    def __init__(self, target: Task, *, background: bool = False):
        'Create a message that waits for the target task.'
        super().__init__(background=background)
        self.target = target

    def register(self, task: Task):
        'Return an existing result or register this task as a joiner.'
        if self.target.done:
            task.return_value(self.target.result)
            self.resolve([])
            self._pending_count = 0
        else:
            self.target._joiners.append((self, task))

class TaskGroupCollect[T](BackgroundMessage, YieldTypeIs[ReturnsAfter[None, list[T]]]):
    'Wait for all target tasks and return results in target order.'
    _type = 'register'

    def __init__(self, targets: tuple[Task, ...], *, background: bool = False):
        'Initialize ordered result storage and pending task tracking.'
        super().__init__(background=background)
        self.targets = targets
        self.results: list[Any] = [None] * len(targets)
        self.indices = {target: index for index, target in enumerate(targets)}
        self._pending_count = sum(not target.done for target in targets)

    def register(self, task: Task):
        'Collect existing results and register for unfinished tasks.'
        for index, target in enumerate(self.targets):
            if target.done:
                self.results[index] = target.result
            else:
                target._joiners.append((self, task))
        if not self._pending_count:
            task.return_value(self.results)
            self.resolve([])

    def task_completed(self, target: Task, waiter: Task):
        'Store a result and wake the waiter when all targets finish.'
        self.results[self.indices[target]] = target.result
        self._pending_count -= 1
        if not self._pending_count:
            self.resolve([])
            waiter.return_value(self.results)

class TaskGroupAsCompleted[T](BackgroundMessage, YieldTypeIs[ReturnsAfter[None, Generator[T]]]):
    'Wait for target tasks and expose results in completion order.'
    _type = 'register'

    def __init__(self, targets: tuple[Task, ...], *, background: bool = False):
        'Initialize completion tracking for target tasks.'
        super().__init__(background=background)
        self.targets = targets
        self.completed: deque[Any] = deque()
        self._pending_count = sum(not target.done for target in targets)
        self.loop: ExecutionLoop | None = None

    def register(self, task: Task):
        'Register for unfinished targets and queue existing results.'
        self.loop = getEventLoop()
        for target in self.targets:
            if target.done:
                self.completed.append(target.result)
            else:
                target._joiners.append((self, task))
        if self.completed or not self._pending_count:
            task.return_value(iter(self))
            self.resolve([])

    def task_completed(self, target: Task, waiter: Task):
        'Queue a result and wake the waiter on the first completion.'
        self.completed.append(target.result)
        if not self.is_resolved():
            self.resolve([])
            waiter.return_value(iter(self))

    def __iter__(self) -> Generator[T, None, None]:
        'Yield results in completion order while driving the event loop.'
        if self.loop is None:
            raise NoEventLoopException('No event loop started.')
        remaining = len(self.targets)
        while remaining:
            with self.loop.pause_accounting():
                while not self.completed:
                    if self.loop.scheduler:
                        self.loop.run_one()
                    else:
                        self.loop.poll()
            remaining -= 1
            yield self.completed.popleft()

# File IO
def _validateFileMode[T: (str, bytes)](self: FileWrite[T]) -> bool:
    'Return whether the file mode matches the data type.'
    return ('b' in self.file.mode) == isinstance(self.data, bytes)

class FileRead[T: (str, bytes)](BackgroundMessage, YieldTypeIs[T]):
    'Read data from a file in the event loop thread pool.'
    _type = 'execute'
    def __init__(self, file: IO[T], n: int = -1, *, background: bool = False):
        'Store the file, read size, and background mode.'
        super().__init__(background=background)
        self.file: IO[T] = file
        self.n: int = n

    def execute(self) -> T:
        'Read up to the configured number of values from the file.'
        return self.file.read(self.n)

class FileWrite[T: (str, bytes)](BackgroundMessage, YieldTypeIs[int]):
    'Write data to a file in the event loop thread pool.'
    _type = 'execute'
    def __init__(self, file: IO[T], data: T, *, background: bool = False):
        'Store the file and data, validating text or binary mode.'
        super().__init__(background=background)
        self.file: IO[T] = file
        self.data = data

        if not _validateFileMode(self):
            raise ValueError('File mode and data type do not match.')

    def execute(self) -> int:
        'Write the configured data and return the number of values written.'
        return self.file.write(self.data)


__all__ = [
    'Message',
    'BackgroundMessage',
    'Messages',
    'Sleep',
    'WaitReadable',
    'WaitWritable',
    'Accept',
    'ToThread',
    'AsCompleted',
    'TaskJoin',
    'TaskGroupCollect',
    'TaskGroupAsCompleted',
    'FileRead',
    'FileWrite',
]
