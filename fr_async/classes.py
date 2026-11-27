from typing import IO, Any, Final, Literal, Self, TypeIs, overload
from collections.abc import Callable, Generator
from contextvars import Context, copy_context
from collections import deque
import inspect
import os

from .typing import OpenMode, FilePath, OpenBinaryMode, OpenTextMode
lazy from .messages import check_resolved
lazy from .loop import getEventLoop
lazy from .utils import log
from .messages import *


class _Missing:
    """Sentinel type used to distinguish a missing value."""
    __slots__ = ()

_MISSING = _Missing()

class Assignable:
    'Store arbitrary data and expose entries as attributes.'
    __slots__ = ('_data',)

    def __init__(self) -> None:
        'Create an empty attribute store.'
        self._data: dict[Any, Any] = {}

    def __setattr__(self, name: str, value: Any) -> None:
        'Store user attributes in the internal mapping.'
        if name == '_data':
            return super().__setattr__(name, value)

        self._data[name] = value
        return None

    def __getattr__(self, name: str):
        'Look up an attribute in the internal mapping.'
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(name) from None

    def update(self, data: dict[Any, Any]):
        'Merge entries into the attribute store.'
        self._data |= data

def _is_generator(x: Any) -> TypeIs[Generator[Any, Any, Any]]:
    'Return whether a value is a generator object.'
    return inspect.isgenerator(x)

# Tasks
class Task[T=Any]:
    'Represent a callable or generator managed by the event loop.'
    __slots__ = (
        '_context',
        '_joiners',
        '_return_val',
        '_stop_requested',
        '_throw_val',
        '_timeout_deadline',
        '_timeout_expired',
        '_timeout_started',
        'args',
        'background',
        'data',
        'done',
        'exception',
        'func',
        'immediate',
        'kwargs',
        'parent',
        'result',
        'timeout',
        'wait_for',
    )

    def __init__(self, func: Generator[Any, Any, T] | Callable[..., T], *args: Any, background=False, immediate=False, parent: Task | None = None, data: dict[Any, Any] | None = None, timeout: float | None = None, **kwargs: Any):
        'Initialize the task and its optional timeout, measured from its first run.'
        if timeout is not None and timeout < 0:
            raise ValueError('Task timeout must be non-negative.')

        self.func: Generator[Any, Any, Any] | Callable[..., Any] = func
        self.immediate = immediate
        self.background = background

        self.args = args
        self.kwargs = kwargs

        self.parent: Task | None = parent
        self.done = False
        self.result: Any = None
        self.exception: BaseException | None = None
        self.timeout = timeout
        self._timeout_deadline: float | None = None
        self._timeout_started = False
        self._timeout_expired = False
        self._joiners: list[tuple[Message, Task]] = []

        self.data = Assignable()
        self.data.update(data or {})

        self._stop_requested = False
        self._return_val: Any = _MISSING
        self._throw_val: Any = _MISSING

        self.wait_for: deque[Message] = deque()
        self._context: Context = copy_context()

    def __call__(self) -> T | None:
        'Advance the generator or invoke the callable and return its yielded value.'
        if self._stop_requested:
            if _is_generator(self.func):
                self.func.close()

            self.done = True
            self.result = None
            return None

        if _is_generator(self.func):
            try:
                if isinstance(self._throw_val, BaseException):
                    exc = self._throw_val
                    self._throw_val = _MISSING
                    self._timeout_expired = False
                    return self.func.throw(exc)

                if self._return_val is not _MISSING:
                    value = self._return_val
                    self._return_val = _MISSING
                    return self.func.send(value)

                return next(self.func)

            except StopIteration as e:
                self.done = True
                self.result = e.value
                return e.value

        result = self.func(*self.args, **self.kwargs)
        self.done = True
        self.result = result
        return result

    def __repr__(self) -> str:
        'Return a readable representation of the task.'
        name: str = getattr(self.func, '__name__', str(self.func))
        return f"Task({name}, {self.args}, {self.kwargs})"

    @property
    def is_ready(self):
        'Return whether all messages blocking this task are resolved.'
        return check_resolved(self.wait_for)

    def throw_value(self, exc: Any):
        'Schedule an exception to throw into the task generator.'
        self._throw_val = exc

    def return_value(self, value: Any):
        'Schedule a value to send into the task generator.'
        log(f'Task returned {value} to {self}')
        self._return_val = value

    def stop(self):
        'Request that the task stop when it next runs.'
        self._stop_requested = True

    def join(self, *, background: bool = False) -> TaskJoin[T]:
        """Wait for this task to finish and return its result."""
        return TaskJoin(self, background=background)

# Task factory function wrapper
class Yieldable:
    'Wrap a callable so it can be converted into an event loop task.'
    __slots__ = ('func',)

    def __init__(self, func: Callable[..., Any]):
        'Store the callable to wrap.'
        self.func = func

    def __call__(self, /, *args: Any, background: bool=False, immediate=False, data: dict[Any, Any] | None = None, timeout: float | None = None, **kwargs: Any) -> Task:
        'Create a task with scheduling options and an optional first-run timeout.'
        if inspect.isgeneratorfunction(self.func):
            return Task(self.func(*args, **kwargs), background=background, immediate=immediate, timeout=timeout)

        return Task(self.func, background=background, immediate=immediate, data=data, timeout=timeout, *args, **kwargs)

    def __repr__(self) -> str:
        'Return a readable representation of the wrapped callable.'
        return f"Yieldable({self.func.__name__})"

# File wrapper
class File[T: (str, bytes)]:
    'Wrap a text or binary file and expose event loop file operations.'
    __slots__ = ('_binary', '_file')

    @overload
    def __init__(
            self: File[bytes],
            path: FilePath,
            mode: OpenBinaryMode,
            buffering: int = -1,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
            closefd: bool = True,
            opener: Callable[[str, int], int] | None = None
        ) -> None:
        """Typing overload for opening a binary file."""
    @overload
    def __init__(
            self: File[str],
            path: FilePath,
            mode: OpenTextMode,
            buffering: int = -1,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
            closefd: bool = True,
            opener: Callable[[str, int], int] | None = None
        ) -> None:
        """Typing overload for opening a text file."""
    def __init__(
            self,
            path: FilePath,
            mode: OpenMode,
            buffering: int = -1,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
            closefd: bool = True,
            opener: Callable[[str, int], int] | None = None
        ):
        'Open a file using the requested mode and options.'
        self._file: Final[IO[T]] = open(path, mode, buffering, encoding, errors, newline, closefd, opener)
        self._binary = 'b' in mode

    def write(self, data: T, *, background: bool = False) -> FileWrite[T]:
        'Create a message that writes data to the file.'
        return FileWrite[T](self._file, data, background=background)

    def read(self, n: int = -1, *, background: bool = False) -> FileRead[T]:
        'Create a message that reads from the current file position.'
        return FileRead[T](self._file, n, background=background)

    def seek(self, offset: int, whence: int = 0) -> int:
        'Move the file cursor and return its new position.'
        return self._file.seek(offset, whence)

    def delete(self):
        'Remove the file from the filesystem.'
        os.remove(self._file.name)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_):
        self._file.close()

# Task group
class TaskGroup:
    'Collect tasks for coordinated scheduling, cancellation, and result handling.'
    __slots__ = ('started', 'tasks')

    def __init__(self) -> None:
        'Create an empty task group that has not started.'
        self.tasks: list[Task] = []
        self.started = False

    def add(self, task: Task, autorun: bool = True) -> Task:
        'Add a task, scheduling it if a started group autoruns additions.'
        if task in self.tasks:
            return task

        self.tasks.append(task)

        if autorun and self.started and not task.done:
            getEventLoop().schedule(task)

        return task

    def collect(self, *, background: bool = False) -> TaskGroupCollect[Any]:
        'Create a message that returns results in group order.'
        return TaskGroupCollect(tuple(self.tasks), background=background)

    def as_completed(self, *, background: bool = False) -> TaskGroupAsCompleted[Any]:
        'Create a message that yields results in completion order.'
        return TaskGroupAsCompleted(tuple(self.tasks), background=background)

    def cancel(self):
        'Request that every task in the group stop.'
        for task in self.tasks:
            task.stop()

    def run(self, background: bool = False) -> TaskGroupCollect[Any] | None:
        'Schedule unfinished tasks and optionally return a collection message.'
        loop = getEventLoop()
        self.started = True
        for task in self.tasks:
            if not task.done:
                loop.schedule(task)
        return None if background else self.collect()

    def join(self, *, background: bool = False) -> TaskGroupCollect[Any]:
        """Wait for all group tasks; use as ``yield group.join()``."""
        return self.collect(background=background)

    def __iter__(self):
        'Iterate over tasks in insertion order.'
        return iter(self.tasks)

    def __len__(self) -> int:
        'Return the number of tasks in the group.'
        return len(self.tasks)

    def __contains__(self, task: object) -> bool:
        'Return whether a task belongs to this group.'
        return task in self.tasks
