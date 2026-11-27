from typing import Any
from collections.abc import Callable, Generator
import os
import socket
from contextlib import contextmanager


def log(*args: Any, **kwargs: Any):
    if not DEBUG: return
    print(*args, **kwargs)


DEBUG = 'FR_ASYNC_DEBUG' in os.environ


@contextmanager
def timeout(seconds: float):
    '''Context manager that sets a per-block timeout on the current task.

    If the block does not yield control before ``seconds`` elapse,
    ``TaskTimeoutError`` is raised into the task at the next resume point.

    Example::

        from fr_async.utils import timeout
        from fr_async.messages import Sleep

        def my_task():
            with timeout(2.0):
                yield Sleep(5.0)   # raises TaskTimeoutError
    '''
    from .loop import getEventLoop

    loop = getEventLoop()
    task = loop._current_task
    if task is None:
        raise RuntimeError('timeout() must be used inside a running task')

    old_timeout = task.timeout
    old_deadline = task._timeout_deadline
    task.timeout = seconds
    task._timeout_started = False
    task._timeout_deadline = None
    task._timeout_expired = False
    try:
        yield
    finally:
        task.timeout = old_timeout
        task._timeout_deadline = old_deadline
        task._timeout_started = old_timeout is not None
        task._timeout_expired = False


def server(
    handler: Callable[[socket.socket, tuple[str, int]], Generator[Any, Any, Any]]
):
    '''Decorator: wrap a ``(cs, addr)`` generator handler into a TCP server factory.

    The decorated function becomes a server factory. Calling it with a port returns
    a :class:`Yieldable` that, when run by an :class:`ExecutionLoop`, binds a listening
    socket and runs an accept loop. Each accepted connection is handed to the handler
    as a background task.

    The handler receives ``(client_socket, address)`` and can yield messages
    (e.g. ``WaitReadable``, ``WaitWritable``, ``Sleep``) to perform async I/O.

    Example::

        from fr_async import server, ExecutionLoop, WaitReadable

        @server
        def handle(cs, addr):
            data = yield WaitReadable(cs)
            cs.sendall(b'pong')
            cs.close()

        # User code must create the loop explicitly:
        loop = ExecutionLoop()
        loop.run(handle(8080))

    The factory can also be used to create a background task::

        srv = server(handle)
        task = srv(8080, background=True)
        task.run()
    '''
    from .classes import Task, Yieldable
    from .messages import Accept

    def serve(port: int, host: str = '0.0.0.0', backlog: int = 128):
        def accept_loop() -> Generator[Accept, Any]:
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((host, port))
            listener.listen(backlog)
            listener.setblocking(False)

            # Accept drains the entire accept queue in a single yield,
            # which is more efficient than yielding Accept once per connection.
            while True:
                connections = yield Accept(listener)
                for cs, addr in connections:
                    Task(handler(cs, addr), background=True, immediate=True)

        return Yieldable(accept_loop)

    return serve
