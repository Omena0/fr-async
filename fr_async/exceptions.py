
# Exceptions
class NoEventLoopException(Exception):
    __slots__ = ()

class TaskTimeoutError(TimeoutError):
    __slots__ = ()

    """Raised when a task exceeds its configured timeout."""

