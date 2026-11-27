
# Exceptions
class NoEventLoopException(Exception):
    __slots__ = ()

    """Raised when requesting an event loop, but one hasn't been created yet."""

class TaskTimeoutError(TimeoutError):
    __slots__ = ()

    """Raised when a task exceeds its configured timeout."""

