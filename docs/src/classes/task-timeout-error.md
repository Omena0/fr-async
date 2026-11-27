# `TaskTimeoutError`

Raised when a task's configured timeout expires.

**Source:** [fr_async/exceptions.py:8](fr_async/exceptions.py:8)

## Base class

```python
class TaskTimeoutError(TimeoutError):
    ...
```

Set a task timeout through `Task(..., timeout=seconds)` or the corresponding
[Yieldable](yieldable.md) call option. The timeout is measured from the task's
first run. The loop throws `TaskTimeoutError` into the task and records it as
the task's exception if it is not handled.
