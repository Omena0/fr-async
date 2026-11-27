# `NoEventLoopException`

Raised when code requests or registers work with an event loop before an
[ExecutionLoop](execution-loop.md) has been constructed.

**Source:** [fr_async/exceptions.py:3](fr_async/exceptions.py:3)

## Base class

```python
class NoEventLoopException(Exception):
    ...
```

The exception has no custom constructor or additional fields. It is raised by
[`getEventLoop()`](../modules/loop.md) and by message registration paths that
require the current loop.
