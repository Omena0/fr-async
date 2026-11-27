# `ToThread`

Runs a callable in the event loop's `ThreadPoolExecutor` and sends its result
back to the yielding task.

**Source:** [fr_async/messages.py:209](fr_async/messages.py:209)

## Constructor

```python
class ToThread[T, **P](BackgroundMessage, YieldTypeIs[T]):
    def __init__(
        self,
        func: Callable[..., T],
        /,
        *args: Any,
        background: bool = False,
        **kwargs: Any,
    ) -> None:
        ...
```

The actual implementation also declares overloads that preserve the callable's
parameter types. `func`, `args`, and `kwargs` are submitted to the thread pool
when the message is registered. `background=True` runs it detached.

## Public state

- `func`, `args`, and `kwargs` store the operation and arguments.
- `future` is the submitted `Future[T]`, or `None` before registration.
- `result` is a result field initialized to `None`; completion delivery is
  performed through the task by the event loop.

## Public method

### `register`

Submits the callable to the current loop's executor and registers a completion
callback that wakes the loop.

## Example

```python
from fr_async.messages import ToThread
import time

def main():
    value = yield ToThread(time.sleep, 0.5)
    print(value)
```

The example's `value` is `None`, which is the return value of `time.sleep`.
