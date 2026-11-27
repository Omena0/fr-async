# `Sleep`

A timer message that becomes resolved after a duration measured with
`time.monotonic()`.

**Source:** [fr_async/messages.py:114](fr_async/messages.py:114)

## Constructor

```python
class Sleep[T: float](BackgroundMessage, YieldTypeIs[ReturnsAfter[T, None]]):
    def __init__(self, duration: LiteralType[T], *, background: bool = False):
        ...
```

The deadline is calculated when the `Sleep` object is constructed, not when it
is yielded. The `background` option enables detached/background behavior.

## Public methods

### `is_resolved`

Returns `True` when the message has already been resolved or its monotonic
deadline has passed.

### `register`

Adds the deadline to the current event loop's timer heap. It raises
[NoEventLoopException](no-event-loop-exception.md) if no loop exists.

## Example

```python
from fr_async.messages import Sleep

def task():
    yield Sleep(0.5)
    print("half a second elapsed")
```

The yielded result is `None`.
