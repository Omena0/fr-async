# `AsCompleted`

Submits several [ToThread](to-thread.md) operations and exposes their results
as an iterator in completion order.

**Source:** [fr_async/messages.py:240](fr_async/messages.py:240)

## Constructor

```python
class AsCompleted[T](
    BackgroundMessage,
    YieldTypeIs[ReturnsAfter[None, Generator[T]]],
):
    def __init__(
        self,
        *operations: ToThread[T, Any],
        background: bool = False,
    ):
        ...
```

Pass one `ToThread` message per operation. All operations are submitted when
the message is registered. With no operations, it resolves immediately and
returns an empty iterator.

## Public methods

### `register`

Submits every operation to the event loop's thread pool. Each completion is
queued as it arrives.

### `receive`

Records a future's result or exception. It reports `True` when the first
completion makes the iterator ready.

### `__iter__`

Returns results in completion order. An operation's exception is raised while
iterating, at the position where that operation completes.

## Example

```python
from fr_async.messages import AsCompleted, ToThread

def main():
    results = yield AsCompleted(
        ToThread(fetch, "first"),
        ToThread(fetch, "second"),
    )
    for result in results:
        print(result)
```
