# `Yieldable`

Wraps a callable so it can be called to create a `Task`. It is also usable as a
decorator because applying it to a function returns a `Yieldable` instance.

**Source:** [fr_async/classes.py:173](fr_async/classes.py:173)

## Constructor

```python
Yieldable(func: Callable[..., Any])
```

Stores the callable in the public `func` attribute.

## Calling the wrapper

```python
def __call__(
    self,
    /,
    *args: Any,
    background: bool = False,
    immediate: bool = False,
    data: dict[Any, Any] | None = None,
    timeout: float | None = None,
    **kwargs: Any,
) -> Task: ...
```

Creates and returns a task. If `func` is a generator function, it calls the
function immediately to obtain the generator and wraps that generator in a
`Task`. Otherwise, the callable and its arguments are stored for invocation
when the task runs.

- `background` and `immediate` set the corresponding task flags.
- `data` initializes task data for ordinary callables.
- `timeout` configures the task's first-run timeout.
- Remaining positional and keyword arguments are passed to the wrapped
  callable.

In the current implementation, `data` is not forwarded when `func` is a
generator function.

## Example

```python
from fr_async import ExecutionLoop, Yieldable
from fr_async.messages import Sleep

@Yieldable
def main():
    yield Sleep(0.1)
    print("finished")

ExecutionLoop().run(main())
```
