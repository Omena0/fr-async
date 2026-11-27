# `Task`

A `Task` represents a callable or an already-created generator managed by an
[ExecutionLoop](execution-loop.md). Generator tasks are advanced one yield at
a time. A yielded message blocks the task until the event loop resolves it.

**Source:** [fr_async/classes.py:53](fr_async/classes.py:53)

## Constructor

```python
class Task[T = Any]:
    def __init__(
        self,
        func: Generator[Any, Any, T] | Callable[..., T],
        *args: Any,
        background: bool = False,
        immediate: bool = False,
        parent: Task | None = None,
        data: dict[Any, Any] | None = None,
        timeout: float | None = None,
        **kwargs: Any,
    ):
        ...
```

- `func` is either a generator object or a callable. Pass a generator object
  when constructing a generator task directly; use [Yieldable](yieldable.md)
  when starting from a generator function.
- `args` and `kwargs` are passed to `func` when it is an ordinary callable.
- `background` marks the task as detached when it is scheduled by the loop.
- `immediate` requests priority placement when the task is scheduled.
- `parent` is the task to resume when this task completes.
- `data` initializes the task's dynamic [Assignable](assignable.md) data store.
- `timeout` is a non-negative number of seconds. Its deadline begins on the
  task's first scheduled run. A negative value raises `ValueError`.

## Public state

- `func`, `args`, and `kwargs` hold the callable/generator and its arguments.
- `background` and `immediate` hold scheduling flags.
- `parent` identifies the task waiting for this task.
- `done`, `result`, and `exception` expose completion state.
- `timeout` stores the configured timeout.
- `data` is an `Assignable` container for application or scheduler metadata.
- `wait_for` contains messages currently blocking the task.

The task's internal generator send/throw state is not part of the public API.

## Public methods

### `__call__`

```python
def __call__(self) -> T | None: ...
```

Advances the generator once, or invokes an ordinary callable. For generators,
`StopIteration.value` becomes `result` and marks the task complete. The event
loop normally calls this method; applications rarely need to call it directly.

### `is_ready`

```python
@property
def is_ready(self): ...
```

Returns whether all messages blocking the task have resolved.

### `throw_value`

```python
def throw_value(self, exc: Any): ...
```

Schedules `exc` to be thrown into the generator on its next advance.

### `return_value`

```python
def return_value(self, value: Any): ...
```

Schedules `value` to be sent into the generator on its next advance.

### `stop`

```python
def stop(self): ...
```

Requests that the task stop when next run. If its callable is a generator,
the generator is closed at that point.

### `join`

```python
def join(self, *, background: bool = False) -> TaskJoin[T]: ...
```

Creates a [TaskJoin](task-join.md) message. Use `yield task.join()` inside a
generator task to wait for completion and receive the result. The target's
exception is propagated to the waiting task.

## Important distinction

`Task(generator_function)` does not automatically call and iterate the
returned generator. Use `Yieldable(generator_function)` for that case, or pass
an already-created generator object to `Task`.
