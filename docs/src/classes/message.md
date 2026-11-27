# `Message`

Base class for objects yielded by a task to request work from the event loop.
Concrete subclasses implement either `register()` or `execute()` according
to their `_type` dispatch mode.

**Source:** [fr_async/messages.py:40](fr_async/messages.py:40)

## Constructor

```python
Message() -> None
```

Creates an unresolved message with one pending operation.

## Public methods

### `is_resolved`

```python
def is_resolved(self) -> bool: ...
```

Returns whether the message has been marked resolved.

### `resolve`

```python
def resolve(self, events: list[Any]) -> None: ...
```

Marks the message resolved. Subclasses may override this to gather results
before delegating to the base implementation.

### `register`

```python
def register(self, task: Task) -> Any: ...
```

Hook for registration-based messages. The base implementation has no
operation; subclasses provide the registration behavior.

### `execute`

```python
def execute(self) -> Any: ...
```

Hook for execution-based messages. The base implementation has no
operation; subclasses provide the blocking operation to run in the pool.

### `__repr__`

Returns a compact representation containing the message class name and its
public slot or instance attributes.

A normal application should yield a concrete message such as
[Sleep](sleep.md), [WaitReadable](wait-readable.md), or
[ToThread](to-thread.md), rather than instantiate `Message` directly.
