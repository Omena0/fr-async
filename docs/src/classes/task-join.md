# `TaskJoin`

A background-capable message used to wait for one target task and obtain its
result.

**Source:** [fr_async/messages.py:308](fr_async/messages.py:308)

## Constructor

```python
TaskJoin(target: Task[T], *, background: bool = False)
```

The message is normally created by [`Task.join()`](task.md).

## Public method

### `register`

If the target is already complete, sends its result to the waiting task or
throws its exception into it, then resolves immediately. Otherwise, registers
the waiting task as a joiner of the target.

## Usage

```python
def parent(child):
    result = yield child.join()
    return result
```
