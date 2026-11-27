# `TaskGroupCollect`

A background-capable message that waits for all tasks in a group and returns
their results in the order the tasks were supplied.

**Source:** [fr_async/messages.py:331](fr_async/messages.py:331)

## Constructor

```python
TaskGroupCollect(targets: tuple[Task, ...], *, background: bool = False)
```

Applications normally obtain this message from
[`TaskGroup.collect()`](task-group.md) or [`TaskGroup.join()`](task-group.md).

## Public state

- `targets` stores the tasks being joined.
- `results` stores the result slots in target order.
- `indices` maps each target task to its result position.

## Public methods

### `register`

Copies results for already-completed tasks and registers the message with
unfinished targets. If all targets are already done, it resolves immediately.

### `task_completed`

Stores one target's result in its original position. When every target has
completed, resolves the message and returns the ordered result list to the
waiting task.

A target exception is propagated to the waiting task.
