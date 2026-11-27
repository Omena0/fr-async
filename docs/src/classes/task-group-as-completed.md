# `TaskGroupAsCompleted`

A background-capable message that waits for a group of tasks and exposes their
results in completion order rather than insertion order.

**Source:** [fr_async/messages.py:368](fr_async/messages.py:368)

## Constructor

```python
TaskGroupAsCompleted(
    targets: tuple[Task, ...],
    *,
    background: bool = False,
)
```

Applications normally obtain it from
[`TaskGroup.as_completed()`](task-group.md).

## Public state

- `targets` stores the tasks being joined.
- `completed` queues `(result, exception)` pairs as tasks finish.
- `loop` stores the loop used to drive iteration.

## Public methods

### `register`

Queues results from tasks already complete and registers with unfinished
tasks. The iterator becomes available when the first result is queued or when
there are no unfinished tasks.

### `task_completed`

Adds the completed task's result or exception to the queue. The first
completion resolves the message and wakes the waiting task.

### `__iter__`

Yields results in completion order. If a task failed, its exception is raised
when its result is consumed. Iteration drives the loop's scheduler or selector
while waiting for the next completion.
