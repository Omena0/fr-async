# `TaskGroup`

Collects tasks for coordinated scheduling, cancellation, and result handling.
Tasks retain their insertion order in the group.

**Source:** [fr_async/classes.py:261](fr_async/classes.py:261)

## Constructor

```python
TaskGroup() -> None
```

Creates an empty group with `started = False` and an empty `tasks` list.

## Public state

- `tasks` is the list of tasks in insertion order.
- `started` records whether `run()` has been called.

## Public methods

### `add`

```python
def add(self, task: Task, autorun: bool = True) -> Task: ...
```

Adds a task unless it is already in the group. If the group has started,
`autorun` is true, and the task is unfinished, it is scheduled immediately.
Returns the supplied task.

### `collect`

```python
def collect(self, *, background: bool = False) -> TaskGroupCollect[Any]: ...
```

Creates a [TaskGroupCollect](task-group-collect.md) message. Its result list
uses the group's task order.

### `as_completed`

```python
def as_completed(
    self, *, background: bool = False
) -> TaskGroupAsCompleted[Any]: ...
```

Creates a [TaskGroupAsCompleted](task-group-as-completed.md) message. Its
iterator yields results in completion order.

### `cancel`

```python
def cancel(self): ...
```

Calls `stop()` on every task. Stopping is cooperative: each task stops when it
next runs.

### `run`

```python
def run(self, background: bool = False) -> TaskGroupCollect[Any] | None: ...
```

Marks the group as started and schedules every unfinished task. Returns
`None` when `background=True`; otherwise returns a collection message that can
be yielded by the current task.

### `join`

```python
def join(self, *, background: bool = False) -> TaskGroupCollect[Any]: ...
```

Creates a collection message for waiting on every task. It does not schedule
the tasks; call `run()` or schedule them separately.

### Container methods

- `__iter__()` iterates over tasks in insertion order.
- `__len__()` returns the number of tasks.
- `__contains__(task)` tests membership.

## Example

```python
from fr_async import Task, TaskGroup

def worker(value: int) -> int:
    return value * 2

def main():
    group = TaskGroup()
    group.add(Task(worker, 2))
    group.add(Task(worker, 3))
    results = yield group.run()
    return results
```
