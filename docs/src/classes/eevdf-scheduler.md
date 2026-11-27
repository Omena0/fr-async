# `EEVDFScheduler`

A weighted scheduler that tracks virtual runtime, deadlines, and lag for each
task. It uses `task.data.weight` and `task.data.time_slice` as per-task
configuration, supplying defaults when those attributes are absent or `None`.

**Source:** [fr_async/scheduler.py:35](fr_async/scheduler.py:35)

## Constructor

```python
EEVDFScheduler(
    *,
    default_slice: float = 0.004,
    sleep_decay: float | None = 1.0,
)
```

- `default_slice` is the default task time slice in seconds.
- `sleep_decay` controls exponential decay of a task's saved lag while it is
  inactive. Set it to `None` to disable this decay.
- Both numeric values must be finite and positive; invalid values raise
  `ValueError`.

## Public methods

### `get_data`

```python
@staticmethod
def get_data(task: Task, name: str, default: Any): ...
```

Reads `name` from `task.data`. If the value is missing or `None`, stores and
returns `default`.

### `add`

```python
def add(self, task: Task, *, immediate: bool = False): ...
```

Queues a task. On first insertion, reads or initializes the task's `weight`
and `time_slice` data values. Re-adding an inactive task restores its saved
lag, optionally decaying it according to `sleep_decay`. Re-adding a task that
is already queued does not duplicate it; `immediate=True` moves its pending
entry into the immediate queue.

### `pop`

```python
def pop(self) -> Task: ...
```

Returns the next task. Immediate entries are selected before normal EEVDF
selection. Raises `IndexError` when empty.

### `remove`

```python
def remove(self, task: Task): ...
```

Removes a queued task and stores its lag and inactive timestamp. Raises
`ValueError` when the task is not queued.

### `account`

```python
def account(self, task: Task, runtime: float): ...
```

Updates a task's virtual runtime and lag using its configured weight. A
completed task is removed from the scheduler's entity tracking.

### Container methods

`__contains__`, `__bool__`, and `__len__` test membership, report whether the
queue is nonempty, and return the queued-task count, respectively.

## Per-task tuning

Set the metadata before a task is first added to this scheduler:

```python
task.data.weight = 2048.0
task.data.time_slice = 0.008
```

The scheduler defaults to `NICE_0_LOAD` (`1024.0`) for weight and
`default_slice` for time slice. Both task values must be finite and positive.
