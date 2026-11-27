# `BaseScheduler`

Interface-like base class for the scheduler used by an
[ExecutionLoop](execution-loop.md). It is not an abstract base class and does
not enforce that subclasses override its methods; implement the operations
below when supplying a custom scheduler.

**Source:** [fr_async/scheduler.py:9](fr_async/scheduler.py:9)

## Required operations

```python
class BaseScheduler:
    def __bool__(self) -> bool: ...
    def __contains__(self, item: Any) -> bool: ...
    def __len__(self) -> int: ...
    def add(self, task: Task, *, immediate: bool = False): ...
    def pop(self) -> Task: ...
    def remove(self, task: Task) -> None: ...
    def account(self, task: Task, runtime: float) -> None: ...
```

- `__bool__` reports whether the scheduler has queued work.
- `__contains__` tests whether a task is queued.
- `__len__` returns the number of queued tasks.
- `add` queues a task. `immediate=True` requests priority placement.
- `pop` removes and returns the next task. It should raise `IndexError` if
  the queue is empty.
- `remove` removes a queued task.
- `account` receives the time a task spent executing, in seconds.

The bundled implementation is [EEVDFScheduler](eevdf-scheduler.md).
