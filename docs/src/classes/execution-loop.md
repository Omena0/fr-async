# `ExecutionLoop`

The cooperative event loop. It schedules tasks and dispatches yielded messages
to timers, socket selectors, or the thread pool.

**Source:** [fr_async/loop.py:70](fr_async/loop.py:70)

## Constructor

```python
ExecutionLoop(scheduler: type[BaseScheduler] | None = None)
```

Creates the default deque-based scheduler when `scheduler` is omitted.
Otherwise, it instantiates the supplied scheduler class. A custom scheduler
must implement the operations described by [BaseScheduler](base-scheduler.md).
Construction also makes this instance the module-level loop returned by
[`getEventLoop`](../modules/loop.md).

## Public state

- `scheduler` is the scheduler instance.
- `selector` is the platform default socket selector.
- `running` controls the main loop while `run()` is active.
- `pending` counts outstanding timer, socket, join, and thread work.
- `running_tasks` contains tasks currently being advanced.
- `executor` is the `ThreadPoolExecutor` used for blocking operations.
- `timers` is the timer heap used by the loop.

These attributes expose implementation state; changing them directly can
break scheduling invariants.

## Main entry point

### `run`

```python
def run(self, initial: Yieldable | Task | None = None): ...
```

Runs the loop on the calling thread. If `initial` is a `Yieldable`, it is
called to create the initial task. The loop continues while `running` is true
and there is scheduled or pending work. The executor is shut down in a
`finally` block when `run()` exits, so an instance should be treated as
single-use.

## Scheduling methods

### `schedule`

```python
def schedule(
    self,
    task: Task,
    parent: Task | None = None,
    *,
    immediate: bool = False,
): ...
```

Adds a task to the scheduler. When `parent` is supplied, it becomes the task's
parent. A task already being advanced is not added again. `immediate=True`
passes the priority request to the scheduler.

### `put_off`

```python
def put_off(self, task: Task): ...
```

Removes a queued task if present and re-adds it without the immediate flag.

## Advanced loop operations

These methods are public, but ordinary applications normally use `run()` and
let the loop invoke them:

- `run_one()` pops and advances one scheduled task, accounts its runtime,
  and processes its yielded result.
- `run_task(task: Task)` advances the task if it is ready; otherwise it returns
  the loop's internal not-ready sentinel.
- `handle_message(task: Task, message: Message)` registers a message or submits
  its `execute()` method to the thread pool.
- `poll()` waits for selector or timer events when no task is queued.
- `process_timers()` handles timer deadlines that have expired.
- `process_event(key: SelectorKey, event: int)` dispatches one selector event.
- `process_completed()` drains completed thread-pool work and resumes waiters.
- `thread_completed(message: Message, task: Task, future: Future[Any])`
  queues a worker completion and wakes the selector.
- `pause_accounting()` is a context manager that excludes the enclosed time
  from currently active task runtime accounting.
- `shutdown()` shuts down the thread pool. `run()` calls it automatically.

## Related function

See [`getEventLoop()`](../modules/loop.md) for access to the most recently
constructed loop.
