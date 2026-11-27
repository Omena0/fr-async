# `fr_async.scheduler`

Defines the scheduler interface accepted by `ExecutionLoop` and the bundled
weighted EEVDF-style implementation.

## Public classes

- [BaseScheduler](../classes/base-scheduler.md) — interface-like base class
  for a task scheduler. [Source](fr_async/scheduler.py:9)
- [EEVDFScheduler](../classes/eevdf-scheduler.md) — weighted virtual-runtime
  scheduler with deadline-based selection. [Source](fr_async/scheduler.py:35)

## Public constants

### `NICE_0_LOAD`

```python
NICE_0_LOAD = 1024.0
```

The reference load used to scale task virtual runtime and deadlines.

[Source](fr_async/scheduler.py:21)
