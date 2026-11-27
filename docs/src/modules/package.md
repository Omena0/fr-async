# `fr_async`

The package root re-exports the main runtime entry points and the
`fr_async.messages` module.

**Source:** [fr_async/__init__.py:1](fr_async/__init__.py:1)

## Explicit package exports

The package's `__all__` contains:

- `EEVDFScheduler` — [scheduler class](../classes/eevdf-scheduler.md).
- `ExecutionLoop` — [event loop](../classes/execution-loop.md).
- `File` — [file wrapper](../classes/file.md).
- `Task` — [task object](../classes/task.md).
- `TaskGroup` — [task collection](../classes/task-group.md).
- `Yieldable` — [callable wrapper](../classes/yieldable.md).
- `getEventLoop` — [current loop accessor](loop.md).
- `messages` — the `fr_async.messages` module.

These are the names included by `from fr_async import *`.

## Message classes at the package root

`fr_async.__init__` also performs `from .messages import *`, so names in
`fr_async.messages.__all__` are available for explicit imports from the
package root, for example `from fr_async import Sleep`. However, those names
are not included in the package's own `__all__`; they are therefore omitted by
`from fr_async import *`. Importing message classes from
`fr_async.messages` makes the intended module boundary explicit.
