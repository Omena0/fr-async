# `fr_async.classes`

Core task abstractions and the file wrapper. The module has no explicit
`__all__`; its non-underscored classes are directly importable from this
module.

## Public classes

- [Assignable](../classes/assignable.md) — an attribute store backed by a
  dictionary. [Source](fr_async/classes.py:21)
- [Task](../classes/task.md) — a callable or generator managed by the loop.
  [Source](fr_async/classes.py:53)
- [Yieldable](../classes/yieldable.md) — adapts a callable into a task factory.
  [Source](fr_async/classes.py:173)
- [File](../classes/file.md) — wraps a text or binary file and creates yielded
  read/write messages. [Source](fr_async/classes.py:193)
- [TaskGroup](../classes/task-group.md) — coordinates a collection of tasks.
  [Source](fr_async/classes.py:261)

## Package-level exports

`fr_async.__init__` re-exports `Task`, `Yieldable`, `File`, and `TaskGroup`.
`Assignable` is available from `fr_async.classes`, but is not re-exported at
the package root.
