# `fr_async.exceptions`

Runtime exceptions raised by loop lookup and task timeout handling.

## Public classes

- [NoEventLoopException](../classes/no-event-loop-exception.md) — raised when
  an operation needs a loop but none has been created.
  [Source](fr_async/exceptions.py:3)
- [TaskTimeoutError](../classes/task-timeout-error.md) — raised when a task's
  configured timeout expires. [Source](fr_async/exceptions.py:8)
