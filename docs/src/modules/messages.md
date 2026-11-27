# `fr_async.messages`

Message objects are yielded by tasks to request work from the event loop.
Registration messages arrange for the loop to resume a task when an event
occurs; execution messages run their blocking operation in the loop's thread
pool.

## Exported classes

The module's `__all__` contains the following classes:

- [Message](../classes/message.md) — base class for yielded operations.
  [Source](fr_async/messages.py:40)
- [BackgroundMessage](../classes/background-message.md) — message with
  detached/background execution support. [Source](fr_async/messages.py:87)
- [Messages](../classes/messages.md) — groups messages that a task waits for
  together. [Source](fr_async/messages.py:105)
- [Sleep](../classes/sleep.md) — waits for a monotonic-clock deadline.
  [Source](fr_async/messages.py:114)
- [WaitReadable](../classes/wait-readable.md) — waits for socket readability.
  [Source](fr_async/messages.py:147)
- [WaitWritable](../classes/wait-writable.md) — waits for socket writability.
  [Source](fr_async/messages.py:162)
- [Accept](../classes/accept.md) — accepts all currently queued connections.
  [Source](fr_async/messages.py:177)
- [ToThread](../classes/to-thread.md) — runs a callable in the thread pool.
  [Source](fr_async/messages.py:209)
- [AsCompleted](../classes/as-completed.md) — exposes thread-pool results in
  completion order. [Source](fr_async/messages.py:240)
- [TaskJoin](../classes/task-join.md) — waits for one task and returns its
  result. [Source](fr_async/messages.py:308)
- [TaskGroupCollect](../classes/task-group-collect.md) — collects task results
  in input order. [Source](fr_async/messages.py:331)
- [TaskGroupAsCompleted](../classes/task-group-as-completed.md) — exposes task
  results in completion order. [Source](fr_async/messages.py:368)
- [FileRead](../classes/file-read.md) — performs a file read in the thread
  pool. [Source](fr_async/messages.py:426)
- [FileWrite](../classes/file-write.md) — performs a file write in the thread
  pool. [Source](fr_async/messages.py:441)

## Additional public module names

These names are defined without a leading underscore, but are intentionally
omitted from `__all__`:

- [MessageWithSocket](../classes/message-with-socket.md) is the shared base
  for socket messages. [Source](fr_async/messages.py:141)
- `check_resolved(blockers: deque[Message]) -> bool` removes resolved messages
  from the front of a blocker queue and reports whether it is empty.
  [Source](fr_async/messages.py:17)
- `register_event(message: MessageWithSocket, task: Task, event: int)`
  registers a socket message with the current loop's selector.
  [Source](fr_async/messages.py:28)

The helpers are implementation-facing; normal applications should yield the
message classes rather than call these helpers directly.

`fr_async.__init__` imports the names in this module's `__all__` into the
package namespace. The package itself has a narrower `__all__`, so explicit
imports such as `from fr_async import Sleep` work, but `from fr_async import *`
does not include message classes.

## Message lifecycle

A task yields a message. The loop calls `register()` for registration-based
messages or submits `execute()` for execution-based messages. When the message
is resolved, the task becomes eligible to run again. The exact value sent back
into the generator is defined by the message type and its typing annotation.
