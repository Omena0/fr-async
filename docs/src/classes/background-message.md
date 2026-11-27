# `BackgroundMessage`

Base class for messages that support detached execution through the
`background` flag. It extends [Message](message.md).

**Source:** [fr_async/messages.py:87](fr_async/messages.py:87)

## Constructor

```python
BackgroundMessage(*, background: bool = False)
```

Initializes the message and stores the `background` flag. When a background
message is yielded, the loop arranges for it to run in a detached background
task while allowing the yielding task to continue without waiting for the
operation to finish.

## Public method

### `task_completed`

```python
def task_completed(self, target: Task, waiter: Task) -> None: ...
```

Resolves the message when its target task completes. The target's exception
is thrown into the waiter; otherwise its result is sent to the waiter.

Concrete background-capable messages include [Sleep](sleep.md),
[ToThread](to-thread.md), and [TaskJoin](task-join.md).
