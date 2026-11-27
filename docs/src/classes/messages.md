# `Messages`

A wrapper for an iterable of messages that a task should wait for together.
It is yielded once; the loop registers each contained message for the same
task.

**Source:** [fr_async/messages.py:105](fr_async/messages.py:105)

## Constructor

```python
Messages(messages: Iterable[Message])
```

Stores the iterable as supplied. The implementation does not eagerly convert
it to a tuple or list.

## Example

```python
from fr_async.messages import Messages, WaitReadable, WaitWritable

def exchange(sock):
    yield Messages([
        WaitReadable(sock),
        WaitWritable(sock),
    ])
```

This is a group of readiness messages for the same task; it does not combine
their results into a separate return value. Use only messages whose
registration behavior is appropriate to wait for together.
