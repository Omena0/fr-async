# `fr_async.loop`

Contains the event-loop implementation and the accessor for the most recently
created loop.

## Public classes

- [ExecutionLoop](../classes/execution-loop.md) — runs tasks, handles yielded
  messages, and coordinates timers, sockets, and thread-pool completions.
  [Source](fr_async/loop.py:70)

## Public functions

### `getEventLoop`

```python
def getEventLoop(): ...
```

Returns the module-level loop instance. Raises
[`NoEventLoopException`](../classes/no-event-loop-exception.md) when no
`ExecutionLoop` has been constructed.

[Source](fr_async/loop.py:505)

## Notes

Constructing an `ExecutionLoop` replaces the module-level loop reference used
by messages. The implementation is designed around that shared reference,
not independent concurrently active loops.
