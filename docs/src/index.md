# fr-async

`fr-async` is a cooperative asynchronous runtime built around Python
**generators** and **yielded messages**, rather than `async` and `await`.
A task yields a message to request an operation; the event loop registers or
executes that operation and resumes the task when the message is ready.

The project requires Python 3.15 or newer.

## First run

```python
from fr_async import ExecutionLoop, Yieldable, Sleep

def main():
    yield Sleep(0.25)
    print("finished")

loop = ExecutionLoop()
loop.run(Yieldable(main))
```

`Yieldable` adapts the generator function into a `Task`. `ExecutionLoop.run()`
then drives it until no scheduled or pending work remains.

## Philosophy

1. Cooperative scheduling: A `Task` is never interrupted by force. It can only be stopped at a `yield`
2. Explicit execution loop: The `ExecutionLoop` should always live in user code, never created implicitly.
3. Explicit background: Everything that can run in the background should block by default. Require `background=True` to run in the background.
4. Don't rewrite stdlib: If there is something in stdlib that does what we want, use that instead of rewriting it. Unless it needs to be specialized for our EventLoop.

## Reference

### Runtime modules

- [Package exports](modules/package.md) — names exposed by `fr_async`.
- [Classes](modules/classes.md) — tasks, task groups, callable wrappers, and
  file access.
- [Event loop](modules/loop.md) — execution, scheduling, and loop lookup.
- [Messages](modules/messages.md) — timers, sockets, thread-pool work, joins,
  and file operations.
- [Schedulers](modules/scheduler.md) — scheduler interface and EEVDF scheduler.
- [Exceptions](modules/exceptions.md) — runtime exception types.
- [Typing](modules/typing.md) — message typing markers and file aliases.
- [Utilities](modules/utils.md) — environment-controlled debug logging.

### Benchmarks

- [HTTP benchmark](modules/http-bench.md) — Linux `epoll` HTTP benchmark
  client shipped as `fr_async.http_bench`.
