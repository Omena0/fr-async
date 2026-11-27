# fr-async

`fr-async` is a cooperative asynchronous runtime built around Python
**generators** and **yielded messages**, rather than `async` and `await`.
A task yields a message to request an operation; the event loop registers or
executes that operation and resumes the task when the message is ready.

The project requires Python 3.15 or newer.

## First run

```python
from fr_async import ExecutionLoop, Yieldable
from fr_async.messages import Sleep

def main():
    yield Sleep(0.25)
    print("finished")

ExecutionLoop().run(Yieldable(main))
```

`Yieldable` adapts the generator function into a `Task`. `ExecutionLoop.run()`
then drives it until no scheduled or pending work remains.

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

### Bundled tool

- [HTTP benchmark](modules/http-bench.md) — Linux `epoll` HTTP benchmark
  client shipped as `fr_async.http_bench`.

Class pages are grouped under the relevant module pages and in the sidebar.
The reference documents the importable runtime surface; it excludes tests and
example programs.
