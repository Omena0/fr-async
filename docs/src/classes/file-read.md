# `FileRead`

An execution message that reads from an already-open text or binary file in
the event loop's thread pool.

**Source:** [fr_async/messages.py:426](fr_async/messages.py:426)

## Constructor

```python
class FileRead[T: (str, bytes)](BackgroundMessage, YieldTypeIs[T]):
    def __init__(
        self,
        file: IO[T],
        n: int = -1,
        *,
        background: bool = False,
    ):
        ...
```

`file` is the file object to read, and `n` is passed to its `read()` method.
The default `n=-1` reads all remaining data. `background=True` enables
detached execution.

## Public method

### `execute`

Calls `file.read(n)` and returns the text or bytes read.

Applications usually obtain this message through
[`File.read()`](file.md) rather than constructing it directly.
