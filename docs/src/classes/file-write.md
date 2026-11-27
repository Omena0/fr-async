# `FileWrite`

An execution message that writes text or bytes to an already-open file in the
event loop's thread pool.

**Source:** [fr_async/messages.py:441](fr_async/messages.py:441)

## Constructor

```python
class FileWrite[T: (str, bytes)](BackgroundMessage, YieldTypeIs[int]):
    def __init__(
        self,
        file: IO[T],
        data: T,
        *,
        background: bool = False,
    ):
        ...
```

The type of `data` must match the file's mode. The constructor raises
`ValueError` when a binary file is paired with `str` data or a text file with
`bytes` data.

## Public method

### `execute`

Calls `file.write(data)` and returns the number of values written.

Applications usually obtain this message through
[`File.write()`](file.md) rather than constructing it directly.
