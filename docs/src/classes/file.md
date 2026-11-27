# `File`

Wraps a text or binary file and exposes read/write operations as messages that
can be yielded by a task. Seeking is synchronous.

**Source:** [fr_async/classes.py:193](fr_async/classes.py:193)

## Type parameter

`File[T]` is constrained to `str` or `bytes`:

- `File[str]` represents a text file.
- `File[bytes]` represents a binary file.

## Constructor

```python
File(
    path: FilePath,
    mode: OpenMode,
    buffering: int = -1,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
    closefd: bool = True,
    opener: Callable[[str, int], int] | None = None,
)
```

The arguments are forwarded to Python's built-in `open()`. The typing
implementation provides separate overloads for text and binary modes.

## Public methods

### `read`

```python
def read(self, n: int = -1, *, background: bool = False) -> FileRead[T]: ...
```

Creates a [FileRead](file-read.md) message. Yield the returned message to read
from the file's current position. `n=-1` requests all remaining data.

### `write`

```python
def write(self, data: T, *, background: bool = False) -> FileWrite[T]: ...
```

Creates a [FileWrite](file-write.md) message. Yield the returned message to
write the supplied text or bytes. The data type must match the file mode.

### `seek`

```python
def seek(self, offset: int, whence: int = 0) -> int: ...
```

Moves the file cursor synchronously and returns the new position.

### `delete`

```python
def delete(self): ...
```

Removes the file path associated with the open file from the filesystem.

### Context manager methods

`__enter__()` returns the `File` instance. `__exit__()` closes the underlying
file. Use a `with` statement to ensure it is closed.

## Example

```python
from fr_async import File

def read_text(path: str):
    with File(path, "r") as file:
        contents = yield file.read()
    return contents
```

The read and write messages use the event loop's thread pool, so file I/O does
not block the event-loop thread while the operation is running.
