# `fr_async.utils`

Small logging helper used internally by the runtime.

## Public names

### `DEBUG`

A boolean evaluated when the module is imported. It is `True` when the
`FR_ASYNC_DEBUG` environment variable exists, regardless of its value.

### `log`

```python
def log(*args: Any, **kwargs: Any): ...
```

Calls `print(*args, **kwargs)` only when `DEBUG` is true. The function is
available from `fr_async.utils`; it is not re-exported at the package root.

[Source](fr_async/utils.py:5)
