# `ReturnsAfter`

A generic typing marker describing a value of type `T` that becomes available
after a duration or event represented by `Duration`.

**Source:** [fr_async/typing.py:24](fr_async/typing.py:24)

## Declaration

```python
class ReturnsAfter[Duration, T = None]:
    ...
```

Message classes use it inside `YieldTypeIs`, for example:

```python
class Sleep[T: float](
    BackgroundMessage,
    YieldTypeIs[ReturnsAfter[T, None]],
):
    ...
```

It describes the static type of a yielded result; it does not implement
waiting or scheduling at runtime.
