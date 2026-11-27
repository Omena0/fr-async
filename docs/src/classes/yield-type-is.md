# `YieldTypeIs`

A generic marker used by message classes to describe the type delivered back
to a task when the message is yielded.

**Source:** [fr_async/typing.py:8](fr_async/typing.py:8)

## Declaration

```python
class YieldTypeIs[T = _InferYieldType]:
    ...
```

Concrete messages inherit from `YieldTypeIs[T]` to declare the type of the
value sent back into the yielding generator. When the type argument is left as
the default marker, the custom type-checker implementation can infer the value
type from the concrete subclass's first type parameter.

`YieldTypeIs` is a typing marker, not a runtime message or base class that
application code needs to instantiate. Its inference behavior is implemented
by the project's [basedpyright fork](https://github.com/Omena0/basedpyright).
