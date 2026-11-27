# `fr_async.typing`

Generic markers and aliases used by the package's type annotations.

## Public classes

- [YieldTypeIs](../classes/yield-type-is.md) — declares the value type
  returned when a message is yielded. [Source](fr_async/typing.py:8)
- [ReturnsAfter](../classes/returns-after.md) — describes a value becoming
  available after a duration. [Source](fr_async/typing.py:24)

## Public type aliases

```python
type LiteralType[T] = Annotated[T, _LiteralType()]
type FilePath = str | PathLike[str]
type OpenTextMode = Literal['r', 'w', 'a', 'x', 'r+', 'w+', 'a+', 'x+']
type OpenBinaryMode = Literal[
    'rb', 'wb', 'ab', 'xb', 'rb+', 'wb+', 'ab+', 'xb+'
]
type OpenMode = OpenTextMode | OpenBinaryMode
```

- `LiteralType[T]` is an `Annotated` marker used to preserve a literal type
  in supported type-checker configurations.
- `FilePath` is a string or `PathLike[str]`.
- `OpenTextMode` and `OpenBinaryMode` describe the accepted text and binary
  modes used by [File](../classes/file.md).
- `OpenMode` is their union.

`YieldTypeIs` and `LiteralType` rely on custom behavior implemented in the
project's [basedpyright fork](https://github.com/Omena0/basedpyright). A
standard type checker may treat the markers as ordinary annotations without
applying those custom inference rules.
