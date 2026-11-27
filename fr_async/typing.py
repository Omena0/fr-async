from typing import Annotated, Any

class _InferYieldType:
    __slots__ = ()


class YieldTypeIs[T = _InferYieldType]:
    __slots__ = ()

    """Declares the type returned when this instance is yielded.

        If T is _InferYieldType, infer the result from the first
        type parameter of the concrete subclass.
    """

class _LiteralType:
    __slots__ = ()

    """Metadata marker for literal-preserving inference."""

type LiteralType[T] = Annotated[T, _LiteralType()]

class ReturnsAfter[Duration, T=None]:
    __slots__ = ()

    """Describes a value of type T becoming available after Duration."""

