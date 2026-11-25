from typing import Annotated, Any

class _InferYieldType: ...

class YieldTypeIs[T = _InferYieldType]:
    """Declares the type returned when this instance is yielded.

        If T is _InferYieldType, infer the result from the first
        type parameter of the concrete subclass.
    """

class _LiteralType:
    """Metadata marker for literal-preserving inference."""

type LiteralType[T] = Annotated[T, _LiteralType()]

class ReturnsAfter[Duration, T=None]:
    """Describes a value of type T becoming available after Duration."""

