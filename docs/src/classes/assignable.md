# `Assignable`

A small attribute container backed by a dictionary. Assigning an attribute
stores it in the internal mapping; reading an unknown attribute raises
`AttributeError`.

**Source:** [fr_async/classes.py:21](fr_async/classes.py:21)

## Constructor

```python
Assignable() -> None
```

Creates an empty store.

## Public methods

### `update`

```python
def update(self, data: dict[Any, Any]): ...
```

Merges the supplied mapping into the attribute store. Existing keys are
overwritten. Values can subsequently be read or assigned as attributes.

## Example

```python
data = Assignable()
data.worker_count = 4
data.update({"priority": 10})

assert data.worker_count == 4
assert data.priority == 10
```

`Task.data` uses this class to expose per-task scheduler metadata and other
user-defined values as attributes.
