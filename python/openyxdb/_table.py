"""The result of a read."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from openyxdb._openyxdb import FieldInfo, SchemaError
from openyxdb._types import Diagnostic


class Column:
    """One column: its field description and its values."""

    __slots__ = ("field", "values")

    def __init__(self, field: FieldInfo, values: list[Any]) -> None:
        self.field = field
        self.values = values

    def __repr__(self) -> str:
        return f"Column({self.field.name!r}, {len(self.values)} values)"


class Table:
    """Columns read from a YXDB file, with their field descriptions.

    Columns keep file order and are addressed by position or, when the name is
    unambiguous, by name. ``warnings`` lists what lenient options relaxed while
    producing it.
    """

    def __init__(
        self,
        fields: Sequence[FieldInfo],
        columns: Sequence[list[Any]],
        warnings: Sequence[Diagnostic] = (),
        *,
        num_rows: int | None = None,
    ) -> None:
        if len(fields) != len(columns):
            raise ValueError("fields and columns must have the same length")
        lengths = {len(c) for c in columns}
        if len(lengths) > 1:
            raise ValueError("columns must all have the same length")
        derived = lengths.pop() if lengths else 0
        if num_rows is not None and columns and num_rows != derived:
            raise ValueError("num_rows does not match the columns")
        self._num_rows = derived if num_rows is None else num_rows
        self._columns = [Column(f, c) for f, c in zip(fields, columns)]
        self.warnings: list[Diagnostic] = list(warnings)

    # -- shape ------------------------------------------------------------- #

    @property
    def fields(self) -> list[FieldInfo]:
        return [c.field for c in self._columns]

    @property
    def columns(self) -> list[Column]:
        return list(self._columns)

    @property
    def column_names(self) -> list[str]:
        return [c.field.name for c in self._columns]

    @property
    def num_rows(self) -> int:
        return self._num_rows

    def __len__(self) -> int:
        return self.num_rows

    def __getitem__(self, key: int | str) -> list[Any]:
        if isinstance(key, int):
            return self._columns[key].values
        hits = [c for c in self._columns if c.field.name == key]
        if not hits:
            raise KeyError(key)
        if len(hits) > 1:
            raise SchemaError(
                f"{len(hits)} columns are named {key!r}; address them by position"
            )
        return hits[0].values

    def __repr__(self) -> str:
        return f"Table({self.num_rows} rows, columns={self.column_names})"

    # -- conversions -------------------------------------------------------- #

    def to_dict(self) -> dict[str, list[Any]]:
        """Columns keyed by name. Raises :class:`SchemaError` on duplicate names."""
        names = self.column_names
        dup = next((n for n in names if names.count(n) > 1), None)
        if dup is not None:
            raise SchemaError(
                f"more than one column is named {dup!r}; a dict keyed by name "
                "cannot hold them"
            )
        return {c.field.name: c.values for c in self._columns}

    def to_pyarrow(self):
        """Convert to a ``pyarrow.Table`` (requires pyarrow)."""
        return self._to_pyarrow(release=False)

    def _to_pyarrow(self, release: bool):
        import pyarrow as pa

        from openyxdb import _column_to_arrow, _yxdb_type_to_arrow

        fields = []
        arrays = []
        for i, col in enumerate(self._columns):
            fields.append(pa.field(col.field.name, _yxdb_type_to_arrow(col.field)))
            arrays.append(_column_to_arrow(col.field, col.values, pa))
            if release:
                # The caller is done with the Python values; free them as the
                # Arrow arrays take over.
                col.values = []
        return pa.table(arrays, schema=pa.schema(fields))

    def to_pandas(self):
        return self.to_pyarrow().to_pandas()

    def to_polars(self):
        import polars as pl

        return pl.from_arrow(self.to_pyarrow())

    def __arrow_c_stream__(self, requested_schema=None):
        """Arrow PyCapsule stream, so any Arrow consumer can read a Table."""
        return self.to_pyarrow().__arrow_c_stream__(requested_schema)
