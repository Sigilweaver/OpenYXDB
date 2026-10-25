"""Reader and Writer over the compiled core."""

from __future__ import annotations

import os
import warnings as _warnings
from collections.abc import Mapping, Sequence
from typing import Any

from openyxdb import _openyxdb as _core
from openyxdb._openyxdb import FieldInfo, FormatError, SchemaError
from openyxdb._table import Table
from openyxdb._types import (
    Diagnostic,
    Leniency,
    OpenYXDBWarning,
    ReadOptions,
    WarningKind,
    WriteOptions,
)


def _emit(sink: list[Diagnostic], diagnostic: Diagnostic) -> None:
    sink.append(diagnostic)
    _warnings.warn(diagnostic.message, OpenYXDBWarning, stacklevel=3)


class Reader:
    """Read a YXDB file (E1 or E2) as a context manager.

    >>> with Reader("data.yxdb") as r:
    ...     table = r.read()

    Pass ``options=ReadOptions(...)`` to relax checks or choose the narrow
    string encoding. Strict is the default.
    """

    def __init__(
        self, path: str | os.PathLike, *, options: ReadOptions | None = None
    ) -> None:
        self._options = options if options is not None else ReadOptions()
        self._warnings: list[Diagnostic] = []
        self._raw = _core.Reader(
            os.fspath(path),
            self._options.allow_unverified_types,
            self._options.narrow_encoding,
        )
        try:
            self._check_field_names()
        except BaseException:
            self._raw.close()
            raise

    def _check_field_names(self) -> None:
        # The core renames duplicate, empty and over-long E1 field names when it
        # loads a schema. Compare with the names stored in the file.
        stored = list(self._raw.file_field_names)
        current = [f.name for f in self._raw.schema]
        if len(stored) != len(current):
            raise FormatError(
                f"schema lists {len(stored)} fields but {len(current)} were loaded"
            )
        for i, (was, now) in enumerate(zip(stored, current)):
            if was == now:
                continue
            msg = (
                f"field {i} is named {was!r} in the file and was renamed to "
                f"{now!r} (names must be unique ignoring case, non-empty and at "
                "most 255 characters)"
            )
            if not (self._options.leniency & Leniency.RENAME_DUPLICATE_FIELDS):
                raise SchemaError(
                    msg + "; pass ReadOptions(leniency=Leniency.RENAME_DUPLICATE_FIELDS) "
                    "to keep the renamed fields"
                )
            _emit(
                self._warnings,
                Diagnostic(
                    kind=WarningKind.RENAMED_FIELD,
                    code=WarningKind.RENAMED_FIELD.value,
                    message=msg,
                    field=now,
                ),
            )

    # -- description -------------------------------------------------------- #

    @property
    def fields(self) -> list[FieldInfo]:
        return list(self._raw.schema)

    @property
    def schema(self) -> list[FieldInfo]:
        """Alias of :attr:`fields`."""
        return self.fields

    @property
    def num_records(self) -> int:
        return self._raw.num_records

    @property
    def format(self) -> str:
        """``"E1"`` (original) or ``"E2"`` (AMP engine)."""
        return self._raw.format

    @property
    def warnings(self) -> list[Diagnostic]:
        """Everything lenient options relaxed so far."""
        return list(self._warnings)

    # -- reading ------------------------------------------------------------ #

    def read(
        self,
        columns: Sequence[str] | None = None,
        *,
        offset: int = 0,
        limit: int | None = None,
    ) -> Table:
        """Read records into a :class:`Table`.

        ``columns`` selects and orders columns by name; ``offset`` and ``limit``
        slice the records. An ``offset`` past the end returns an empty table.
        """
        if offset < 0:
            raise ValueError("offset must be >= 0")
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0 or None")
        data = self._raw.read_columns_subset(
            list(columns) if columns is not None else None,
            offset,
            -1 if limit is None else limit,
        )
        by_name = {f.name: f for f in self._raw.schema}
        names = list(data.keys())
        return Table(
            [by_name[n] for n in names],
            [data[n] for n in names],
            self._warnings,
        )

    def read_records(self) -> list[dict[str, Any]]:
        """All records as a list of dicts keyed by field name."""
        return [dict(r) for r in self._raw.read_records()]

    # -- lifecycle ---------------------------------------------------------- #

    @property
    def closed(self) -> bool:
        return self._closed

    _closed = False

    def close(self) -> None:
        self._raw.close()
        self._closed = True

    def __enter__(self) -> "Reader":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class Writer:
    """Write a YXDB file.

    The file appears at ``path`` when the writer closes cleanly. If the
    ``with`` block raises, the output is discarded.
    """

    def __init__(
        self,
        path: str | os.PathLike,
        schema: Sequence[FieldInfo],
        *,
        options: WriteOptions | None = None,
    ) -> None:
        self._options = options if options is not None else WriteOptions()
        self._warnings: list[Diagnostic] = []
        self._raw = _core.Writer(
            os.fspath(path), list(schema), self._options.narrow_encoding
        )

    @property
    def warnings(self) -> list[Diagnostic]:
        """Everything lenient options relaxed so far (none can be relaxed yet)."""
        return list(self._warnings)

    def write_record(self, row: Mapping[str, Any]) -> None:
        self._raw.write_record(dict(row))

    def write_columns(self, columns: Mapping[str, Sequence[Any]] | Table) -> None:
        """Write columns keyed by name, or a :class:`Table`."""
        if isinstance(columns, Table):
            columns = columns.to_dict()
        self._raw.write_columns(
            {k: (v if isinstance(v, list) else list(v)) for k, v in columns.items()}
        )

    @property
    def closed(self) -> bool:
        return self._closed

    _closed = False

    def close(self) -> None:
        self._raw.close()
        self._closed = True

    def abort(self) -> None:
        """Discard the output without finalizing it."""
        self._raw.abort()
        self._closed = True

    def __enter__(self) -> "Writer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self.abort()
        else:
            self.close()
