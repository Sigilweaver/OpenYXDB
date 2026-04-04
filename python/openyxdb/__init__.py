"""OpenYXDB - Read and write Alteryx YXDB files.

Provides functions to convert between YXDB files and
PyArrow Tables, Pandas DataFrames, and Polars DataFrames.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from openyxdb._openyxdb import FieldInfo, Reader, Writer

__all__ = [
    "FieldInfo",
    "Reader",
    "Writer",
    "read_yxdb",
    "write_yxdb",
    "to_pyarrow",
    "to_pandas",
    "to_polars",
    "scan_yxdb",
    "from_pyarrow",
    "from_pandas",
    "from_polars",
]

# --------------------------------------------------------------------------- #
# Schema mapping helpers
# --------------------------------------------------------------------------- #

def _yxdb_type_to_arrow(fi: FieldInfo):
    """Map a YXDB FieldInfo to a PyArrow data type."""
    import pyarrow as pa

    t = fi.type
    if t == "Bool":
        return pa.bool_()
    if t == "Byte":
        return pa.uint8()
    if t == "Int16":
        return pa.int16()
    if t == "Int32":
        return pa.int32()
    if t == "Int64":
        return pa.int64()
    if t == "Float":
        return pa.float32()
    if t == "Double":
        return pa.float64()
    if t == "FixedDecimal":
        return pa.float64()
    if t in ("String", "WString", "V_String", "V_WString"):
        return pa.utf8()
    if t == "Date":
        return pa.date32()
    if t == "Time":
        return pa.time64("us")
    if t == "DateTime":
        return pa.timestamp("us")
    if t in ("Blob", "SpatialObj"):
        return pa.binary()
    return pa.utf8()


def _arrow_type_to_yxdb(name: str, dtype) -> FieldInfo:
    """Map a PyArrow field to a YXDB FieldInfo."""
    import pyarrow as pa

    fi = FieldInfo()
    fi.name = name
    fi.scale = 0

    if pa.types.is_boolean(dtype):
        fi.type = "Bool"
        fi.size = 1
    elif pa.types.is_uint8(dtype):
        fi.type = "Byte"
        fi.size = 1
    elif pa.types.is_int8(dtype) or pa.types.is_int16(dtype):
        fi.type = "Int16"
        fi.size = 2
    elif pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        fi.type = "Int32"
        fi.size = 4
    elif pa.types.is_int64(dtype) or pa.types.is_uint32(dtype) or pa.types.is_uint64(dtype):
        fi.type = "Int64"
        fi.size = 8
    elif pa.types.is_float16(dtype) or pa.types.is_float32(dtype):
        fi.type = "Float"
        fi.size = 4
    elif pa.types.is_float64(dtype):
        fi.type = "Double"
        fi.size = 8
    elif pa.types.is_date(dtype):
        fi.type = "Date"
        fi.size = 10
    elif pa.types.is_time(dtype):
        fi.type = "Time"
        fi.size = 8
    elif pa.types.is_timestamp(dtype):
        fi.type = "DateTime"
        fi.size = 19
    elif pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        fi.type = "Blob"
        fi.size = 0
    elif pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        fi.type = "V_WString"
        fi.size = 262144
    else:
        fi.type = "V_WString"
        fi.size = 262144
    return fi


# --------------------------------------------------------------------------- #
# Core read/write
# --------------------------------------------------------------------------- #

def read_yxdb(path: str | os.PathLike) -> dict[str, list[Any]]:
    """Read a YXDB file and return columnar data as a dict of lists."""
    with Reader(str(path)) as r:
        return dict(r.read_columns())


def write_yxdb(
    path: str | os.PathLike,
    columns: dict[str, list[Any]],
    schema: list[FieldInfo] | None = None,
) -> None:
    """Write columnar data to a YXDB file.

    If schema is not provided, it will be inferred from the data.
    """
    if schema is None:
        schema = _infer_schema(columns)
    with Writer(str(path), schema) as w:
        w.write_columns(columns)


def _infer_schema(columns: dict[str, list[Any]]) -> list[FieldInfo]:
    """Infer YXDB schema from Python column data."""
    schema = []
    for name, values in columns.items():
        fi = FieldInfo()
        fi.name = name
        fi.scale = 0
        # Find first non-None value
        sample = None
        for v in values:
            if v is not None:
                sample = v
                break
        if sample is None or isinstance(sample, str):
            fi.type = "V_WString"
            fi.size = 262144
        elif isinstance(sample, bool):
            fi.type = "Bool"
            fi.size = 1
        elif isinstance(sample, int):
            fi.type = "Int64"
            fi.size = 8
        elif isinstance(sample, float):
            fi.type = "Double"
            fi.size = 8
        elif isinstance(sample, bytes):
            fi.type = "Blob"
            fi.size = 0
        else:
            fi.type = "V_WString"
            fi.size = 262144
        schema.append(fi)
    return schema


# --------------------------------------------------------------------------- #
# PyArrow
# --------------------------------------------------------------------------- #

def to_pyarrow(path: str | os.PathLike):
    """Read a YXDB file and return a PyArrow Table."""
    import pyarrow as pa

    with Reader(str(path)) as r:
        schema_info = r.schema
        columns = r.read_columns()

    fields = []
    arrays = []
    for fi in schema_info:
        arrow_type = _yxdb_type_to_arrow(fi)
        col_data = list(columns[fi.name])

        # Convert date/time strings to proper types
        if fi.type == "Date":
            import datetime
            col_data = [
                datetime.date.fromisoformat(v) if v is not None else None
                for v in col_data
            ]
        elif fi.type == "Time":
            import datetime
            col_data = [
                datetime.time.fromisoformat(v) if v is not None else None
                for v in col_data
            ]
        elif fi.type == "DateTime":
            import datetime
            col_data = [
                datetime.datetime.fromisoformat(v) if v is not None else None
                for v in col_data
            ]

        fields.append(pa.field(fi.name, arrow_type))
        arrays.append(pa.array(col_data, type=arrow_type))

    return pa.table(arrays, schema=pa.schema(fields))


def from_pyarrow(table, path: str | os.PathLike) -> None:
    """Write a PyArrow Table to a YXDB file."""
    import pyarrow as pa

    schema = []
    for field in table.schema:
        schema.append(_arrow_type_to_yxdb(field.name, field.type))

    columns = {}
    for i, field in enumerate(table.schema):
        col = table.column(i)
        py_list = col.to_pylist()
        fi = schema[i]
        # Convert date/time objects to strings for YXDB
        if fi.type == "Date":
            py_list = [v.isoformat() if v is not None else None for v in py_list]
        elif fi.type == "Time":
            py_list = [v.isoformat() if v is not None else None for v in py_list]
        elif fi.type == "DateTime":
            py_list = [
                v.strftime("%Y-%m-%d %H:%M:%S") if v is not None else None
                for v in py_list
            ]
        columns[field.name] = py_list

    with Writer(str(path), schema) as w:
        w.write_columns(columns)


# --------------------------------------------------------------------------- #
# Pandas
# --------------------------------------------------------------------------- #

def to_pandas(path: str | os.PathLike):
    """Read a YXDB file and return a Pandas DataFrame."""
    import pandas as pd

    table = to_pyarrow(path)
    return table.to_pandas()


def from_pandas(df, path: str | os.PathLike) -> None:
    """Write a Pandas DataFrame to a YXDB file."""
    import pyarrow as pa

    table = pa.Table.from_pandas(df)
    from_pyarrow(table, path)


# --------------------------------------------------------------------------- #
# Polars
# --------------------------------------------------------------------------- #

def _yxdb_type_to_polars(fi: FieldInfo):
    """Map a YXDB FieldInfo to a Polars data type."""
    import polars as pl

    t = fi.type
    if t == "Bool":
        return pl.Boolean
    if t == "Byte":
        return pl.UInt8
    if t == "Int16":
        return pl.Int16
    if t == "Int32":
        return pl.Int32
    if t == "Int64":
        return pl.Int64
    if t == "Float":
        return pl.Float32
    if t == "Double":
        return pl.Float64
    if t == "FixedDecimal":
        return pl.Float64
    if t in ("String", "WString", "V_String", "V_WString"):
        return pl.String
    if t == "Date":
        return pl.Date
    if t == "Time":
        return pl.Time
    if t == "DateTime":
        return pl.Datetime("us")
    if t in ("Blob", "SpatialObj"):
        return pl.Binary
    return pl.String


def to_polars(path: str | os.PathLike):
    """Read a YXDB file and return a Polars DataFrame."""
    import polars as pl

    table = to_pyarrow(path)
    return pl.from_arrow(table)


def scan_yxdb(path: str | os.PathLike) -> "pl.LazyFrame":
    """Scan a YXDB file as a Polars LazyFrame (IO plugin).

    Supports projection pushdown, predicate pushdown, and row limit.
    Use ``.collect()`` to materialize the result.

    Example::

        import openyxdb
        df = openyxdb.scan_yxdb("file.yxdb").collect()
        df = openyxdb.scan_yxdb("file.yxdb").select("col_a", "col_b").head(100).collect()
    """
    import polars as pl
    from polars.io.plugins import register_io_source
    from typing import Iterator

    path_str = str(path)

    with Reader(path_str) as r:
        schema_info = r.schema

    pl_schema = pl.Schema(
        {fi.name: _yxdb_type_to_polars(fi) for fi in schema_info}
    )

    def _source(
        with_columns: list[str] | None,
        predicate: pl.Expr | None,
        n_rows: int | None,
        batch_size: int | None,
    ) -> Iterator[pl.DataFrame]:
        table = to_pyarrow(path_str)
        df = pl.from_arrow(table)

        if with_columns is not None:
            df = df.select(with_columns)
        if predicate is not None:
            df = df.filter(predicate)
        if n_rows is not None:
            df = df.head(n_rows)

        yield df

    return register_io_source(io_source=_source, schema=pl_schema)


def from_polars(df, path: str | os.PathLike) -> None:
    """Write a Polars DataFrame to a YXDB file."""
    table = df.to_arrow()
    from_pyarrow(table, path)
