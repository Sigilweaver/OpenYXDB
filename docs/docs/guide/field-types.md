---
sidebar_position: 8
---

# Field types

YXDB supports 17 field types. The table below shows how each type maps to
Python and PyArrow.

## Type reference

| YXDB type | `FieldInfo.type` | PyArrow type | Python type | Notes |
| --- | --- | --- | --- | --- |
| Bool | `"Bool"` | `pa.bool_()` | `bool` | Single bit stored as a byte |
| Byte | `"Byte"` | `pa.uint8()` | `int` | Unsigned 8-bit integer |
| Int16 | `"Int16"` | `pa.int16()` | `int` | Signed 16-bit integer |
| Int32 | `"Int32"` | `pa.int32()` | `int` | Signed 32-bit integer |
| Int64 | `"Int64"` | `pa.int64()` | `int` | Signed 64-bit integer |
| Float | `"Float"` | `pa.float32()` | `float` | 32-bit IEEE 754 |
| Double | `"Double"` | `pa.float64()` | `float` | 64-bit IEEE 754 |
| FixedDecimal | `"FixedDecimal"` | `pa.float64()` | `float` | Stored as fixed-point; read as float64 |
| String | `"String"` | `pa.utf8()` | `str` | Fixed-width narrow string (Latin-1/UTF-8) |
| WString | `"WString"` | `pa.utf8()` | `str` | Fixed-width wide string (UTF-16) |
| V_String | `"V_String"` | `pa.utf8()` | `str` | Variable-length narrow string |
| V_WString | `"V_WString"` | `pa.utf8()` | `str` | Variable-length wide string (default for text) |
| Date | `"Date"` | `pa.date32()` | `str` | Stored as `YYYY-MM-DD` |
| Time | `"Time"` | `pa.time64("us")` | `str` | Stored as `HH:MM:SS` |
| DateTime | `"DateTime"` | `pa.timestamp("us")` | `str` | Stored as `YYYY-MM-DD HH:MM:SS` |
| Blob | `"Blob"` | `pa.binary()` | `bytes` | Variable-length binary blob |
| SpatialObj | `"SpatialObj"` | `pa.binary()` | `bytes` | SHP-encoded spatial object (read as raw bytes) |

## Null values

All 17 types support null values. Nulls are represented as `None` in Python
lists and as null entries in Arrow/Pandas/Polars columns.

## Arrow-to-YXDB mapping

When writing from PyArrow (or Pandas/Polars via Arrow), types are mapped
automatically:

| PyArrow type | YXDB type written |
| --- | --- |
| `bool_` | `Bool` |
| `uint8` | `Byte` |
| `int8`, `int16` | `Int16` |
| `int32`, `uint16` | `Int32` |
| `int64`, `uint32`, `uint64` | `Int64` |
| `float16`, `float32` | `Float` |
| `float64` | `Double` |
| `date32`, `date64` | `Date` |
| `time32`, `time64` | `Time` |
| `timestamp` | `DateTime` |
| `binary`, `large_binary` | `Blob` |
| `string`, `large_string`, other | `V_WString` |

## Schema inference from Python dicts

When writing via `write_yxdb` without an explicit schema, the type is inferred
from the first non-None value in each column:

| Python type | YXDB type written |
| --- | --- |
| `bool` | `Bool` |
| `int` | `Int64` |
| `float` | `Double` |
| `str` or `None` | `V_WString` |
| `bytes` | `Blob` |
| other | `V_WString` |

## FixedDecimal

`FixedDecimal` fields carry `size` (total digits) and `scale` (decimal places)
in their `FieldInfo`. When writing, use the `FieldInfo` class directly to set
these:

```python
from openyxdb import FieldInfo

fi = FieldInfo()
fi.name = "price"
fi.type = "FixedDecimal"
fi.size = 10   # total digits
fi.scale = 2   # decimal places
```

On read, `FixedDecimal` is returned as `float64` in PyArrow and as `float` in
Python. Precision beyond what `float64` can represent may be lost.

## Spatial objects

`SpatialObj` fields are returned as raw `bytes` (SHP-encoded binary). Parsing
the SHP geometry is not in scope for this library.
