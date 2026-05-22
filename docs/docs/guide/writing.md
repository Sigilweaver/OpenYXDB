---
sidebar_position: 5
---

# Writing

OpenYXDB provides symmetric write paths for every read path.

## High-level functions

### `from_pyarrow(table, path, chunk_size=None)`

Writes a `pyarrow.Table` to a YXDB file. Arrow types are mapped to YXDB field
types automatically (see [Field types](./field-types)):

```python
import pyarrow as pa, openyxdb

table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]})
openyxdb.from_pyarrow(table, "output.yxdb")
```

For very large tables, pass `chunk_size` to control how many rows are written
per block:

```python
openyxdb.from_pyarrow(large_table, "output.yxdb", chunk_size=65_536)
```

### `from_pandas(df, path, chunk_size=None)`

```python
import pandas as pd, openyxdb

df = pd.DataFrame({"x": [1.0, 2.0, 3.0], "label": ["a", "b", "c"]})
openyxdb.from_pandas(df, "output.yxdb")
```

### `from_polars(df, path)`

```python
import polars as pl, openyxdb

df = pl.DataFrame({"x": [1, 2, 3], "y": [4.0, 5.0, 6.0]})
openyxdb.from_polars(df, "output.yxdb")
```

### `write_yxdb(path, columns, schema=None)`

Writes a `dict[str, list]` of Python lists to a YXDB file. Schema is inferred
from the data when not provided:

```python
import openyxdb

openyxdb.write_yxdb("output.yxdb", {
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Carol"],
    "active": [True, False, True],
})
```

To override the inferred schema, pass a list of `FieldInfo` objects:

```python
from openyxdb import FieldInfo, Writer

fi = FieldInfo()
fi.name = "score"
fi.type = "Double"
fi.size = 8
fi.scale = 0

openyxdb.write_yxdb("output.yxdb", {"score": [1.5, 2.5, 3.5]}, schema=[fi])
```

## Streaming write from a Polars lazy plan

`sink_yxdb(lf, path)` executes a lazy plan and writes the result in chunks
without buffering the full output in Python:

```python
import polars as pl, openyxdb

lf = pl.scan_yxdb("source.yxdb").filter(pl.col("active") == True)
openyxdb.sink_yxdb(lf, "active_only.yxdb", chunk_size=65_536)
```

The namespace shorthand is equivalent:

```python
lf.yxdb.sink("active_only.yxdb")
```

:::note Polars streaming limitation

Polars 1.x does not expose a public plugin API for custom `sink_*` formats, so
`sink_yxdb` still performs a single `collect()` before chunk-writing. The
chunking reduces the peak Python-side buffer to `chunk_size` rows, but the
query result must fully materialize first. True per-batch push sinks require an
upstream Polars API.

:::

## Low-level write

Use `Writer` directly when you need full control over the schema:

```python
from openyxdb import Writer, FieldInfo

fields = []
for name, typ, size in [("id", "Int32", 4), ("label", "V_WString", 262144)]:
    fi = FieldInfo()
    fi.name = name
    fi.type = typ
    fi.size = size
    fi.scale = 0
    fields.append(fi)

with Writer("output.yxdb", fields) as w:
    w.write_columns({"id": [1, 2, 3], "label": ["a", "b", "c"]})
```
