---
sidebar_position: 4
---

# Reading

OpenYXDB provides several read paths depending on how you want to consume the
data.

:::tip Format auto-detection

The reader inspects the file's magic bytes at open time and selects the right
decoder, so all the APIs below work transparently against either on-disk
layout (see [Format / Overview](../format/overview.md) for details). You can
inspect which one was detected via `Reader(...).format`, which returns
either `"E1"` (original) or `"E2"` (AMP-engine) for diagnostic purposes.

Note that the newer layout uses variable-length records, so on those files
`read_columns_subset(offset=...)` decodes sequentially from the start rather
than seeking.

:::

## High-level functions

### `to_pyarrow(path)`

Returns a `pyarrow.Table`:

```python
import openyxdb

table = openyxdb.to_pyarrow("data.yxdb")
print(table.schema)
print(table.num_rows)
```

### `to_pandas(path)`

Returns a `pandas.DataFrame`:

```python
import openyxdb

df = openyxdb.to_pandas("data.yxdb")
print(df.dtypes)
print(df.head())
```

### `to_polars(path)`

Returns a `polars.DataFrame` (eager, full file):

```python
import openyxdb

df = openyxdb.to_polars("data.yxdb")
print(df.schema)
```

### `read_yxdb(path)`

Returns a `dict[str, list[Any]]` of column name to Python list. Useful when
you do not need a dataframe and want the raw Python values:

```python
import openyxdb

columns = openyxdb.read_yxdb("data.yxdb")
print(columns.keys())
print(columns["score"][:5])
```

## Lazy scan with Polars

For large files, use `scan_yxdb` (or `pl.scan_yxdb` after importing
`openyxdb`) to avoid loading the whole file into memory. Only the columns and
rows you actually request are decoded from disk:

```python
import polars as pl
import openyxdb

lf = pl.scan_yxdb("data.yxdb")
df = lf.select("id", "name", "score").filter(pl.col("score") > 70).collect()
```

See [Polars integration](./polars) for full pushdown details.

## Schema inspection

Use the low-level `Reader` to inspect schema without reading data:

```python
from openyxdb import Reader

with Reader("data.yxdb") as r:
    for field in r.fields:
        print(field.name, field.type, field.size)
    print(r.num_records)
```

## Reading specific columns

`Reader.read_columns_subset` decodes only the requested columns, starting at
an optional offset and up to an optional limit:

```python
from openyxdb import Reader

with Reader("data.yxdb") as r:
    columns = r.read_columns_subset(["id", "score"], offset=0, limit=1000)
```

This is the same primitive used by `scan_yxdb` for projection and row-limit
pushdown.
