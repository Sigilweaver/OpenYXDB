---
sidebar_position: 6
---

# Polars integration

Importing `openyxdb` automatically monkey-patches Polars with YXDB support
when Polars is installed. No additional setup is needed.

```python
import polars as pl
import openyxdb  # registers everything on import
```

## Top-level read functions

### `pl.read_yxdb(path)`

Eagerly reads a YXDB file and returns a `DataFrame`:

```python
df = pl.read_yxdb("data.yxdb")
```

### `pl.scan_yxdb(path)`

Returns a `LazyFrame`. Projection and row-limit pushdown are performed at the
C++ reader level -- only the columns and rows you request are decoded from
disk:

```python
lf = pl.scan_yxdb("data.yxdb")
df = lf.select("id", "score").filter(pl.col("score") > 90).collect()
```

## Pushdown support

| Optimization | Supported | Notes |
| --- | --- | --- |
| Projection pushdown | Yes | Only requested columns are decoded from disk. |
| Row-limit pushdown | Yes | `head(n)` / `fetch(n)` stops decoding once `n` rows are produced. |
| Batched streaming | Yes | Default 65,536 rows; honours Polars' `batch_size` hint. |
| Predicate pushdown | Partial | Predicates evaluate per batch after decode. YXDB has no per-block statistics, so genuine file-level predicate skipping is not possible. Combined with `head`, predicates still short-circuit once enough rows are collected. |

## DataFrame and LazyFrame namespace plugins

`df.yxdb.write(path)` -- write a `DataFrame` to a YXDB file:

```python
df.yxdb.write("output.yxdb")
df.yxdb.write("output.yxdb", chunk_size=65_536)
```

`lf.yxdb.sink(path)` -- execute a lazy plan and write to YXDB in chunks:

```python
lf.yxdb.sink("output.yxdb")
lf.yxdb.sink("output.yxdb", chunk_size=65_536)
```

## `openyxdb.scan_yxdb(path)`

The standalone `openyxdb.scan_yxdb` is the same as `pl.scan_yxdb` and is
available before Polars monkey-patching runs:

```python
import openyxdb
lf = openyxdb.scan_yxdb("data.yxdb")
```

## `openyxdb.sink_yxdb(lf, path, chunk_size=None, engine="streaming")`

The standalone sink function accepts either a `LazyFrame` or a `DataFrame`:

```python
import openyxdb, polars as pl

lf = pl.scan_csv("source.csv").select("id", "value")
openyxdb.sink_yxdb(lf, "output.yxdb", chunk_size=65_536)
```

## Manual registration

If `openyxdb` is imported before Polars is installed, or if you want to
re-register:

```python
openyxdb.register_polars()
```

## End-to-end example

```python
import polars as pl
import openyxdb

# Lazy scan -> filter -> project -> sink
(
    pl.scan_yxdb("sales.yxdb")
    .filter(pl.col("region") == "US")
    .select("order_id", "amount", "date")
    .yxdb.sink("us_sales.yxdb")
)

# Verify the output
print(pl.read_yxdb("us_sales.yxdb").head(5))
```
