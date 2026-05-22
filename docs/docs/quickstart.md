---
sidebar_position: 3
---

# Quickstart

## Read a file

```python
import openyxdb

# As a PyArrow Table
table = openyxdb.to_pyarrow("data.yxdb")

# As a Pandas DataFrame
df = openyxdb.to_pandas("data.yxdb")

# As a Polars DataFrame
df = openyxdb.to_polars("data.yxdb")
```

## Write a file

```python
import openyxdb
import polars as pl

df = pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]})
openyxdb.from_polars(df, "output.yxdb")
```

## Lazy scan with Polars

Importing `openyxdb` monkey-patches Polars with YXDB support automatically:

```python
import polars as pl
import openyxdb

# Lazy scan -- only requested columns are decoded from disk
df = pl.scan_yxdb("data.yxdb").select("id", "score").filter(pl.col("score") > 90).collect()

# Eager read
df = pl.read_yxdb("data.yxdb")
```

## Write from a lazy plan

```python
import polars as pl
import openyxdb

lf = pl.scan_yxdb("data.yxdb").filter(pl.col("score") > 50)
lf.yxdb.sink("filtered.yxdb")
```

## DuckDB

```python
import duckdb, openyxdb

con = duckdb.connect()
openyxdb.register_duckdb(con, "yx", "data.yxdb")
result = con.execute("SELECT COUNT(*) FROM yx WHERE score > 90").fetchone()
print(result)
```

## Next

- [Reading](./guide/reading) -- all read paths and the high-level API
- [Writing](./guide/writing) -- all write paths
- [Polars integration](./guide/polars) -- lazy scans, streaming sinks, and namespace plugins
- [DuckDB integration](./guide/duckdb) -- SQL over YXDB files
- [Field types](./guide/field-types) -- all 17 YXDB types and their Python/Arrow mappings
