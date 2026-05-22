---
sidebar_position: 7
---

# DuckDB integration

OpenYXDB provides helpers to use YXDB files as data sources in DuckDB queries
and to write DuckDB query results back to YXDB.

The integration works through Arrow interop: YXDB files are loaded into Arrow
tables and registered on the DuckDB connection. A native DuckDB table function
(`SELECT * FROM read_yxdb(...)`) would require a C++ DuckDB extension, which
is out of scope for this package.

## Setup

```sh
pip install openyxdb[duckdb]
```

## Register a file as a SQL view

`register_duckdb(con, name, path)` registers a YXDB file as a named view on
an existing connection:

```python
import duckdb, openyxdb

con = duckdb.connect()
openyxdb.register_duckdb(con, "sales", "sales.yxdb")

result = con.execute("SELECT region, SUM(amount) FROM sales GROUP BY region").df()
```

## Get a DuckDB relation

`to_duckdb(path, con=None, table_name=None)` loads the file into a DuckDB
connection and returns a relation. If `table_name` is provided, the relation
is also registered as a view:

```python
import duckdb, openyxdb

con = duckdb.connect()
rel = openyxdb.to_duckdb("data.yxdb", con=con, table_name="yx")

# Use the relation directly
print(rel.query("yx", "SELECT COUNT(*) FROM yx").fetchone())

# Or use SQL now that it's registered
con.execute("SELECT * FROM yx LIMIT 5").fetchdf()
```

## Write a DuckDB query to YXDB

`from_duckdb(source, path, con=None, chunk_size=None)` accepts a SQL string or
a `DuckDBPyRelation` and writes the result to a YXDB file:

```python
import duckdb, openyxdb

con = duckdb.connect()
openyxdb.register_duckdb(con, "yx", "data.yxdb")

# Write a filtered query to a new YXDB file
openyxdb.from_duckdb(
    "SELECT id, score FROM yx WHERE score > 90",
    "top_scores.yxdb",
    con=con,
)
```

You can also pass a relation object:

```python
rel = con.execute("SELECT id, score FROM yx WHERE score > 90")
openyxdb.from_duckdb(rel, "top_scores.yxdb")
```

## In-memory workflow

Connect without a file to work entirely in memory:

```python
import duckdb, openyxdb

con = duckdb.connect(":memory:")
openyxdb.register_duckdb(con, "orders", "orders.yxdb")
openyxdb.register_duckdb(con, "customers", "customers.yxdb")

openyxdb.from_duckdb(
    "SELECT o.*, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
    "enriched_orders.yxdb",
    con=con,
)
```
