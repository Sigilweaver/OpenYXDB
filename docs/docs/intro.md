---
sidebar_position: 1
slug: /
---

# OpenYXDB

OpenYXDB is a Python and C++ library for reading and writing Alteryx YXDB files.
It is built on [Alteryx's official open-source YXDB implementation](https://github.com/alteryx/OpenYXDB)
(GPL-3.0) and extends it with cross-platform support, bug fixes, and
first-class integration with PyArrow, Pandas, Polars, and DuckDB.

## What is YXDB?

YXDB is the native binary format used by Alteryx Designer. It is row-oriented:
each file contains UTF-16 XML metadata describing the schema, followed by
LZF-compressed blocks of records, and a block index at the end for random
access. YXDB stores up to 17 distinct field types covering booleans, integers,
floating-point numbers, fixed-decimal values, strings (narrow and wide, fixed
and variable-length), dates, times, blobs, and spatial objects.

This library reads both on-disk variants of YXDB (the original layout used
by the classic engine and the newer layout emitted by the AMP engine). The
correct decoder is selected automatically based on the file's magic bytes.
Writes produce the original layout.

## What changed from the Alteryx original

| Change | Detail |
| --- | --- |
| Cross-platform support | Builds and runs on Linux, macOS, and Windows. The original targeted Windows only. |
| Python bindings | Via nanobind, published to PyPI as `openyxdb`. |
| PyArrow / Pandas / Polars / DuckDB integration | Automatic type mapping for round-trip I/O. |
| Bug fixes | Block index was never written to disk (broke files with more than 65,536 records); reference-counting crash on GCC. |
| Modern CMake | 3.20+, target-based; pixi for dependency management; Catch2 test suite; GitHub Actions CI. |

## Feature highlights

- **High-level API** -- `read_yxdb`, `write_yxdb`, `to_pyarrow`, `from_pyarrow`, `to_pandas`, `from_pandas`, `to_polars`, `from_polars`
- **Polars IO plugin** -- `scan_yxdb` returns a lazy `LazyFrame` with projection and row-limit pushdown; `pl.read_yxdb` / `pl.scan_yxdb` / `df.yxdb.write` / `lf.yxdb.sink` are monkey-patched on import
- **Streaming write** -- `sink_yxdb(lf, path)` executes a Polars lazy plan on the streaming engine and writes in chunks without buffering the whole file
- **DuckDB integration** -- register YXDB files as SQL views, query them, and write DuckDB query results to YXDB
- **Low-level API** -- `Reader` / `Writer` / `FieldInfo` classes for fine-grained control
- **100% corpus pass rate** -- validated against 1,012 real-world E1 YXDB files

## Next steps

- [Install](./install) -- get the package
- [Quickstart](./quickstart) -- read and write your first file
- [Reading](./guide/reading) -- all read paths
- [Writing](./guide/writing) -- all write paths
- [Polars integration](./guide/polars) -- lazy scans, streaming sinks, namespace plugins
- [DuckDB integration](./guide/duckdb) -- SQL over YXDB files
- [Field types](./guide/field-types) -- all 17 YXDB types and their Python/Arrow mappings
