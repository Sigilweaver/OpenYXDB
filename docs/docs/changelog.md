---
sidebar_position: 98
---

# Changelog

The canonical changelog lives at
[`CHANGELOG.md`](https://github.com/Sigilweaver/OpenYXDB/blob/main/CHANGELOG.md)
in the repository root. The notes below mirror the latest releases.

## 1.3.0 - 2026-04-21

- **`sink_yxdb(lf, path, chunk_size=..., engine="streaming")`** -- streaming-friendly write from a Polars `LazyFrame`. Executes on Polars' streaming engine and writes to disk in chunks.
- **`df.yxdb.write(..., chunk_size=...)` / `lf.yxdb.sink(..., chunk_size=..., engine=...)`** -- Polars namespace methods now forward `chunk_size` to the batched writer.
- **DuckDB integration** -- `to_duckdb`, `register_duckdb`, and `from_duckdb` helpers. New `duckdb` optional extra.
- **`from_pyarrow(..., chunk_size=...)` / `from_pandas(..., chunk_size=...)`** -- optional chunked write paths for very large tables.
- PyArrow is now a required runtime dependency (was optional).
- Full E1 corpus (1,012 files) validated under sink and DuckDB paths. Full suite: 1,040 passed.

## 1.2.0 - 2026-04-18

- **Real pushdown for `scan_yxdb`** -- true projection and row-limit pushdown at the C++ reader. `scan_yxdb(path).head(1000).collect()` on a 126 MB / 336k-row file drops from ~770 ms to ~2 ms.
- **`Reader.read_columns_subset(columns, offset, limit)`** -- new low-level binding for partial reads.
- **Batch streaming in `scan_yxdb`** -- the IO source yields chunks (default 65,536 rows) instead of one large frame.
- Fixed: `scan_yxdb` no longer re-decodes the whole file on every collect.

## 1.1.0 - 2026-04-18

- Python 3.14 wheel builds.
- `openyxdb.help()` -- prints quick-start examples and the full list of supported field types.
- `python -m openyxdb` -- module entry point; `--version` flag.

## 1.0.2 - 2026-04-04

- C++ exception translation -- C++ errors now produce a Python `RuntimeError` with the original message.
- UTF-8 filename support on Linux.
- UTF-16 surrogate pair handling.
- Non-UTF-8 narrow string field fallback (Latin-1).
- Zero-record file crash fix.
- Validated against 1,012 real-world E1 YXDB files -- 100% pass rate.

## 1.0.1 - 2026-04-04

- Polars IO plugin -- `scan_yxdb()` with projection, predicate, and row-limit pushdown.
- Polars namespace plugins -- `df.yxdb.write(path)` and `lf.yxdb.sink(path)`.
- Top-level Polars aliases -- `pl.read_yxdb` and `pl.scan_yxdb`.

## 1.0.0 - 2026-04-03

First production release, forked from the dormant Alteryx repository.

- Python bindings via nanobind.
- High-level API: `read_yxdb`, `write_yxdb`, `to_pyarrow`, `from_pyarrow`, `to_pandas`, `from_pandas`, `to_polars`, `from_polars`.
- Low-level `Reader` / `Writer` / `FieldInfo` classes.
- Cross-platform C++ core (Linux, macOS, Windows).
- Block index write bug fixed (broke files with more than 65,536 records in the original Alteryx code).
- Modern CMakeLists.txt, Catch2 test suite, GitHub Actions CI.
