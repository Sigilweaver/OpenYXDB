# Changelog

All notable changes to this project will be documented in this file.

## [1.3.0] - 2026-04-21

### Added
- **`openyxdb.sink_yxdb(lf, path, chunk_size=..., engine="streaming")`** — streaming-friendly write from a Polars `LazyFrame`. The plan is executed on Polars' streaming engine and written to disk in chunks so the YXDB writer never holds more than one batch in Python memory at a time. Accepts a `DataFrame` for convenience.
- **`df.yxdb.write(..., chunk_size=...)` / `lf.yxdb.sink(..., chunk_size=..., engine=...)`** — Polars namespace methods now forward `chunk_size` to the new batched writer. `lf.yxdb.sink` routes through `sink_yxdb`.
- **DuckDB integration** — three new top-level helpers:
    - `openyxdb.to_duckdb(path, con=None, table_name=None)` — load a YXDB file into a DuckDB connection as a relation (and optionally register it as a view).
    - `openyxdb.register_duckdb(con, name, path)` — register a YXDB file as a named view on an existing connection.
    - `openyxdb.from_duckdb(source, path, con=None, chunk_size=None)` — write a DuckDB SQL string or `DuckDBPyRelation` to a YXDB file (chunked). Normalises both `pyarrow.Table` and `pyarrow.RecordBatchReader` return shapes from `rel.arrow()` across DuckDB versions.

  New `duckdb` optional extra (`pip install openyxdb[duckdb]`) pulls in DuckDB + PyArrow. The `all` extra now includes DuckDB as well.
- **`from_pyarrow(table, path, chunk_size=...)`** and **`from_pandas(df, path, chunk_size=...)`** — optional chunked write paths for very large tables.

### Changed
- **`pyarrow` is now a required runtime dependency** (previously only an optional extra, but every high-level read/write function required it). This makes the published wheels self-sufficient for the common read/write paths.
- Arrow → YXDB write is factored through `_arrow_schema_to_yxdb` / `_arrow_batch_to_columns` helpers shared by the one-shot and streaming paths.
- `CMakeLists.txt` project version bumped from stale `1.0.0` to match the package version.

### Tested
- **Full E1 corpus** (1,012 files, 100% pass) exercised under the new sink and DuckDB paths:
    - `tests/test_scan_corpus.py::test_sink_yxdb_roundtrip` — parametrised across a 25-file sample of the corpus, round-trips each file through `sink_yxdb(lf, ...)` and compares bytes-equal to the eager read.
    - `tests/test_scan_corpus.py::test_duckdb_roundtrip_corpus` — registers a corpus file as a DuckDB view, counts rows, writes a projected query back out, and verifies the result.
    - `tests/test_scan_corpus.py::test_scan_head_then_sink` — end-to-end: lazy scan with `head` pushdown piped through `sink_yxdb` to a new file, chunk-size 64.
- `tests/test_openyxdb.py` gained a `TestSinkYxdb` class (5 tests) and `TestDuckDB` class (6 tests) covering filter/projection propagation, empty-result schema preservation, SQL and relation inputs, and round-trips.
- Full suite: **1,040 passed, 1 legitimate size-cap skip**.

### Documented honestly
- README now carries an explicit pushdown matrix for `scan_yxdb`:
    - Projection pushdown: **yes**, only requested columns are decoded from disk.
    - Row-limit pushdown: **yes**, decoding stops once `n_rows` produced.
    - Batched streaming: **yes**, default 65 536 rows, honours Polars' `batch_size` hint.
    - Predicate pushdown: **partial** — predicates are applied per batch after decode. YXDB has no per-block statistics, so genuine file-level predicate-skipping is impossible; combined with `head`, predicates still short-circuit once enough rows are collected.
- `sink_yxdb` still performs a single `collect()` under the hood because Polars 1.x does not expose a public plugin API for custom `sink_*` formats. The chunking happens on the write side; true per-batch push sinks require an upstream Polars plugin API.
- A native DuckDB `read_yxdb('file.yxdb')` SQL table function would require a C++ DuckDB extension (out of scope); the Python helpers give the same ergonomics via Arrow interop.

---

## [1.2.0] - 2026-04-18

### Added
- **Real pushdown for `scan_yxdb`** — the Polars IO plugin now performs true projection and row-limit pushdown at the C++ reader, instead of materialising the whole file and filtering in Python. `scan_yxdb(path).head(1000).collect()` on a 126 MB / 336k-row file drops from ~770 ms (full decode) to ~2 ms; projecting to a subset of columns further reduces decode work. Predicates are still applied per batch after decode (YXDB has no per-block statistics, so genuine predicate-pushdown at the file level is not possible).
- **`Reader.read_columns_subset(columns, offset, limit)`** — new low-level binding that decodes only the requested columns, starting at `offset`, up to `limit` records. This is the primitive the new `scan_yxdb` is built on, and is also useful directly for preview/inspection use cases.
- **Batch streaming in `scan_yxdb`** — the IO source now yields chunks (default 65 536 rows, honours Polars' `batch_size` hint) instead of one large frame, reducing peak memory on large files and letting `head`/limit short-circuit early.

### Fixed
- **`scan_yxdb` no longer re-decodes the whole file on every collect.** Previously the IO source called `to_pyarrow(path)` unconditionally inside the closure, so advertised pushdown was a lie and every `.collect()` paid the full decode cost. This was the root cause behind downstream freeze reports (e.g. studio preview stacking 5 s+ decodes on the event loop).

### Tested
- Added `tests/test_scan_corpus.py`: parametrised correctness tests across the full E1 corpus (973 files) plus dedicated projection, row-limit, offset, and perf-regression tests. Full suite: 1003 passed.

---

## [1.1.0] - 2026-04-18

### Added
- **Python 3.14 wheel builds** — added `cp314-*` to the cibuildwheel matrix in the publish workflow; bumped cibuildwheel from v2.22 to v3.4 which ships with CPython 3.14 support
- **`openyxdb.help()`** — new public function that prints quick-start examples, Polars integration usage, and the full list of supported field types
- **`python -m openyxdb`** — new module entry point; prints the same help text; `--version` flag prints the installed version
- **Polars monkey-patching docs** — added a dedicated "Polars integration" section to the README documenting the automatic `pl.read_yxdb()`, `pl.scan_yxdb()`, `df.yxdb.write()`, `lf.yxdb.sink()`, and `register_polars()` APIs that have been available since v1.0.1

### Changed
- Added `Programming Language :: Python :: 3.14` classifier to `pyproject.toml`

---

## [1.0.2] - 2026-04-04

### Fixed
- **C++ exception translation** — registered the library's `Error` exception class with nanobind so C++ errors now produce a Python `RuntimeError` with the original message instead of a cryptic `SystemError: nanobind exception could not be translated`
- **UTF-8 filename support on Linux** — replaced Latin-1 `ConvertToAString` path conversion with proper UTF-16 → UTF-8 conversion (`WStringToUtf8Path`), enabling files with non-ASCII names (accented characters, etc.) to be opened correctly; removed the artificial rejection of Unicode filenames
- **UTF-16 surrogate pair handling** — `u16_to_utf8` now correctly decodes surrogate pairs (characters above U+FFFF) into 4-byte UTF-8, and replaces lone surrogates with U+FFFD instead of producing invalid UTF-8
- **Non-UTF-8 narrow string fields** — added `narrow_to_python_str()` which tries UTF-8 first then falls back to Latin-1 for `String`/`V_String` fields, fixing `UnicodeDecodeError` on files with Windows-1252 encoded text (accented characters, currency symbols, etc.)
- **Zero-record file crash** — `read_records()` and `read_columns()` no longer call `GoRecord(0)` on files with 0 records, which previously threw because `0 >= 0` triggered the bounds check
- **PyPI license link** — changed relative `[LICENSE](LICENSE)` link in README to absolute GitHub URL so it resolves correctly on PyPI

### Changed
- Added `License` URL to `[project.urls]` in pyproject.toml for proper PyPI sidebar display

### Tested
- Verified all fixes by reading 1,012 real-world E1 YXDB files sourced from [YXDB-Sources](https://github.com/Sigilweaver/YXDB-Sources) — 100% pass rate (up from 90% on v1.0.1)

---

## [1.0.1] - 2026-04-04

### Added
- **Polars IO plugin** — `openyxdb.scan_yxdb()` returns a lazy `pl.LazyFrame` via `register_io_source`, with projection pushdown, predicate pushdown, and row limit support
- **Polars namespace plugins** — `df.yxdb.write(path)` and `lf.yxdb.sink(path)` via `@pl.api.register_dataframe_namespace` / `@pl.api.register_lazyframe_namespace`
- **Top-level Polars aliases** — `pl.read_yxdb(path)` and `pl.scan_yxdb(path)` monkey-patched onto the polars module on `import openyxdb`
- 9 new pytest tests covering scan, pushdown, namespace plugins, and monkey-patches (30 total)

### Fixed
- **Cross-platform wheel builds** — static library linking for Python wheels (fixes macOS delocate), `BUILDING_OPEN_ALTERYX` define to blank out `__declspec`, MSVC flag syntax, `_WIN32` guard corrections, `Threads::Threads` + `${CMAKE_DL_LIBS}` for portable linking
- **Windows test crash** — removed `sys.path.insert` that caused pytest to import source tree instead of installed wheel
- **Linux manylinux test skip** — `CIBW_TEST_SKIP` for manylinux containers where polars/pyarrow need glibc 2.28+
- **macOS build** — disabled PCH for Python builds (incompatible with multi-arch), dropped universal2 in favor of arm64-only
- **pixi dev workflow** — replaced fragile copy-so hack with `pip install -e .` editable install via scikit-build-core

### Changed
- Publish workflow no longer runs wheel builds on push to `main` — only on PRs (pre-merge check) and tags (publish to PyPI)
- Added `cmake>=3.20` and `ninja` to `[build-system] requires`

---

## [1.0.0] - 2026-04-03

First production release, forked from the dormant Alteryx repository and rebuilt as a cross-platform C++/Python library.

- **Python bindings** via nanobind with high-level and low-level APIs
  - `openyxdb.read_yxdb()` / `openyxdb.write_yxdb()` for dict-of-lists I/O
  - PyArrow integration (`to_pyarrow`, `from_pyarrow`)
  - Pandas integration (`to_pandas`, `from_pandas`)
  - Polars integration (`to_polars`, `from_polars`)
  - Low-level `Reader` / `Writer` classes with context manager support
  - Columnar read (`read_columns`) and write (`write_columns`) paths
  - Automatic schema inference for Python types (int, float, str, bool)
  - Null value support across all field types
- **Cross-platform C++ core** — block index write correctness, macOS (`__APPLE__`) platform guards, non-MSVC refcount fix
- **Modernized CMakeLists.txt** — proper shared library target, Catch2 integration, nanobind Python module, precompiled headers
- **Catch2 unit test suite** — 13 C++ tests (roundtrip, all field types, multi-block, random access, nulls, blob, schema validation)
- **Python test suite** — 21 pytest tests (low-level API, high-level API, PyArrow/Pandas/Polars roundtrips, schema inference)
- **GitHub Actions CI/CD** — automated testing on push/PR, uv-accelerated wheel builds via cibuildwheel
- **pixi project configuration** for reproducible dev environments
- **Cross-platform wheel builds** for Linux x64, macOS Intel/ARM, and Windows x64
- **Python 3.10+** required (3.9 EOL upstream, polars incompatible)
- **GPL-3.0-only license metadata** aligned across LICENSE, pyproject.toml, and README

---

## [0.0.0] — Alteryx (2022-03-09 to 2022-04-18)

> Original open-source release by Alteryx, Inc. under GPL-3.0.

### Added
- Initial open-source release of the Alteryx YXDB C++ reader/writer library
- Core `Open_AlteryxYXDB` class for reading and writing `.yxdb` files
- `RecordInfo` / `FieldBase` / `Record` classes for schema and record management
- Full field type support: Bool, Byte, Int16, Int32, Int64, Float, Double, FixedDecimal, String, WString, V_String, V_WString, Date, Time, DateTime, Blob, SpatialObj
- LZF compression for record blocks
- SpookyHash-based record hashing
- Embedded SQLite3 for internal metadata
- MiniXmlParser for RecordInfo XML serialization
- Unicode support with case-folding tables
- GPL-3.0 license

### Fixed
- GCC compilation warnings (PR #1)

### Changed
- README updated with build instructions and corrected VS version (PR #3)
