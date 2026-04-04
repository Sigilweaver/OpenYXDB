# Changelog

All notable changes to this project will be documented in this file.

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
