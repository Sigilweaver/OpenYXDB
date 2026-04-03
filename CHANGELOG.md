# Changelog

All notable changes to this project will be documented in this file.

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
