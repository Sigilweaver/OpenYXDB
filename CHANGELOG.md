# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2026-04-03

### Added
- **Python bindings** via nanobind with high-level and low-level APIs
  - `openyxdb.read_yxdb()` / `openyxdb.write_yxdb()` for dict-of-lists I/O
  - PyArrow integration (`to_pyarrow`, `from_pyarrow`)
  - Pandas integration (`to_pandas`, `from_pandas`)
  - Polars integration (`to_polars`, `from_polars`)
  - Low-level `Reader` / `Writer` classes with context manager support
  - Columnar read (`read_columns`) and write (`write_columns`) paths
  - Automatic schema inference for Python types (int, float, str, bool)
  - Null value support across all field types
- **Catch2 unit test suite** — 13 C++ tests covering:
  - Full roundtrip (write + read)
  - All numeric types (Bool, Byte, Int16, Int32, Int64, Float, Double)
  - String types (String, WString, V_String, V_WString)
  - Date/Time/DateTime fields
  - Null value handling
  - Blob field roundtrip
  - FixedDecimal field type
  - Multi-block files (>65,536 records)
  - Random access via `GoRecord`
  - Empty file with schema only
  - RecordInfo XML metadata validation
  - Field type predicates (`IsString`, `IsNumeric`, `IsDate`, `IsBinary`)
- **Python test suite** — 21 pytest tests covering:
  - Low-level Reader/Writer roundtrips
  - Context manager protocol
  - Null handling
  - All numeric and string types
  - Columnar read/write
  - Schema introspection
  - High-level dict I/O with schema inference
  - PyArrow roundtrip and type mapping
  - Pandas roundtrip
  - Polars roundtrip (including large DataFrames)
- **GitHub Actions CI/CD** — automated testing on push/PR to `main`
- **PyPI publishing workflow** — build and publish wheels on release
- **pixi project configuration** for reproducible dev environments
- **Cross-platform build support** (Linux, macOS Intel/ARM, Windows)

### Fixed
- **Block index write bug** — `Close()` now writes the block index to the file, enabling correct multi-block reads
- **SmartPointerRefObj refcount bug** on non-MSVC compilers (undefined behavior in constructor)
- **macOS support** — added `__APPLE__` to platform-specific preprocessor guards
- **pixi.lock committed** for reproducible CI caching

### Changed
- **Modernized CMakeLists.txt** — proper shared library target, Catch2 integration, nanobind Python module, precompiled headers
- **README rewritten** for Sigilweaver OpenYXDB with provenance, build instructions, and Python usage examples
- **CI simplified** to Linux-only for testing, all platforms for builds

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
