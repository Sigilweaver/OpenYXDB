# OpenYXDB

Read and write Alteryx YXDB files from Python and C++.

Built on [Alteryx's official open-source YXDB implementation](https://github.com/alteryx/OpenYXDB), released under GPLv3. This fork modernizes the build system, fixes bugs, adds cross-platform support, and wraps the C++ core in a Python package with first-class PyArrow, Pandas, and Polars integration.

## What changed from the Alteryx original

- **Cross-platform support** -- builds and runs on Linux, macOS, and Windows (the original targeted Windows only)
- **Python bindings** via nanobind, published to PyPI as `openyxdb`
- **PyArrow, Pandas, and Polars** read/write integration with automatic type mapping
- **Bug fixes** -- block index was never written to disk (broke files with more than 65,536 records), reference-counting crash on GCC
- **Modern CMake** (3.20+, target-based), pixi for dependency management, Catch2 test suite, GitHub Actions CI

## What is YXDB?

YXDB is the native binary format used by Alteryx Designer. It is row-oriented: each file contains UTF-16 XML metadata describing the schema, followed by LZF-compressed blocks of records, and a block index at the end for random access.

This library supports **E1 (non-AMP) YXDB files only**.

## Install

```bash
pip install openyxdb
```

## Usage

```python
import openyxdb

# Read to PyArrow, Pandas, or Polars
table = openyxdb.to_pyarrow("data.yxdb")
df = openyxdb.to_pandas("data.yxdb")
df = openyxdb.to_polars("data.yxdb")

# Write from any of them
openyxdb.from_polars(df, "output.yxdb")
openyxdb.from_pandas(df, "output.yxdb")
openyxdb.from_pyarrow(table, "output.yxdb")
```

All 17 YXDB field types are supported: Bool, Byte, Int16, Int32, Int64, FixedDecimal, Float, Double, String, WString, V_String, V_WString, Date, Time, DateTime, Blob, and SpatialObj.

## Building from source

```bash
pixi install
pixi run build
pixi run test
```

## Limitations

- Spatial indexes are skipped on read and not created on write.
- Spatial objects are accessible only as raw blobs (SHP-encoded binary).
- Little-endian architectures only (x86, ARM).

## Testing

The reader has been validated against **1,012 real-world E1 YXDB files** sourced from the community corpus at [Sigilweaver/YXDB-Sources](https://github.com/Sigilweaver/YXDB-Sources), covering a wide range of field types, encodings, record counts (0 to 200k+), and filenames including non-ASCII characters — **100% pass rate**.

## License

GPLv3 -- see [LICENSE](https://github.com/Sigilweaver/OpenYXDB/blob/main/LICENSE).

