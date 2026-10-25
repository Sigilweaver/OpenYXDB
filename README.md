# OpenYXDB

[![CI](https://github.com/Sigilweaver/OpenYXDB/actions/workflows/ci.yml/badge.svg)](https://github.com/Sigilweaver/OpenYXDB/actions/workflows/ci.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.20470611.svg)](https://doi.org/10.5281/zenodo.20470611)
[![PyPI](https://img.shields.io/pypi/v/openyxdb.svg)](https://pypi.org/project/openyxdb/)
[![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-blue.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/docs-sigilweaver.app-blue.svg)](https://sigilweaver.app/openyxdb/docs/)

Read and write Alteryx YXDB files from Python and C++.

Built on [Alteryx's official open-source YXDB implementation](https://github.com/alteryx/OpenYXDB), released under GPLv3. This fork modernizes the build system, fixes bugs, adds cross-platform support, and wraps the C++ core in a Python package with first-class PyArrow, Pandas, and Polars integration.

## What changed from the Alteryx original

- **Cross-platform support** -- builds and runs on Linux, macOS, and Windows (the original targeted Windows only)
- **Python bindings** via nanobind, published to PyPI as `openyxdb`
- **PyArrow, Pandas, Polars, and DuckDB** read/write integration with automatic type mapping
- **Bug fixes** -- block index was never written to disk (broke files with more than 65,536 records), reference-counting crash on GCC
- **Modern CMake** (3.20+, target-based), pixi for dependency management, Catch2 test suite, GitHub Actions CI

## What is YXDB?

YXDB is the native binary format used by Alteryx Designer. Two on-disk layouts
are in active use: the original format used by the classic engine and a newer
columnar-friendly variant emitted by the AMP engine.

This library reads both layouts automatically -- the file's magic bytes are
sniffed at open time and the appropriate decoder is selected. New files written
through `openyxdb.Writer` use the original format.

The newer E2 layout is only partially documented. Its reader validates record
framing and rejects ambiguous integer encodings, invalid scalar prefixes, and
unresolved blob references rather than returning guessed values. A bounded set
of producer variants is therefore known to fail explicitly until their byte
encoding can be verified.

## Choosing between OpenYXDB and SigilYX

OpenYXDB wraps Alteryx's own C++ implementation. Reach for it when you want that
provenance, a C++ library to link against, or GPLv3.

[SigilYX](https://github.com/Sigilweaver/SigilYX) is an independent Rust
implementation of the same format under Apache-2.0. It decodes straight into
Arrow buffers rather than through Python objects, so it is substantially faster
and uses less memory on large files, and it offers constant-memory batch reads.
Prefer it for large inputs, for throughput-sensitive Python work, or when GPLv3
does not suit your project.

For reading large files through OpenYXDB, `scan_yxdb` streams in batches and
keeps peak memory bounded; `to_pyarrow` and friends materialise the whole file.

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

## Polars integration

Importing `openyxdb` automatically monkey-patches `polars` with YXDB support (no-op if polars is not installed). No extra setup is required.

### Top-level aliases

```python
import polars as pl
import openyxdb  # registers everything on import

# Eager read - returns a DataFrame
df = pl.read_yxdb("data.yxdb")

# Lazy scan - returns a LazyFrame with projection & predicate pushdown
lf = pl.scan_yxdb("data.yxdb")
df = lf.select("col_a", "col_b").filter(pl.col("col_a") > 10).collect()
```

### Namespace plugins

```python
# Write a DataFrame directly
df.yxdb.write("output.yxdb")

# Stream a LazyFrame through the streaming engine and write in chunks
lf.yxdb.sink("output.yxdb", chunk_size=65_536)
```

`lf.yxdb.sink()` (and the top-level `openyxdb.sink_yxdb(lf, path)`) execute the
lazy plan on Polars' streaming engine and write the result to the YXDB file in
chunks, so the writer never buffers more than `chunk_size` rows in Python at a
time. Polars 1.x does not expose a public plugin API for custom `sink_*`
formats, so the query result still passes through a single `collect()` call
before being chunk-written; true per-batch push sinks require upstream Polars
support.

### Pushdown support

`scan_yxdb` implements pushdown against the underlying reader:

| Optimization         | Supported | Notes                                                          |
| -------------------- | --------- | -------------------------------------------------------------- |
| Projection pushdown  | Yes       | Only requested columns are decoded from disk.                  |
| Row-limit pushdown   | Yes       | `head(n)` / `fetch(n)` stops decoding once `n` rows produced.  |
| Batched streaming    | Yes       | Default 65 536 rows; honours Polars' `batch_size` hint.        |
| Predicate pushdown   | Partial   | Predicates evaluate per batch after decode. YXDB has no per-block statistics, so genuine file-level skipping isn't possible; combined with `head` predicates still short-circuit once enough rows are collected. |

### Manual registration

If you import `openyxdb` after polars is already loaded, everything is registered automatically. To re-register or verify:

```python
openyxdb.register_polars()
```

## DuckDB integration

```python
import duckdb, openyxdb

con = duckdb.connect()

# Register a YXDB file as a SQL view
openyxdb.register_duckdb(con, "yx", "data.yxdb")
con.execute("SELECT COUNT(*) FROM yx WHERE score > 90").fetchone()

# Or get a relation directly
rel = openyxdb.to_duckdb("data.yxdb", con)

# Write the result of a DuckDB query to YXDB
openyxdb.from_duckdb(
    "SELECT id, score FROM yx WHERE score > 90",
    "top_scores.yxdb",
    con=con,
)
```

DuckDB support is provided through Arrow interop: YXDB files are loaded into an
Arrow table and registered on the connection. A native DuckDB table function
(`SELECT * FROM read_yxdb('file.yxdb')`) would require a C++ extension, which is
out of scope for this package.

## Building from source

```bash
pixi install
pixi run build
pixi run test
```

## String encoding

`String` and `V_String` fields hold bytes with no encoding label. OpenYXDB
decodes every such value of a file with one encoding, `narrow_encoding`, which
defaults to strict UTF-8. Bytes that are not valid UTF-8 raise
`ConversionError` naming the field, the record and the byte offset. Files from
Windows tools usually hold single-byte text; read them with
`narrow_encoding="cp1252"`, or `"latin-1"` (which maps every byte, and is the
only choice when a file contains the bytes 0x81, 0x8D, 0x8F, 0x90 or 0x9D that
cp1252 leaves undefined).

Known limits:

- A file can mix UTF-8 and single-byte values (a few corpus files do). No
  single codec reads both correctly, and OpenYXDB does not guess per value.
- Text that is valid UTF-8 but wrong is not detectable. Four corpus files
  (`*_atheles.yxdb`, produced from a CSV import) hold the bytes `CC A9` where
  "e with acute" is expected, which decode as "C" + U+0329 + "line" and not
  "Celine"; the `Driver Profiles` files show the same pattern. Neither the
  file header nor the field metadata names an encoding, and the pattern is not
  a standard encoding, so there is nothing for a reader to detect.
- The writer encodes with the same codec and counts field sizes in encoded
  bytes. Narrow fields written as UTF-8 are read as garbage by tools that
  assume a legacy code page; pass `narrow_encoding="cp1252"` (or
  `"latin-1"`) when writing for such tools.

## Limitations

- Spatial indexes are skipped on read and not created on write.
- Spatial objects are accessible only as raw blobs (SHP-encoded binary).
- Little-endian architectures only (x86, ARM).

## Testing

The reader has been validated against **1,012 real-world E1 YXDB files** sourced from the community corpus at [Sigilweaver/YXDB-Sources](https://github.com/Sigilweaver/YXDB-Sources), covering a wide range of field types, encodings, record counts (0 to 200k+), and filenames including non-ASCII characters - every file reads; 87 of the 1,001 E1 files hold single-byte text and need `narrow_encoding` (see [String encoding](#string-encoding)).

The 1.3.0 streaming sink (`sink_yxdb`) and DuckDB round-trip paths are exercised against a sampled 25-file subset of the corpus in addition to the per-feature unit tests. Full Python suite: **1,040 passed**.

## License

GPLv3 -- see [LICENSE](https://github.com/Sigilweaver/OpenYXDB/blob/main/LICENSE).
