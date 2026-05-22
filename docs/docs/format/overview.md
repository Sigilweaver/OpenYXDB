---
sidebar_position: 10
---

# YXDB Format

YXDB is the native binary file format used by Alteryx Designer for persisting
workflow data between tools. This page describes the on-disk layout as
implemented by this library.

:::note Format scope

This documentation covers **E1 (non-AMP) YXDB files** only. AMP (multi-threaded)
YXDB files produced by Alteryx Server are not supported.

:::

## High-level layout

```
+---------------------------+
|  File header (fixed)      |  magic number, version, record count, meta-data offset
+---------------------------+
|  UTF-16 XML metadata      |  RecordInfo XML -- field names, types, sizes, scales
+---------------------------+
|  LZF-compressed blocks    |  one or more blocks of LZF-compressed record data
|  (variable count)         |
+---------------------------+
|  Block index              |  byte offset of each block for random access
+---------------------------+
```

## File header

The file header occupies the first bytes of the file and contains:

| Field | Type | Description |
| --- | --- | --- |
| Signature | bytes | Magic number identifying the file as YXDB |
| Version | uint32 | Format version (E1 = 1) |
| Metadata size | uint32 | Byte length of the UTF-16 XML metadata block |
| Record count | uint64 | Total number of records in the file |
| Block index offset | uint64 | Byte offset from the start of the file to the block index |

## Metadata block

Immediately following the header is a UTF-16LE encoded XML string. The root
element is `RecordInfo` and contains one `Field` element per column:

```xml
<RecordInfo>
  <Field name="id" size="4" type="Int32" scale="0"/>
  <Field name="label" size="262144" type="V_WString" scale="0"/>
</RecordInfo>
```

`size` is the maximum byte length for the field. For numeric types this equals
the storage width (1, 2, 4, or 8 bytes). For variable-length string types
(`V_String`, `V_WString`) it is a declared maximum that does not affect the
on-disk storage of individual values.

## Record blocks

Records are stored in LZF-compressed blocks. Each block holds a fixed number
of rows (up to the block size). The raw (uncompressed) bytes for a block are
a flat concatenation of the fixed- or variable-length byte representations of
each field for each row, in column-major order within each record.

For variable-length fields (`V_String`, `V_WString`, `Blob`, `SpatialObj`),
each value is preceded by a 4-byte length prefix. A length of 0 encodes null.
For all other types, a trailing null-flag byte follows the fixed-width value.

## Block index

The block index is written at the end of the file. It stores the byte offset
of each block, allowing O(1) random access to any block by record number
(each block holds a known number of records).

:::note Bug in original Alteryx implementation

The original Alteryx code never wrote the block index to disk. This caused
silent data truncation for any file with more than 65,536 records. OpenYXDB
fixes this -- the block index is always written correctly.

:::

## LZF compression

OpenYXDB uses the same embedded LZF implementation as the original Alteryx
code. LZF is a fast, byte-oriented compression algorithm. Each block is
independently compressed and decompressed. Block boundaries are stored in the
block index.

## Field encoding details

| Type | Encoding |
| --- | --- |
| Bool | 1 byte: `0x01` = true, `0x00` = false; followed by 1 null-flag byte |
| Byte | 1 unsigned byte + 1 null-flag byte |
| Int16 | 2-byte little-endian signed integer + 1 null-flag byte |
| Int32 | 4-byte little-endian signed integer + 1 null-flag byte |
| Int64 | 8-byte little-endian signed integer + 1 null-flag byte |
| Float | 4-byte IEEE 754 little-endian + 1 null-flag byte |
| Double | 8-byte IEEE 754 little-endian + 1 null-flag byte |
| FixedDecimal | ASCII decimal string, padded to `size` bytes + 1 null-flag byte |
| String | Fixed-width byte string, padded to `size` bytes + 1 null-flag byte |
| WString | Fixed-width UTF-16LE string, padded to `size * 2` bytes + 1 null-flag byte |
| V_String | 4-byte length prefix + variable-length bytes (0 = null) |
| V_WString | 4-byte length prefix + variable-length UTF-16LE bytes (0 = null) |
| Date | 10-byte ASCII `YYYY-MM-DD` + 1 null-flag byte |
| Time | 8-byte ASCII `HH:MM:SS` + 1 null-flag byte |
| DateTime | 19-byte ASCII `YYYY-MM-DD HH:MM:SS` + 1 null-flag byte |
| Blob | 4-byte length prefix + variable-length bytes (0 = null) |
| SpatialObj | 4-byte length prefix + SHP-encoded bytes (0 = null) |
