"""Focused tests for strict E2 record framing and ambiguity handling."""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

from openyxdb._openyxdb import Reader


def _snappy_literal(data: bytes) -> bytes:
    """Encode one raw Snappy literal, sufficient for the tiny test blocks."""
    remaining = len(data)
    varint = bytearray()
    while remaining >= 0x80:
        varint.append((remaining & 0x7F) | 0x80)
        remaining >>= 7
    varint.append(remaining)

    size_minus_one = len(data) - 1
    if len(data) <= 60:
        tag = bytes([size_minus_one << 2])
    else:
        width = max(1, (size_minus_one.bit_length() + 7) // 8)
        tag = bytes([(59 + width) << 2]) + size_minus_one.to_bytes(width, "little")
    return bytes(varint) + tag + data


def _write_e2(path: Path, fields: list[tuple[str, str]], record: bytes,
              *, record_count: int = 1) -> Path:
    xml = (
        "<MetaInfo><RecordInfo>"
        + "".join(
            f'<Field name="{name}" type="{type_name}" size="64"/>'
            for name, type_name in fields
        )
        + "</RecordInfo></MetaInfo>"
    ).encode()
    header = bytearray(100)
    header[:24] = b"Alteryx e2 Database file"
    struct.pack_into("<I", header, 64, 0x00440208)
    struct.pack_into("<I", header, 96, len(xml))
    block = b"\0" * 4 + struct.pack("<II", record_count, len(record)) + record
    compressed = b"\x0a" + _snappy_literal(block)
    path.write_bytes(
        header + xml + b"\x02" + struct.pack("<I", len(compressed))
        + compressed + b"\x00"
    )
    return path


def test_mixed_int32_widths_have_one_exact_parse(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "mixed.yxdb",
        [("annual", "Int32"), ("weekly", "Int32")],
        bytes.fromhex("09 C0 CD 17 00 08 30 75"),
    )
    with Reader(str(path)) as reader:
        assert reader.read_columns() == {
            "annual": [1_560_000],
            "weekly": [30_000],
        }


def test_multiple_exact_int32_parses_raise(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "ambiguous.yxdb",
        [("left", "Int32"), ("right", "Int32"), ("tail", "String")],
        bytes.fromhex("06 09 06 00 81 00"),
    )
    with Reader(str(path)) as reader:
        with pytest.raises(RuntimeError, match="ambiguous Int32 encoding"):
            reader.read_columns()


def test_failed_uniform_parse_that_reaches_09_still_checks_mixed_paths(
    tmp_path: Path,
) -> None:
    path = _write_e2(
        tmp_path / "failed-uniform-09.yxdb",
        [("first", "Int32"), ("second", "Int32"), ("third", "Int32")],
        bytes.fromhex("07 01 08 09 02 08 03 05"),
    )
    with Reader(str(path)) as reader:
        with pytest.raises(RuntimeError, match="ambiguous Int32 encoding"):
            reader.read_columns()


def test_distinct_uniform_parses_without_09_raise(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "distinct-uniform.yxdb",
        [("left", "Int32"), ("right", "Int32"), ("tail", "String")],
        bytes.fromhex("06 07 05 81 00"),
    )
    with Reader(str(path)) as reader:
        with pytest.raises(RuntimeError, match="ambiguous Int32 encoding"):
            reader.read_columns()


def test_invalid_integer_prefix_does_not_become_null(tmp_path: Path) -> None:
    path = _write_e2(tmp_path / "invalid-int.yxdb", [("value", "Int32")], b"\0")
    with Reader(str(path)) as reader:
        with pytest.raises(RuntimeError, match="no exact match"):
            reader.read_columns()


def test_nine_byte_inline_blob_is_not_treated_as_reference(tmp_path: Path) -> None:
    payload = b"123456789"
    path = _write_e2(tmp_path / "inline-blob.yxdb", [("value", "Blob")], b"\x89" + payload)
    with Reader(str(path)) as reader:
        assert reader.read_columns() == {"value": [payload]}


def test_unresolved_blob_reference_raises(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "unresolved-blob.yxdb",
        [("value", "Blob")],
        b"\x12" + b"\0" * 8,
    )
    with Reader(str(path)) as reader:
        with pytest.raises(RuntimeError, match="unresolved file-offset blob reference"):
            reader.read_columns()


def test_record_count_is_bounded_by_block_framing(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "record-count.yxdb",
        [("value", "Int32")],
        b"",
        record_count=0xFFFFFFFF,
    )
    with Reader(str(path)) as reader:
        with pytest.raises(RuntimeError, match="record count exceeds"):
            _ = reader.num_records
