"""Focused tests for strict E2 record framing and ambiguity handling."""

from __future__ import annotations

import datetime
import struct
from pathlib import Path

import pytest

import openyxdb

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
              *, record_count: int = 1, trailer: bytes = b"\x00YXE2",
              xml_override: str | None = None, extra_record_bytes: bytes = b"") -> Path:
    xml = xml_override.encode() if xml_override is not None else (
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
    block = (b"\0" * 4 + struct.pack("<II", record_count, len(record)) + record
             + extra_record_bytes)
    compressed = b"\x0a" + _snappy_literal(block)
    path.write_bytes(
        header + xml + b"\x02" + struct.pack("<I", len(compressed))
        + compressed + trailer
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
        with pytest.raises(openyxdb.FormatError, match="ambiguous Int32 encoding"):
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
        with pytest.raises(openyxdb.FormatError, match="ambiguous Int32 encoding"):
            reader.read_columns()


def test_distinct_uniform_parses_without_09_raise(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "distinct-uniform.yxdb",
        [("left", "Int32"), ("right", "Int32"), ("tail", "String")],
        bytes.fromhex("06 07 05 81 00"),
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="ambiguous Int32 encoding"):
            reader.read_columns()


def test_invalid_integer_prefix_does_not_become_null(tmp_path: Path) -> None:
    path = _write_e2(tmp_path / "invalid-int.yxdb", [("value", "Int32")], b"\0")
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="no exact match"):
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
        with pytest.raises(openyxdb.FormatError, match="unresolved file-offset blob reference"):
            reader.read_columns()


def test_record_count_is_bounded_by_block_framing(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "record-count.yxdb",
        [("value", "Int32")],
        b"",
        record_count=0xFFFFFFFF,
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="record count exceeds"):
            _ = reader.num_records


def test_byte_values_above_127_are_unsigned(tmp_path: Path) -> None:
    path = _write_e2(tmp_path / "byte.yxdb", [("b", "Byte")], b"\x07\xc8")
    with Reader(str(path)) as reader:
        assert reader.read_columns() == {"b": [200]}


def test_time_with_fraction_raises(tmp_path: Path) -> None:
    centi = 360055
    path = _write_e2(
        tmp_path / "frac.yxdb", [("t", "Time")], b"\x0f" + centi.to_bytes(3, "little")
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="sub-second"):
            reader.read_columns()


def test_time_of_30_hours_raises(tmp_path: Path) -> None:
    centi = 30 * 3600 * 100
    path = _write_e2(
        tmp_path / "hours.yxdb", [("t", "Time")], b"\x0f" + centi.to_bytes(3, "little")
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="24 hours"):
            reader.read_columns()


def test_whole_second_time_reads(tmp_path: Path) -> None:
    centi = 3661 * 100
    path = _write_e2(
        tmp_path / "ok.yxdb", [("t", "Time")], b"\x0f" + centi.to_bytes(3, "little")
    )
    with Reader(str(path)) as reader:
        assert reader.read_columns() == {"t": [datetime.time(1, 1, 1)]}


def test_datetime_with_fraction_raises(tmp_path: Path) -> None:
    raw = (45000 << 24) | 55
    path = _write_e2(
        tmp_path / "dt.yxdb", [("t", "DateTime")], b"\x0d" + raw.to_bytes(5, "little")
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="sub-second"):
            reader.read_columns()


def test_missing_end_marker_raises(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "cut.yxdb", [("value", "Blob")], b"\x89" + b"123456789", trailer=b""
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="end-of-data marker"):
            reader.read_columns()
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="end-of-data marker"):
            reader.num_records


def test_missing_trailer_raises(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "notrailer.yxdb", [("value", "Blob")], b"\x89" + b"123456789",
        trailer=b"\x00",
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="trailer"):
            reader.read_columns()


def test_wrong_trailer_raises(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "badtrailer.yxdb", [("value", "Blob")], b"\x89" + b"123456789",
        trailer=b"\x00ABCD",
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="YXE2"):
            reader.read_columns()


def test_bytes_after_last_record_raise(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "extra.yxdb", [("value", "Blob")], b"\x89" + b"123456789",
        extra_record_bytes=b"\x00\x00",
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="unread bytes"):
            reader.read_columns()


def test_field_without_a_name_raises(tmp_path: Path) -> None:
    xml = '<MetaInfo><RecordInfo><Field type="Blob" size="64"/></RecordInfo></MetaInfo>'
    path = _write_e2(tmp_path / "noname.yxdb", [], b"\x89" + b"123456789", xml_override=xml)
    with pytest.raises(openyxdb.FormatError, match="no name or no type"):
        Reader(str(path))


def test_attribute_names_match_whole_words(tmp_path: Path) -> None:
    # 'rename' must not be read as 'name'.
    xml = (
        '<MetaInfo><RecordInfo><Field rename="x" name="real" type="Blob" size="64"/>'
        "</RecordInfo></MetaInfo>"
    )
    path = _write_e2(tmp_path / "attr.yxdb", [], b"\x89" + b"123456789", xml_override=xml)
    with Reader(str(path)) as reader:
        assert [f.name for f in reader.schema] == ["real"]


def test_empty_block_with_payload_raises(tmp_path: Path) -> None:
    path = _write_e2(
        tmp_path / "empty-payload.yxdb", [("value", "Blob")], b"\x89" + b"123456789",
        record_count=0,
    )
    with Reader(str(path)) as reader:
        with pytest.raises(openyxdb.FormatError, match="declares no records"):
            reader.read_columns()
