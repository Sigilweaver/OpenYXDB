"""E1 header and schema validation: unexpected input must raise."""

from __future__ import annotations

import struct

import pytest
from e1_files import (
    OFF_COMPRESSION,
    OFF_FILE_ID,
    OFF_INDEX_POS,
    OFF_NUM_RECORDS,
    patch,
    replace_utf16,
    write_int_file,
)

from openyxdb._openyxdb import Reader


def read_all(path):
    with Reader(str(path)) as r:
        return r.read_records()


@pytest.fixture
def valid(tmp_path):
    p = tmp_path / "valid.yxdb"
    data = write_int_file(p)
    return p, data


def test_valid_file_reads(valid):
    p, _ = valid
    assert [dict(r)["x"] for r in read_all(p)] == [0, 1, 2, 3, 4]


@pytest.mark.parametrize(
    "bad_name",
    ["Bxxx2", "Integ", "inT3a", "Int33"],
)
def test_unknown_field_type_name_raises(valid, bad_name):
    p, data = valid
    p.write_bytes(replace_utf16(data, "Int32", bad_name))
    with pytest.raises(RuntimeError, match="Unknown field type"):
        read_all(p)


def test_field_type_name_is_case_insensitive(valid):
    p, data = valid
    p.write_bytes(replace_utf16(data, "Int32", "INT32"))
    assert len(read_all(p)) == 5


@pytest.mark.parametrize("version", [2, 3, 7, -1])
def test_unsupported_compression_version_raises(valid, version):
    p, data = valid
    p.write_bytes(patch(data, OFF_COMPRESSION, struct.pack("<i", version)))
    with pytest.raises(RuntimeError, match="compression version"):
        read_all(p)


def test_negative_record_count_raises(valid):
    p, data = valid
    p.write_bytes(patch(data, OFF_NUM_RECORDS, struct.pack("<q", -5)))
    with pytest.raises(RuntimeError, match="record count"):
        read_all(p)


def test_unfinalized_file_raises(valid):
    """A header never rewritten at close has a zero index position."""
    p, data = valid
    zeroed = patch(data, OFF_NUM_RECORDS, struct.pack("<q", 0))
    zeroed = patch(zeroed, OFF_INDEX_POS, struct.pack("<q", 0))
    p.write_bytes(zeroed)
    with pytest.raises(RuntimeError, match="not finalized"):
        read_all(p)


def test_empty_finalized_file_reads(tmp_path):
    p = tmp_path / "empty.yxdb"
    write_int_file(p, rows=0)
    assert read_all(p) == []


def test_bad_magic_raises(valid):
    p, data = valid
    p.write_bytes(patch(data, 0, b"Not a database"))
    with pytest.raises(RuntimeError, match="Not a YXDB file"):
        read_all(p)


@pytest.mark.parametrize("version", [2, 3, 6])
def test_unsupported_file_version_raises(valid, version):
    p, data = valid
    # File IDs 2 and 3 pass the legacy range check but have no known writer.
    p.write_bytes(patch(data, OFF_FILE_ID, struct.pack("<i", 0x00440200 | version)))
    with pytest.raises(RuntimeError):
        read_all(p)


def test_schema_without_record_info_raises(valid):
    p, data = valid
    p.write_bytes(replace_utf16(data, "RecordInfo", "RecordInfX"))
    with pytest.raises(RuntimeError, match="no RecordInfo"):
        read_all(p)


def test_schema_without_fields_raises(valid):
    p, data = valid
    p.write_bytes(replace_utf16(data, "<Field ", "<Fiel_ "))
    with pytest.raises(RuntimeError, match="no RecordInfo"):
        read_all(p)


def test_truncated_file_reports_sizes_not_errno(valid):
    p, data = valid
    p.write_bytes(data[:-60])
    with pytest.raises(RuntimeError, match=r"expected \d+ bytes at offset \d+ but got \d+"):
        read_all(p)
