"""Corrupt E1 files must raise, never crash or return stale or shifted rows.

Every read runs in a child process (see ``run_read``) so a reader that corrupts
memory fails the test instead of killing the test run.
"""

from __future__ import annotations

import struct

import pytest
from e1_files import (
    OFF_INDEX_POS,
    OFF_META_LEN,
    OFF_NUM_RECORDS,
    data_start,
    field,
    i64,
    patch,
    run_read,
    u32,
    with_raw_chunk,
)

from openyxdb._openyxdb import Writer


def write_wide_file(path, rows: int) -> bytes:
    """Compressible multi-block-capable file: Int32 + V_WString."""
    with Writer(str(path), [field("i", "Int32", 4), field("s", "V_WString", 100)]) as w:
        for i in range(rows):
            w.write_record({"i": i, "s": "row-%d" % (i % 10)})
    return path.read_bytes()


@pytest.fixture(scope="module")
def small(tmp_path_factory):
    p = tmp_path_factory.mktemp("small") / "small.yxdb"
    return write_wide_file(p, 5000)


@pytest.fixture(scope="module")
def big(tmp_path_factory):
    """Three record blocks (RecordsPerBlock is 65536)."""
    p = tmp_path_factory.mktemp("big") / "big.yxdb"
    return write_wide_file(p, 140000)


# Appended after the index so an oversized chunk length has bytes to read; a
# file that ends first would fail with a short read before any overflow.
PAD = b"\0" * (8 << 20)


def check(tmp_path, data: bytes, expect_error: bool = True):
    p = tmp_path / "bad.yxdb"
    p.write_bytes(data)
    status, detail = run_read(p)
    if expect_error:
        assert status == "error", (status, detail)
    return status, detail


def test_valid_files_read(tmp_path, small, big):
    assert check(tmp_path, small, expect_error=False) == ("ok", 5000)
    assert check(tmp_path, big, expect_error=False) == ("ok", 140000)


# -- LZF chunk framing ------------------------------------------------------ #


@pytest.mark.parametrize("length", [0, 0x40001, 0x100000, 0x7FFFFFFF])
def test_compressed_chunk_length_out_of_range(tmp_path, small, length):
    start = data_start(small)
    assert u32(small, start) & 0x80000000 == 0, "fixture should start with a compressed chunk"
    check(tmp_path, patch(small, start, struct.pack("<I", length)) + PAD)


@pytest.mark.parametrize("length", [0, 0x40001, 0x7FFFFFFF])
def test_uncompressed_chunk_length_out_of_range(tmp_path, small, length):
    start = data_start(small)
    check(tmp_path, patch(small, start, struct.pack("<I", 0x80000000 | length)) + PAD)


def test_undecodable_compressed_chunk_raises(tmp_path, small):
    start = data_start(small)
    size = u32(small, start)
    # 0xFF 0xFF: back reference to before the start of the output.
    garbage = b"\xff" * size
    check(tmp_path, patch(small, start + 4, garbage))


def test_truncated_chunk_raises(tmp_path, small):
    start = data_start(small)
    size = u32(small, start)
    cut = start + 4 + size // 2
    # Keep the header's index position pointing past the cut.
    check(tmp_path, small[:cut])


# -- block index ------------------------------------------------------------ #


def index_of(data: bytes):
    pos = i64(data, OFF_INDEX_POS)
    n = u32(data, pos)
    return pos, n, [i64(data, pos + 4 + 8 * k) for k in range(n)]


def test_index_count_too_small(tmp_path, big):
    pos, n, _ = index_of(big)
    assert n == 3
    check(tmp_path, patch(big, pos, struct.pack("<I", 1)))


def test_index_count_too_large(tmp_path, big):
    pos, _, _ = index_of(big)
    check(tmp_path, patch(big, pos, struct.pack("<I", 0x10000000)))


def test_index_duplicate_entry(tmp_path, big):
    pos, _, entries = index_of(big)
    check(tmp_path, patch(big, pos + 4 + 8, struct.pack("<q", entries[0])))


def test_index_entry_out_of_file(tmp_path, big):
    pos, _, _ = index_of(big)
    check(tmp_path, patch(big, pos + 4 + 16, struct.pack("<q", 1 << 40)))


def test_index_entry_negative(tmp_path, big):
    pos, _, _ = index_of(big)
    check(tmp_path, patch(big, pos + 4 + 8, struct.pack("<q", -8)))


def test_index_position_past_end(tmp_path, small):
    check(tmp_path, patch(small, OFF_INDEX_POS, struct.pack("<q", len(small) + 1000)))


# -- record count ----------------------------------------------------------- #


@pytest.mark.parametrize("n", [3, 4999, 5001, 70000, 1 << 40])
def test_record_count_disagrees_with_data(tmp_path, small, n):
    check(tmp_path, patch(small, OFF_NUM_RECORDS, struct.pack("<q", n)))


def test_record_count_disagrees_across_blocks(tmp_path, big):
    check(tmp_path, patch(big, OFF_NUM_RECORDS, struct.pack("<q", 65537)))


# -- metadata length -------------------------------------------------------- #


@pytest.mark.parametrize("n", [0, 0x7FFFFFFF, 0x80000000, 0x80000001, 0x80000100, 0xFFFFFF00, 0xFFFFFFFF])
def test_metadata_length_out_of_range(tmp_path, small, n):
    check(tmp_path, patch(small, OFF_META_LEN, struct.pack("<I", n)))


def test_metadata_length_past_end_of_file(tmp_path, small):
    check(tmp_path, patch(small, OFF_META_LEN, struct.pack("<I", len(small))))


def test_metadata_with_embedded_nul(tmp_path, small):
    start = data_start(small)
    meta_len = u32(small, OFF_META_LEN)
    # Put a NUL in the middle of the schema XML.
    mid = 512 + (meta_len // 2) * 2
    check(tmp_path, patch(small, mid, b"\x00\x00"))


# -- var data --------------------------------------------------------------- #

# One V_WString column: fixed part is the 4 byte position, then a 4 byte var
# data size, then the var data. The position is relative to the field, so a
# value of 8 is the first byte of var data.


@pytest.fixture(scope="module")
def vtemplate(tmp_path_factory):
    p = tmp_path_factory.mktemp("v") / "v.yxdb"
    with Writer(str(p), [field("s", "V_WString", 100)]) as w:
        w.write_record({"s": "x"})
    return p.read_bytes()


def vrecord(pos: int, var: bytes, var_size: int | None = None) -> bytes:
    size = len(var) if var_size is None else var_size
    return struct.pack("<I", pos) + struct.pack("<i", size) + var


def wide_var(text: str) -> bytes:
    raw = text.encode("utf-16-le")
    return bytes([(len(raw) << 1) | 1]) + raw


def test_hand_built_valid_record_reads(tmp_path, vtemplate):
    data = with_raw_chunk(vtemplate, vrecord(8, wide_var("hi")), 1)
    assert check(tmp_path, data, expect_error=False) == ("ok", 1)


@pytest.mark.parametrize(
    "rec",
    [
        vrecord(8 + 4, wide_var("hi")),  # points past the var data
        vrecord(4, wide_var("hi")),  # points into the size field
        vrecord(0x7FFFFFF0, wide_var("hi")),  # far outside
        vrecord(0x0FFFFFFF, wide_var("hi")),
        vrecord(8, bytes([(40 << 1) | 1]) + b"ab"),  # length exceeds var data
        vrecord(8, wide_var("hi") + b"x", var_size=0),  # var size smaller than prefix
        vrecord(8, wide_var("hi"), var_size=-1),
        vrecord(8, wide_var("hi"), var_size=-5),
        vrecord(8, wide_var("hi"), var_size=0x7FFFFFFF),
        vrecord(0x50000000 | 0x10000000, b""),  # small string form with bit 30 set
        vrecord(0x30000000, b""),  # 3 byte small string in a wide field (odd length)
        vrecord(8, bytes([(3 << 1) | 1]) + b"abc"),  # odd byte length for UTF-16
    ],
    ids=[
        "past-var-data",
        "into-size-field",
        "far-outside",
        "max-offset",
        "len-exceeds",
        "size-smaller-than-prefix",
        "size-minus-1",
        "size-minus-5",
        "size-huge",
        "small-string-bit30",
        "wide-odd-small-string",
        "wide-odd-length",
    ],
)
def test_var_data_corruption(tmp_path, vtemplate, rec):
    check(tmp_path, with_raw_chunk(vtemplate, rec, 1))


def test_short_final_record_raises(tmp_path, vtemplate):
    rec = vrecord(8, wide_var("hi"))
    # Two records claimed, one present.
    check(tmp_path, with_raw_chunk(vtemplate, rec, 2))


# -- valid files keep working ------------------------------------------------ #


def test_windowed_reads_across_blocks(tmp_path, big):
    from openyxdb._openyxdb import Reader

    p = tmp_path / "big.yxdb"
    p.write_bytes(big)
    with Reader(str(p)) as r:
        ids = r.read_columns_subset(["i"], 130000, 100)["i"]
    assert ids == list(range(130000, 130100))
    with Reader(str(p)) as r:
        ids = r.read_columns_subset(["i"], 65530, 20)["i"]
    assert ids == list(range(65530, 65550))
    with Reader(str(p)) as r:
        ids = r.read_columns_subset(["i"], 139990, 100)["i"]
    assert ids == list(range(139990, 140000))


def test_bitflip_check_has_no_crashes():
    """Short run of scripts/bitflip_check.py; the full run uses 300 flips."""
    import subprocess
    import sys
    from pathlib import Path

    script = Path(__file__).resolve().parent.parent / "scripts" / "bitflip_check.py"
    import os

    r = subprocess.run(
        [sys.executable, str(script), "--flips", "40", "--rows", "2000"],
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        timeout=600,
    )
    assert r.returncode == 0, r.stdout + r.stderr
    assert "crash         0 /" in r.stdout


def test_corruption_errors_are_format_errors(tmp_path, big):
    import openyxdb

    pos, _, _ = index_of(big)
    p = tmp_path / "bad.yxdb"
    p.write_bytes(patch(big, pos, struct.pack("<I", 1)))
    with pytest.raises(openyxdb.FormatError):
        openyxdb.read_yxdb(p)


# -- header fields and the end of the file ----------------------------------- #

OFF_SPATIAL_POS = 0x58
INT64_MAX = (1 << 63) - 1


@pytest.mark.parametrize("n", [INT64_MAX, INT64_MAX - 65534, INT64_MAX - 65535])
def test_record_count_near_int64_max(tmp_path, small, n):
    check(tmp_path, patch(small, OFF_NUM_RECORDS, struct.pack("<q", n)))


@pytest.mark.parametrize("pos", [INT64_MAX, INT64_MAX - 3, INT64_MAX - 4])
def test_index_position_near_int64_max(tmp_path, small, pos):
    check(tmp_path, patch(small, OFF_INDEX_POS, struct.pack("<q", pos)))


@pytest.mark.parametrize("pos", [1, 512, -1, INT64_MAX, 1 << 40])
def test_spatial_position_on_a_file_without_a_spatial_index(tmp_path, small, pos):
    assert i64(small, OFF_SPATIAL_POS) == 0
    check(tmp_path, patch(small, OFF_SPATIAL_POS, struct.pack("<q", pos)))


@pytest.mark.parametrize("extra", [b"\0", b"\0" * 8, b"junk"])
def test_bytes_after_the_block_index(tmp_path, small, extra):
    check(tmp_path, small + extra)


def test_schema_length_error_is_not_blamed_on_finalization(tmp_path, small):
    status, detail = check(tmp_path, patch(small, OFF_META_LEN, struct.pack("<I", 0x7FFFFFFF)))
    assert "schema length" in detail
