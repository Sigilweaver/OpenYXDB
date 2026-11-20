"""Sizes read from an E2 file are checked against the file before memory is reserved."""

from __future__ import annotations

import struct
import subprocess
import sys
from pathlib import Path

import pytest
from test_e2_strict import _snappy_literal, _write_e2

import openyxdb
from openyxdb._openyxdb import Reader

BLOB = b"\x89" + b"123456789"


def block_size_offset(data: bytes) -> int:
    meta = struct.unpack_from("<I", data, 96)[0]
    return 100 + meta + 1  # the block type byte comes first


def run_limited(path: Path, limit_mb: int = 512):
    """Read ``path`` in a child whose address space is capped, so reserving the
    declared size would fail the test instead of succeeding slowly."""
    if sys.platform in ("darwin", "win32"):
        pytest.skip("RLIMIT_AS cannot be applied on this platform (macOS does not enforce it; Windows has no resource module)")
    code = (
        "import resource, sys\n"
        f"resource.setrlimit(resource.RLIMIT_AS, ({limit_mb} << 20, {limit_mb} << 20))\n"
        "import openyxdb\n"
        "from openyxdb._openyxdb import Reader\n"
        "try:\n"
        "    Reader(sys.argv[1]).read_columns()\n"
        "except openyxdb.FormatError as e:\n"
        "    print('FormatError', e)\n"
        "except MemoryError:\n"
        "    print('MemoryError')\n"
    )
    import os

    p = subprocess.run(
        [sys.executable, "-c", code, str(path)],
        capture_output=True,
        text=True,
        timeout=120,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
    )
    return p.returncode, p.stdout.strip(), p.stderr.strip()[-200:]


def test_huge_record_block_size_is_rejected_before_allocating(tmp_path):
    path = _write_e2(tmp_path / "big.yxdb", [("v", "Blob")], BLOB)
    data = bytearray(path.read_bytes())
    struct.pack_into("<I", data, block_size_offset(bytes(data)), 0xFFFFFFF0)
    path.write_bytes(bytes(data))
    code, out, err = run_limited(path)
    assert code == 0 and out.startswith("FormatError") and "extends past the end" in out, (code, out, err)


def test_huge_metadata_size_is_rejected_before_allocating(tmp_path):
    path = _write_e2(tmp_path / "meta.yxdb", [("v", "Blob")], BLOB)
    data = bytearray(path.read_bytes())
    struct.pack_into("<I", data, 96, 60 * 1024 * 1024)
    path.write_bytes(bytes(data))
    code, out, err = run_limited(path)
    assert code == 0 and out.startswith("FormatError") and "extends past the end" in out, (code, out, err)


def test_snappy_length_larger_than_the_input_can_produce_is_rejected(tmp_path):
    # Declared output of 3 GiB from a 10 byte payload.
    varint = bytearray()
    n = 3 << 30
    while n >= 0x80:
        varint.append((n & 0x7F) | 0x80)
        n >>= 7
    varint.append(n)
    payload = bytes(varint) + b"\x00A"
    path = _write_e2(tmp_path / "snappy.yxdb", [("v", "Blob")], BLOB)
    data = path.read_bytes()
    off = block_size_offset(data)
    body = b"\x0a" + payload
    tail = data[off + 4 + struct.unpack_from("<I", data, off)[0] :]
    path.write_bytes(data[:off] + struct.pack("<I", len(body)) + body + tail)
    code, out, err = run_limited(path)
    assert code == 0 and out.startswith("FormatError") and "larger than the input can produce" in out, (code, out, err)


def test_snappy_output_cannot_exceed_its_declared_length(tmp_path):
    # Declares 4 bytes, then supplies a 5 byte literal.
    payload = b"\x04" + b"\x10" + b"ABCDE"
    path = _write_e2(tmp_path / "over.yxdb", [("v", "Blob")], BLOB)
    data = path.read_bytes()
    off = block_size_offset(data)
    body = b"\x0a" + payload
    tail = data[off + 4 + struct.unpack_from("<I", data, off)[0] :]
    path.write_bytes(data[:off] + struct.pack("<I", len(body)) + body + tail)
    with pytest.raises(openyxdb.FormatError, match="exceeds the declared length"):
        Reader(str(path)).read_columns()


def test_valid_file_still_reads(tmp_path):
    path = _write_e2(tmp_path / "ok.yxdb", [("v", "Blob")], BLOB)
    assert Reader(str(path)).read_columns() == {"v": [b"123456789"]}
