"""Helpers for building and corrupting small E1 (.yxdb) files in tests."""

from __future__ import annotations

import struct
from pathlib import Path

from openyxdb._openyxdb import FieldInfo, Writer

# Header layout (see HeaderData in include/Open_AlteryxYXDB.h)
OFF_FILE_ID = 0x40
OFF_META_LEN = 0x50
OFF_SPATIAL_POS = 0x58
OFF_INDEX_POS = 0x60
OFF_NUM_RECORDS = 0x68
OFF_COMPRESSION = 0x70
HEADER_SIZE = 512


def field(name: str, type_: str, size: int, scale: int = 0) -> FieldInfo:
    fi = FieldInfo()
    fi.name = name
    fi.type = type_
    fi.size = size
    fi.scale = scale
    return fi


def write_int_file(path: Path, rows: int = 5) -> bytes:
    """Write a one-column Int32 file and return its bytes."""
    with Writer(str(path), [field("x", "Int32", 4)]) as w:
        for i in range(rows):
            w.write_record({"x": i})
    return path.read_bytes()


def patch(data: bytes, offset: int, new: bytes) -> bytes:
    return data[:offset] + new + data[offset + len(new) :]


def replace_utf16(data: bytes, old: str, new: str) -> bytes:
    """Replace every occurrence in the UTF-16 metadata; lengths must match."""
    assert len(old) == len(new)
    o, n = old.encode("utf-16-le"), new.encode("utf-16-le")
    assert data.count(o) >= 1, old
    return data.replace(o, n)


def u32(data: bytes, offset: int) -> int:
    return struct.unpack_from("<I", data, offset)[0]


def i64(data: bytes, offset: int) -> int:
    return struct.unpack_from("<q", data, offset)[0]


def run_read(path, timeout: int = 60):
    """Read ``path`` in a child process.

    Returns ("ok", row_count), ("error", message) or ("crash", detail) so that a
    reader that corrupts memory cannot take the test process down with it.
    """
    import os
    import subprocess
    import sys

    code = (
        "import sys\n"
        "from openyxdb._openyxdb import Reader\n"
        "r = Reader(sys.argv[1])\n"
        "rows = r.read_records()\n"
        "print(len(rows))\n"
    )
    try:
        p = subprocess.run(
            [sys.executable, "-c", code, str(path)],
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        )
    except subprocess.TimeoutExpired:
        return "crash", "timeout"
    if p.returncode == 0:
        return "ok", int(p.stdout.strip())
    if "Traceback" in p.stderr:
        return "error", p.stderr.strip().splitlines()[-1]
    return "crash", f"exit {p.returncode}: {p.stderr.strip()[-200:]}"


def data_start(data: bytes) -> int:
    return HEADER_SIZE + u32(data, OFF_META_LEN) * 2


def with_raw_chunk(template: bytes, payload: bytes, n_records: int) -> bytes:
    """Rebuild ``template`` with one uncompressed LZF chunk holding ``payload``."""
    start = data_start(template)
    head = bytearray(template[:start])
    chunk = struct.pack("<I", 0x80000000 | len(payload)) + payload
    index_pos = start + len(chunk)
    struct.pack_into("<q", head, OFF_INDEX_POS, index_pos)
    struct.pack_into("<q", head, OFF_NUM_RECORDS, n_records)
    index = struct.pack("<I", 1 if n_records else 0) + (
        struct.pack("<q", start) if n_records else b""
    )
    return bytes(head) + chunk + index
