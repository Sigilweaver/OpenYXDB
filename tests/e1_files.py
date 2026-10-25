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
