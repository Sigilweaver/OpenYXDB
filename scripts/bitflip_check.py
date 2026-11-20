#!/usr/bin/env python3
"""Single-bit-flip check for the E1 reader.

Writes a deterministic 5000-row file, flips one bit at a time inside the
record stream (between the metadata and the block index), and reads each
mutated copy in a subprocess so crashes are counted instead of aborting the
run. Each flip is classified as:

  error      the reader raised
  unchanged  the read returned exactly the original data
  changed    the read returned different data with no error (silent)
  crash      the reader process died or timed out

The format has no checksum, so a flip inside literal data bytes can change a
value without breaking framing; no reader can detect those. The number worth
watching is `crash`, which must be zero, and the error share.

Usage: python scripts/bitflip_check.py [--flips 300] [--seed 1] [--region stream|header|schema|index|all]
Exits non-zero if any flip crashes the reader. The default region is the record
stream; the other regions flip bits in the 512-byte header, the schema XML, or
the block index and anything after it.
"""

from __future__ import annotations

import argparse
import hashlib
import random
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

import openyxdb
from openyxdb import FieldInfo

READ_SNIPPET = (
    "import sys, hashlib, openyxdb\n"
    "d = openyxdb.read_yxdb(sys.argv[1])\n"
    "print(hashlib.sha256(repr(sorted(d.items())).encode()).hexdigest())\n"
)


def _field(name: str, type_: str, size: int) -> FieldInfo:
    fi = FieldInfo()
    fi.name = name
    fi.type = type_
    fi.size = size
    fi.scale = 0
    return fi


def build_file(path: Path, rows: int) -> None:
    rng = random.Random(7)
    schema = [
        _field("id", "Int32", 4),
        _field("name", "V_WString", 64),
        _field("score", "Double", 8),
        _field("tag", "String", 12),
    ]
    cols = {
        "id": list(range(rows)),
        "name": [f"name-{rng.randrange(10**6)}-{i % 97}" for i in range(rows)],
        "score": [rng.random() * 1000 for _ in range(rows)],
        "tag": [rng.choice(["alpha", "beta", "gamma", "delta"]) for _ in range(rows)],
    }
    openyxdb.write_yxdb(str(path), cols, schema)


def stream_bounds(data: bytes) -> tuple[int, int]:
    """Return (start, end) byte offsets of the record stream."""
    # Header: 512 bytes, then UTF-16 metadata. nMetaInfoLen is at 0x50 and
    # nRecordBlockIndexPos at 0x60 (see HeaderData in Open_AlteryxYXDB.h).
    start = 512 + struct.unpack_from("<I", data, 0x50)[0] * 2
    end = struct.unpack_from("<q", data, 0x60)[0]
    return start, end


def read_hash(path: Path) -> tuple[str, str]:
    try:
        r = subprocess.run(
            [sys.executable, "-c", READ_SNIPPET, str(path)],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        return "crash", "timeout"
    if r.returncode == 0:
        return "ok", r.stdout.strip()
    if "Traceback" in r.stderr:
        return "error", r.stderr.strip().splitlines()[-1]
    return "crash", f"exit {r.returncode}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--flips", type=int, default=300)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--rows", type=int, default=5000)
    ap.add_argument(
        "--region", choices=["stream", "header", "schema", "index", "all"], default="stream"
    )
    args = ap.parse_args()

    counts = {"error": 0, "unchanged": 0, "changed": 0, "crash": 0}
    with tempfile.TemporaryDirectory() as td:
        src = Path(td) / "src.yxdb"
        build_file(src, args.rows)
        data = src.read_bytes()
        status, base = read_hash(src)
        if status != "ok":
            print("baseline read failed:", base)
            return 2
        start, end = stream_bounds(data)
        meta_end = start
        regions = {
            "stream": (start, end),
            "header": (0, 512),
            "schema": (512, meta_end),
            "index": (end, len(data)),
            "all": (0, len(data)),
        }
        lo, hi = regions[args.region]
        rng = random.Random(args.seed)
        mut = Path(td) / "mut.yxdb"
        for _ in range(args.flips):
            pos = rng.randrange(lo, hi)
            bit = rng.randrange(8)
            buf = bytearray(data)
            buf[pos] ^= 1 << bit
            mut.write_bytes(bytes(buf))
            status, out = read_hash(mut)
            if status == "ok":
                counts["unchanged" if out == base else "changed"] += 1
            else:
                counts[status] += 1
    total = args.flips
    for k, v in counts.items():
        print(f"{k:10s} {v:4d} / {total}")
    return 1 if counts["crash"] else 0


if __name__ == "__main__":
    sys.exit(main())
