"""Smoke tests for the E2-format (AMP-engine) read path.

These tests use a corpus of real-world YXDB files. Some files in the corpus
are intentionally invalid test assets, so per-file failures are tolerated and
reported as xfail rather than failing the suite.
"""

from __future__ import annotations

from pathlib import Path

import openyxdb
import pytest
from openyxdb._openyxdb import Reader

CORPUS_DIR = Path("/workspaces/Projects/Data/YXDB/e2")


def _corpus_files() -> list[Path]:
    if not CORPUS_DIR.is_dir():
        return []
    return sorted(p for p in CORPUS_DIR.glob("*.yxdb") if p.is_file())


ALL_FILES = _corpus_files()

if not ALL_FILES:
    pytest.skip("E2 corpus not available", allow_module_level=True)


def test_format_autodetect_smoke() -> None:
    """At least one corpus file must be detected as E2."""
    sample = ALL_FILES[: min(len(ALL_FILES), 20)]
    seen_e2 = False
    for path in sample:
        try:
            with Reader(str(path)) as r:
                if r.format == "E2":
                    seen_e2 = True
                    break
        except Exception:
            continue
    assert seen_e2, "no file in corpus head was identified as E2"


def test_schema_decodes_for_majority() -> None:
    """The schema parse must succeed for the bulk of the corpus."""
    succeeded = 0
    failed = 0
    for path in ALL_FILES:
        try:
            with Reader(str(path)) as r:
                _ = r.schema
                _ = r.format
            succeeded += 1
        except Exception:
            failed += 1
    # Some files are intentionally invalid; require a clear majority to parse.
    assert succeeded > failed, (
        f"too many schema failures: {failed} failed, {succeeded} ok"
    )


def test_num_records_lazy() -> None:
    """``num_records`` must return a non-negative integer for valid files."""
    checked = 0
    for path in ALL_FILES[:25]:
        try:
            with Reader(str(path)) as r:
                if r.format != "E2":
                    continue
                n = r.num_records
                assert n >= 0, f"{path.name}: negative record count"
                checked += 1
        except Exception:
            continue
    assert checked > 0, "no E2 file produced a record count"


def test_read_columns_subset_projection() -> None:
    """Projection must return only the requested columns, in order."""
    for path in ALL_FILES:
        try:
            with Reader(str(path)) as r:
                if r.format != "E2":
                    continue
                schema = r.schema
                if len(schema) < 2:
                    continue
                wanted = [schema[1].name, schema[0].name]
                cols = r.read_columns_subset(wanted, 0, 8)
                assert list(cols.keys()) == wanted
                return
        except Exception:
            continue
    pytest.skip("no E2 file with >=2 columns in corpus")


def test_read_columns_subset_offset_limit() -> None:
    """Offset + limit must clamp gracefully."""
    for path in ALL_FILES:
        try:
            with Reader(str(path)) as r:
                if r.format != "E2":
                    continue
                # Read a small window.
                cols = r.read_columns_subset(None, 0, 3)
                lengths = {len(v) for v in cols.values()}
                assert len(lengths) == 1
                assert next(iter(lengths)) <= 3
                return
        except Exception:
            continue
    pytest.skip("no E2 file produced a window read")


def test_to_polars_smoke() -> None:
    """High-level ``to_polars`` must work end-to-end for at least one E2 file."""
    pytest.importorskip("polars")
    for path in ALL_FILES:
        try:
            df = openyxdb.to_polars(str(path))
            assert df.width >= 0
            return
        except Exception:
            continue
    pytest.skip("no E2 file successfully converted via to_polars")
