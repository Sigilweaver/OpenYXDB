"""Correctness + pushdown tests for ``openyxdb.scan_yxdb`` against the E1 corpus.

Each test iterates the real-world YXDB files under ``OpenYXDB/e1/`` and asserts
that the lazy scan behaves identically to the eager readers for full collects,
projection, and row-limit pushdown. Also includes a perf guard that confirms
``head(k)`` is near-constant-time regardless of file size.
"""

from __future__ import annotations

import time
from pathlib import Path

import openyxdb
import pytest
from openyxdb._openyxdb import Reader

pl = pytest.importorskip("polars")

CORPUS_DIR = Path(__file__).resolve().parent.parent / "e1"


def _corpus_files() -> list[Path]:
    if not CORPUS_DIR.is_dir():
        return []
    return sorted(p for p in CORPUS_DIR.glob("*.yxdb") if p.is_file())


ALL_FILES = _corpus_files()

if not ALL_FILES:
    pytest.skip("E1 corpus not available", allow_module_level=True)


# Cap the per-file full-scan test at files of reasonable size so the suite
# stays fast in CI. The largest files are covered separately by the
# dedicated pushdown / perf tests below.
_FULL_SCAN_SIZE_CAP = 5 * 1024 * 1024  # 5 MB
FULL_SCAN_FILES = [p for p in ALL_FILES if p.stat().st_size <= _FULL_SCAN_SIZE_CAP]


@pytest.mark.parametrize(
    "path",
    FULL_SCAN_FILES,
    ids=lambda p: p.name,
)
def test_scan_full_matches_to_polars(path: Path) -> None:
    """Full lazy collect must equal the eager Polars read."""
    ref = openyxdb.to_polars(str(path))
    got = openyxdb.scan_yxdb(str(path)).collect()
    assert got.height == ref.height, path.name
    assert got.columns == ref.columns, path.name
    assert got.equals(ref), path.name


def test_scan_head_matches_prefix() -> None:
    """``scan_yxdb(...).head(k).collect()`` must equal the first k rows."""
    # Exercise a range of files, not just one.
    sample = ALL_FILES[:: max(1, len(ALL_FILES) // 20)][:20]
    for path in sample:
        with Reader(str(path)) as r:
            n = r.num_records
        if n == 0:
            # head of empty is empty; covered separately.
            continue
        k = min(100, n)
        ref = openyxdb.to_polars(str(path)).head(k)
        got = openyxdb.scan_yxdb(str(path)).head(k).collect()
        assert got.height == ref.height, path.name
        assert got.equals(ref), path.name


def test_scan_projection_matches_selection() -> None:
    """``.select(cols).collect()`` must equal the same projection applied eagerly."""
    sample = ALL_FILES[:: max(1, len(ALL_FILES) // 10)][:20]
    for path in sample:
        with Reader(str(path)) as r:
            names = [fi.name for fi in r.schema]
        if not names:
            continue
        # Reverse + drop one to ensure order and subset both matter.
        proj = list(reversed(names))[: max(1, len(names) // 2)]
        ref = openyxdb.to_polars(str(path)).select(proj).head(500)
        got = openyxdb.scan_yxdb(str(path)).select(proj).head(500).collect()
        assert got.columns == proj, path.name
        assert got.equals(ref), path.name


def test_scan_offset_via_skip_matches() -> None:
    """Polars ``slice`` on top of scan must produce correct offsets.

    The plugin does not implement slice-offset pushdown, so this confirms
    the fallback behaviour is still correct.
    """
    # Pick a file with enough rows to make offset interesting.
    candidates = []
    for p in ALL_FILES:
        with Reader(str(p)) as r:
            if r.num_records >= 50:
                candidates.append((p, r.num_records))
        if len(candidates) >= 5:
            break
    assert candidates, "no files with >=50 rows in corpus"
    for path, n in candidates:
        ref = openyxdb.to_polars(str(path)).slice(10, 20)
        got = openyxdb.scan_yxdb(str(path)).slice(10, 20).collect()
        assert got.equals(ref), path.name


def test_head_is_near_constant_time() -> None:
    """``head(k)`` must not scale with file size — row-limit pushdown works."""
    # Use the largest file in the corpus as the stress case.
    largest = max(ALL_FILES, key=lambda p: p.stat().st_size)
    size_mb = largest.stat().st_size / 1e6
    # Warm caches (OS page cache + any lazy schema init).
    openyxdb.scan_yxdb(str(largest)).head(10).collect()

    t0 = time.perf_counter()
    df = openyxdb.scan_yxdb(str(largest)).head(1000).collect()
    dt = time.perf_counter() - t0

    assert df.height == min(1000, df.height)
    # Generous budget: 500 ms on a multi-MB file. The full scan on the same
    # file takes hundreds of ms to seconds; head must be orders of magnitude
    # faster.
    assert dt < 0.5, (
        f"head(1000) on {largest.name} ({size_mb:.1f} MB) took {dt * 1000:.0f}ms; "
        "row-limit pushdown regressed"
    )


def test_projection_reduces_work() -> None:
    """Projection pushdown: selecting 1 column must be faster than full collect."""
    largest = max(ALL_FILES, key=lambda p: p.stat().st_size)
    with Reader(str(largest)) as r:
        names = [fi.name for fi in r.schema]
    if len(names) < 2:
        pytest.skip("need multi-column file for projection timing test")

    # Warm
    openyxdb.scan_yxdb(str(largest)).head(100).collect()

    t0 = time.perf_counter()
    openyxdb.scan_yxdb(str(largest)).collect()
    t_full = time.perf_counter() - t0

    t0 = time.perf_counter()
    openyxdb.scan_yxdb(str(largest)).select(names[:1]).collect()
    t_proj = time.perf_counter() - t0

    # Single-column scan should be meaningfully faster than full-column scan.
    # Use a lenient ratio to avoid CI flakiness.
    assert t_proj < t_full, (
        f"projection scan ({t_proj * 1000:.0f}ms) not faster than full "
        f"({t_full * 1000:.0f}ms) for {largest.name}"
    )


def test_unknown_projection_column_raises() -> None:
    """Projecting a non-existent column surfaces a clear error."""
    path = ALL_FILES[0]
    with pytest.raises(Exception):
        # Polars may wrap the underlying ValueError.
        openyxdb.scan_yxdb(str(path)).select("__definitely_not_a_column__").collect()
