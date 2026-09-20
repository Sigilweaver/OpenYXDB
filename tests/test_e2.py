"""E2 reader tests against reviewed real-world corpus outcomes.

Unsupported encodings must fail with the hash-specific error and record number
recorded below. Every other fixture must decode completely.
"""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path

import openyxdb
import pytest
from conftest import corpus_files
from openyxdb._openyxdb import Reader

ALL_FILES = corpus_files("e2")

if not ALL_FILES:
    pytest.skip("E2 corpus not available", allow_module_level=True)


# Reviewed strict-decoder baseline. These files use encodings that are not yet
# understood well enough to return values safely. Key by content hash so a
# renamed or replaced fixture cannot inherit an expected failure by accident.
KNOWN_UNSUPPORTED = {
    "e68fc1f7a8771eab5a6c1637e827c7f1406c91180a24e5658b1fdd8dd7b1942e":
        r"^E2: failed to decode record 0: no exact match for the declared schema$",
    "fbc5b83fd88256eba152f79afe1811795b6e585831fc65225697dbcd681c8530":
        r"^E2: failed to decode record 0: no exact match for the declared schema$",
    "b06b54bf232f0e7889bd6bad96a45dde952aa0b95aeb721f63d6a19fd4db19e5":
        r"^E2: failed to decode record 4: no exact match for the declared schema$",
    "dec8f038dcce4a07ea425f8711c967134c4ccc31300cd51bc38d824f7730cae2":
        r"^E2: failed to decode record 10: no exact match for the declared schema$",
    "7dfb79432fde920670425606f24229afd713e82e165875469814cd917faf76c3":
        r"^E2: ambiguous Int32 encoding in record 41$",
    "7ab36db86d50b3aec6ec6620a3098a8c8b1a9fe031dd7b87359e4d3816cea66f":
        r"^E2: ambiguous Int32 encoding in record 4537$",
    "b39d8516fb12922e509a2b7ecef27b3cc59ea2d672aee703d73e768f5cf340ec":
        r"^E2: failed to decode record 0: no exact match for the declared schema$",
    "b1d89869879c269654c0407ebbda06b01f3453de8d5a7e4407a30982e8fb8f12":
        r"^E2: ambiguous Int32 encoding in record 37$",
    "7eca4c3cb1eeb0ad8e174989dd795d4f7b4127a828a549a8f45cc87162ec320e":
        r"^E2: ambiguous Int32 encoding in record 205$",
    "0016c205e7983f71438dcbd22101cdf559a0b3dcb7640e9d13db7c6aff535d48":
        r"^E2: ambiguous Int32 encoding in record 5$",
    "1099621aa7f5ba0fe48a1377d3146dc5ecd21f07476e0c71a21e404c8a7d9fcf":
        r"^E2: ambiguous Int32 encoding in record 5$",
    "9291a2eb0e072dbfb8b49757b5f06374d6ef8af1470921b1ad05c254ddb16262":
        r"^E2: ambiguous Int32 encoding in record 32$",
    "5eeddc560976e28ca8a1302887a6e5d667f0bde996dc58bc779040c4bc5dddb3":
        r"^E2: ambiguous Int32 encoding in record 68$",
    "6a41c1522c60d1d2b778fd7a3c3d220163b46219f966329747ce67cdd4d999f4":
        r"^E2: ambiguous Int32 encoding in record 48789$",
    "9ad5fe00a6e9e0184a6b1c35a85b5d937151ffd82a3d880fd099e910059d3bf4":
        r"^E2: ambiguous Int32 encoding in record 0$",
    "baeec5eff8c7702b0d944151f06d75b27391196fb1c84afce73f41cb5d81117d":
        r"^E2: ambiguous Int32 encoding in record 10$",
}


@lru_cache(maxsize=None)
def _digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _supported_files() -> list[Path]:
    return [path for path in ALL_FILES if _digest(path) not in KNOWN_UNSUPPORTED]


def test_format_autodetect_smoke() -> None:
    """Every file in the E2 corpus must be detected as E2."""
    for path in ALL_FILES:
        with Reader(str(path)) as reader:
            assert reader.format == "E2", path.name


def test_schema_decodes_for_corpus() -> None:
    """Every reviewed E2 fixture must expose a schema."""
    for path in ALL_FILES:
        with Reader(str(path)) as reader:
            assert reader.schema, path.name


@pytest.mark.parametrize("path", ALL_FILES, ids=lambda p: p.name)
def test_num_records_matches_decoded_rows(path: Path) -> None:
    """``num_records`` must equal the number of rows the reader decodes."""
    with Reader(str(path)) as r:
        if r.format != "E2":
            pytest.skip("not an E2 file")
        declared = r.num_records
        expected_error = KNOWN_UNSUPPORTED.get(_digest(path))
        if expected_error:
            with pytest.raises(RuntimeError, match=expected_error):
                r.read_columns()
            return
        columns = r.read_columns()
    decoded = len(next(iter(columns.values()))) if columns else 0
    assert declared == decoded, path.name


def test_num_records_is_stable_across_calls() -> None:
    """Repeated reads must not change the reported record count."""
    for path in _supported_files():
        with Reader(str(path)) as r:
            if r.format != "E2":
                continue
            first = r.num_records
            if first == 0:
                continue
            assert r.num_records == first, path.name
            return
    pytest.skip("no non-empty E2 file in corpus")


def test_amazon_int32_prices_use_the_valid_exact_interpretation() -> None:
    """Reject the ambiguous row while preserving reviewed later prices."""
    amazon_hash = "9291a2eb0e072dbfb8b49757b5f06374d6ef8af1470921b1ad05c254ddb16262"
    path = next((item for item in ALL_FILES if _digest(item) == amazon_hash), None)
    if path is None:
        pytest.skip("reviewed Amazon fixture not present")
    with Reader(str(path)) as reader:
        with pytest.raises(RuntimeError, match=r"ambiguous Int32 encoding in record 32$"):
            reader.read_columns_subset(
                ["product_id", "discounted_price", "actual_price"], 32, 1
            )
    expected = {
        43: ("B0B997FBZT", 35_999, 49_990),
        60: ("B0B15GSPQW", 47_990, 70_900),
    }
    for row, values in expected.items():
        with Reader(str(path)) as reader:
            columns = reader.read_columns_subset(
                ["product_id", "discounted_price", "actual_price"], row, 1
            )
        assert tuple(column[0] for column in columns.values()) == values


def test_limited_subset_read_does_not_change_num_records() -> None:
    """A windowed read must not redefine the file's total record count."""
    for path in _supported_files():
        with Reader(str(path)) as r:
            if r.format != "E2":
                continue
            total = r.num_records
            if total < 3:
                continue
            r.read_columns_subset(None, 0, 2)
            assert r.num_records == total, path.name
            return
    pytest.skip("no E2 file with >=3 records in corpus")


def test_read_columns_subset_projection() -> None:
    """Projection must return only the requested columns, in order."""
    for path in _supported_files():
        with Reader(str(path)) as r:
            schema = r.schema
            if len(schema) < 2:
                continue
            wanted = [schema[1].name, schema[0].name]
            cols = r.read_columns_subset(wanted, 0, 8)
            assert list(cols.keys()) == wanted
            return
    pytest.skip("no E2 file with >=2 columns in corpus")


def test_read_columns_subset_offset_limit() -> None:
    """Offset + limit must clamp gracefully."""
    for path in _supported_files():
        with Reader(str(path)) as r:
            cols = r.read_columns_subset(None, 0, 3)
            lengths = {len(v) for v in cols.values()}
            assert len(lengths) == 1
            assert next(iter(lengths)) <= 3
            return
    pytest.skip("no E2 file produced a window read")


def test_to_polars_smoke() -> None:
    """High-level ``to_polars`` must work end-to-end for at least one E2 file."""
    pytest.importorskip("polars")
    for path in _supported_files():
        df = openyxdb.to_polars(str(path))
        assert df.width >= 0
        return
    pytest.skip("no E2 file successfully converted via to_polars")


def test_scan_yxdb_matches_eager_read() -> None:
    """The lazy scan must return every row, matching the eager read."""
    pytest.importorskip("polars")
    checked = 0
    supported = _supported_files()
    for path in supported[:: max(1, len(supported) // 15)][:15]:
        ref = openyxdb.to_polars(str(path))
        got = openyxdb.scan_yxdb(str(path)).collect()
        assert got.columns == ref.columns, path.name
        assert got.height == ref.height, path.name
        assert got.equals(ref), path.name
        checked += 1
    assert checked > 0, "no E2 file exercised through scan_yxdb"


def test_scan_yxdb_head_matches_prefix() -> None:
    """Row-limit pushdown on an E2 scan must return the first k rows."""
    pytest.importorskip("polars")
    for path in _supported_files():
        ref = openyxdb.to_polars(str(path))
        if ref.height < 5:
            continue
        k = min(3, ref.height)
        got = openyxdb.scan_yxdb(str(path)).head(k).collect()
        assert got.height == k, path.name
        assert got.equals(ref.head(k)), path.name
        return
    pytest.skip("no E2 file with >=5 rows in corpus")
