"""Reader-side checks in the Python layer."""

import pytest

import openyxdb
from openyxdb._openyxdb import FieldInfo, Reader, Writer

pl = pytest.importorskip("polars")


def fi(name, type_, size, scale=0):
    f = FieldInfo()
    f.name, f.type, f.size, f.scale = name, type_, size, scale
    return f


def utf16(s):
    return s.encode("utf-16-le")


@pytest.fixture
def dup_file(tmp_path):
    """An E2 file whose two fields are both named 'v'.

    The E1 reader renames duplicates (v, v2) itself; the E2 reader keeps the
    names as written.
    """
    from test_e2_strict import _write_e2

    blob = b"\x89" + b"123456789"
    return _write_e2(tmp_path / "dup.yxdb", [("v", "Blob"), ("v", "Blob")], blob * 2)


def test_duplicate_names_raise_on_read(dup_file):
    with Reader(str(dup_file)) as r:
        assert [f.name for f in r.schema] == ["v", "v"]
        with pytest.raises(openyxdb.SchemaError, match="both named 'v'"):
            r.read_columns()
        with pytest.raises(openyxdb.SchemaError, match="both named 'v'"):
            r.read_records()
        with pytest.raises(openyxdb.SchemaError, match="both named 'v'"):
            r.read_columns_subset(["v"])


def test_duplicate_names_raise_in_high_level_readers(dup_file):
    pytest.importorskip("pyarrow")
    with pytest.raises(openyxdb.SchemaError):
        openyxdb.read_yxdb(dup_file)
    with pytest.raises(openyxdb.SchemaError):
        openyxdb.to_pyarrow(dup_file)
    with pytest.raises(openyxdb.SchemaError):
        openyxdb.scan_yxdb(dup_file)


def test_fixed_decimal_partial_number_raises(tmp_path):
    p = tmp_path / "fd.yxdb"
    with Writer(str(p), [fi("d", "FixedDecimal", 5, 2)]) as w:
        w.write_record({"d": 12.5})
    with Reader(str(p)) as r:
        assert dict(r.read_records()[0])["d"] == 12.5
    data = p.read_bytes()
    assert data.count(b"12.50") == 1
    p.write_bytes(data.replace(b"12.50", b"12a50"))
    with Reader(str(p)) as r:
        with pytest.raises(openyxdb.ConversionError, match="not a number"):
            r.read_records()


def test_scan_raises_on_short_batch(tmp_path, monkeypatch):
    p = tmp_path / "s.yxdb"
    openyxdb.write_yxdb(p, {"a": list(range(10))})
    real = openyxdb.Reader

    class Short:
        def __init__(self, *a, **k):
            self._r = real(*a, **k)

        def __getattr__(self, name):
            return getattr(self._r, name)

        def __enter__(self):
            self._r.__enter__()
            return self

        def __exit__(self, *a):
            return self._r.__exit__(*a)

        def read_columns_subset(self, cols, offset, limit):
            out = self._r.read_columns_subset(cols, offset, limit)
            return {k: v[:-1] for k, v in out.items()}

    lf = openyxdb.scan_yxdb(p)
    monkeypatch.setattr(openyxdb, "Reader", Short)
    with pytest.raises(pl.exceptions.ComputeError, match="returned 9 rows"):
        lf.collect()


def test_scan_full_read_still_works(tmp_path):
    p = tmp_path / "s.yxdb"
    openyxdb.write_yxdb(p, {"a": list(range(10))})
    assert openyxdb.scan_yxdb(p).collect()["a"].to_list() == list(range(10))
