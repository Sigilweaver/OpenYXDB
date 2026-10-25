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

    class Short(real):
        def read(self, columns=None, *, offset=0, limit=None):
            t = super().read(columns, offset=offset, limit=limit)
            return openyxdb.Table(t.fields, [c.values[:-1] for c in t.columns])

    lf = openyxdb.scan_yxdb(p)
    monkeypatch.setattr(openyxdb, "Reader", Short)
    with pytest.raises((pl.exceptions.ComputeError, openyxdb.FormatError), match="returned 9 rows"):
        lf.collect()


def test_scan_full_read_still_works(tmp_path):
    p = tmp_path / "s.yxdb"
    openyxdb.write_yxdb(p, {"a": list(range(10))})
    assert openyxdb.scan_yxdb(p).collect()["a"].to_list() == list(range(10))


def test_fixed_decimal_is_the_correctly_rounded_double_of_its_text(tmp_path):
    """Stored text '0.366119' must read as 0.366119, not 0.36611899999999997."""
    p = tmp_path / "fd2.yxdb"
    values = [0.366119, 0.991230, 9.034802, 0.063062, 0.143997, 0.141749, 0.444244]
    with Writer(str(p), [fi("d", "FixedDecimal", 19, 6)]) as w:
        for v in values:
            w.write_record({"d": v})
    with Reader(str(p)) as r:
        assert [dict(x)["d"] for x in r.read_records()] == values
