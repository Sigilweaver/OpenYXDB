"""Options, leniency, warnings and the Table result."""

import dataclasses

import pytest

import openyxdb
from openyxdb import (
    Diagnostic,
    FieldInfo,
    Leniency,
    OpenYXDBWarning,
    ReadOptions,
    Reader,
    SchemaError,
    Table,
    WarningKind,
    WriteOptions,
    Writer,
)


def fi(name, type_="Int32", size=4):
    return FieldInfo(name=name, type=type_, size=size)


@pytest.fixture
def two_cols(tmp_path):
    p = tmp_path / "t.yxdb"
    with Writer(p, [fi("a"), fi("b", "V_WString", 10)]) as w:
        w.write_columns({"a": [1, 2, 3, 4], "b": ["w", "x", "y", "z"]})
    return p


def dup_names_file(tmp_path, first="va", second="vb", patched=None):
    """An E1 file whose schema names two fields alike (the writer refuses to)."""
    p = tmp_path / "dup.yxdb"
    with Writer(p, [fi(first), fi(second)]) as w:
        w.write_record({first: 1, second: 2})
    data = p.read_bytes()
    old = f'name="{second}"'.encode("utf-16-le")
    new = f'name="{patched or first}"'.encode("utf-16-le")
    assert len(old) == len(new) and data.count(old) == 1
    p.write_bytes(data.replace(old, new))
    return p


# -- options ------------------------------------------------------------------ #


def test_options_are_keyword_only_and_frozen():
    with pytest.raises(TypeError):
        ReadOptions(Leniency.NONE)
    with pytest.raises(TypeError):
        WriteOptions("utf-8")
    opts = ReadOptions()
    assert opts.leniency is Leniency.NONE and opts.narrow_encoding == "utf-8"
    with pytest.raises(dataclasses.FrozenInstanceError):
        opts.narrow_encoding = "latin-1"


def test_leniency_is_a_combinable_flag_set():
    assert not Leniency.NONE
    assert Leniency.RENAME_DUPLICATE_FIELDS in (
        Leniency.NONE | Leniency.RENAME_DUPLICATE_FIELDS
    )


def test_fieldinfo_keyword_constructor():
    f = FieldInfo(name="x", type="Int32", size=4)
    assert (f.name, f.type, f.size, f.scale) == ("x", "Int32", 4, 0)
    with pytest.raises(TypeError):
        FieldInfo("x", "Int32")


# -- Table and Reader.read ---------------------------------------------------- #


def test_read_returns_a_table(two_cols):
    with Reader(two_cols) as r:
        assert r.num_records == 4 and r.format == "E1"
        assert [f.name for f in r.fields] == [f.name for f in r.schema] == ["a", "b"]
        t = r.read()
    assert isinstance(t, Table)
    assert t.num_rows == len(t) == 4
    assert t.column_names == ["a", "b"]
    assert t["a"] == t[0] == [1, 2, 3, 4]
    assert t.to_dict() == {"a": [1, 2, 3, 4], "b": ["w", "x", "y", "z"]}
    assert [c.field.type for c in t.columns] == ["Int32", "V_WString"]
    assert t.warnings == []
    with pytest.raises(KeyError):
        t["nope"]


def test_read_selects_orders_and_slices(two_cols):
    with Reader(two_cols) as r:
        t = r.read(["b", "a"], offset=1, limit=2)
        assert t.column_names == ["b", "a"]
        assert t.to_dict() == {"b": ["x", "y"], "a": [2, 3]}
        assert r.read(offset=10).num_rows == 0
        with pytest.raises(ValueError):
            r.read(offset=-1)
        with pytest.raises(ValueError):
            r.read(limit=-1)


def test_table_converts(two_cols):
    pa = pytest.importorskip("pyarrow")
    with Reader(two_cols) as r:
        t = r.read()
    arrow = t.to_pyarrow()
    assert arrow.column("a").to_pylist() == [1, 2, 3, 4]
    assert pa.table(t).equals(arrow)  # Arrow PyCapsule stream
    assert t.to_pandas()["b"].tolist() == ["w", "x", "y", "z"]
    pl = pytest.importorskip("polars")
    assert t.to_polars()["a"].to_list() == [1, 2, 3, 4]


def test_writer_takes_a_table(two_cols, tmp_path):
    out = tmp_path / "copy.yxdb"
    with Reader(two_cols) as r:
        t = r.read()
    with Writer(out, t.fields) as w:
        w.write_columns(t)
    assert openyxdb.read_yxdb(out) == openyxdb.read_yxdb(two_cols)


def test_writer_exit_discards_output_on_error(tmp_path):
    out = tmp_path / "x.yxdb"
    with pytest.raises(RuntimeError, match="boom"):
        with Writer(out, [fi("a")]) as w:
            w.write_record({"a": 1})
            raise RuntimeError("boom")
    assert not out.exists() and w.closed


def test_reader_and_writer_report_closed(two_cols, tmp_path):
    r = Reader(two_cols)
    assert not r.closed
    r.close()
    assert r.closed
    w = Writer(tmp_path / "w.yxdb", [fi("a")])
    assert w.warnings == []
    w.close()
    assert w.closed


# -- duplicate names ---------------------------------------------------------- #


def test_duplicate_names_raise_by_default(tmp_path):
    p = dup_names_file(tmp_path)
    with pytest.raises(SchemaError, match="field 1 is named 'va' in the file.*'va2'"):
        Reader(p)
    with pytest.raises(SchemaError):
        openyxdb.read_yxdb(p)


def test_names_differing_only_in_case_count_as_duplicates(tmp_path):
    p = dup_names_file(tmp_path, first="Name", second="nome", patched="name")
    with pytest.raises(SchemaError, match="renamed"):
        Reader(p)


def test_lenient_rename_keeps_the_rename_and_records_a_warning(tmp_path):
    p = dup_names_file(tmp_path)
    opts = ReadOptions(leniency=Leniency.RENAME_DUPLICATE_FIELDS)
    with pytest.warns(OpenYXDBWarning, match="renamed"):
        r = Reader(p, options=opts)
    with r:
        assert [f.name for f in r.fields] == ["va", "va2"]
        (d,) = r.warnings
        assert isinstance(d, Diagnostic)
        assert d.kind is WarningKind.RENAMED_FIELD
        assert d.code == "renamed_field" and d.field == "va2"
        t = r.read()
    assert t.to_dict() == {"va": [1], "va2": [2]}
    assert t.warnings == [d]


def test_high_level_readers_accept_options(tmp_path):
    p = dup_names_file(tmp_path)
    opts = ReadOptions(leniency=Leniency.RENAME_DUPLICATE_FIELDS)
    with pytest.warns(OpenYXDBWarning):
        assert openyxdb.read_yxdb(p, options=opts) == {"va": [1], "va2": [2]}


def test_clean_file_has_no_warnings(two_cols):
    with Reader(two_cols, options=ReadOptions(leniency=Leniency.RENAME_DUPLICATE_FIELDS)) as r:
        assert r.warnings == []


def test_unknown_field_type_in_a_schema_mapping_raises():
    pytest.importorskip("pyarrow")
    from openyxdb import _yxdb_type_to_arrow

    with pytest.raises(openyxdb.UnsupportedError):
        _yxdb_type_to_arrow(FieldInfo(name="x", type="Mystery"))


def test_from_pyarrow_accepts_any_arrow_stream(two_cols, tmp_path):
    pytest.importorskip("pyarrow")
    out = tmp_path / "from_table.yxdb"
    with Reader(two_cols) as r:
        t = r.read()
    openyxdb.from_pyarrow(t, out)  # an openyxdb Table exports an Arrow stream
    assert openyxdb.read_yxdb(out) == openyxdb.read_yxdb(two_cols)


def test_version_is_a_string():
    assert isinstance(openyxdb.__version__, str)
