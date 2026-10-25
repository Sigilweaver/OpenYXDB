"""String and V_String values use one explicit encoding for the whole file."""

import pytest

import openyxdb
from openyxdb._openyxdb import FieldInfo, Reader, Writer


def fi(name, type_, size):
    f = FieldInfo()
    f.name, f.type, f.size, f.scale = name, type_, size, 0
    return f


@pytest.fixture
def p(tmp_path):
    return tmp_path / "n.yxdb"


@pytest.mark.parametrize("type_", ["String", "V_String"])
def test_default_is_utf8_both_ways(p, type_):
    with Writer(str(p), [fi("s", type_, 5)]) as w:
        w.write_record({"s": "café"})  # 5 bytes in UTF-8
    with Reader(str(p)) as r:
        assert [dict(x) for x in r.read_records()] == [{"s": "café"}]


def test_size_counts_encoded_bytes(p):
    with pytest.raises(openyxdb.ConversionError, match="truncated"):
        with Writer(str(p), [fi("s", "V_String", 4)]) as w:
            w.write_record({"s": "café"})  # 5 bytes into 4


def test_single_byte_text_raises_with_location_and_advice(p):
    with Writer(str(p), [fi("s", "V_String", 10)], narrow_encoding="latin-1") as w:
        w.write_record({"s": "ok"})
        w.write_record({"s": "café"})
    with Reader(str(p)) as r:
        with pytest.raises(openyxdb.ConversionError) as e:
            r.read_records()
    msg = str(e.value)
    assert "field 's'" in msg
    assert "record 1" in msg
    assert "byte offset 3" in msg
    assert 'narrow_encoding="cp1252"' in msg


def test_error_reports_the_record_in_windowed_reads(p):
    with Writer(str(p), [fi("s", "V_String", 10)], narrow_encoding="latin-1") as w:
        for v in ["a", "b", "c", "dé"]:
            w.write_record({"s": v})
    with Reader(str(p)) as r:
        with pytest.raises(openyxdb.ConversionError, match="record 3"):
            r.read_columns_subset(["s"], 2, 2)


def test_latin1_and_cp1252_read_single_byte_text(p):
    with Writer(str(p), [fi("s", "V_String", 10)], narrow_encoding="cp1252") as w:
        w.write_record({"s": "€"})
    with Reader(str(p), narrow_encoding="cp1252") as r:
        assert dict(r.read_records()[0])["s"] == "€"
    with Reader(str(p), narrow_encoding="latin-1") as r:
        assert dict(r.read_records()[0])["s"] == "\x80"


def test_values_are_decoded_the_same_way_in_every_row(p):
    # Valid UTF-8 as latin-1 text must not be reinterpreted row by row.
    values = ["Ã©", "é", "plain"]
    with Writer(str(p), [fi("s", "V_String", 10)], narrow_encoding="latin-1") as w:
        for v in values:
            w.write_record({"s": v})
    with Reader(str(p), narrow_encoding="latin-1") as r:
        assert [dict(x)["s"] for x in r.read_records()] == values


def test_unencodable_character_raises(p):
    with pytest.raises(openyxdb.ConversionError, match="cannot be encoded as latin-1"):
        with Writer(str(p), [fi("s", "V_String", 10)], narrow_encoding="latin-1") as w:
            w.write_record({"s": "€"})
    assert not p.exists()


def test_unknown_encoding_raises(p):
    with pytest.raises(openyxdb.UnsupportedError, match="narrow_encoding"):
        Writer(str(p), [fi("s", "V_String", 3)], narrow_encoding="nope")
    with pytest.raises(openyxdb.UnsupportedError, match="narrow_encoding"):
        Reader(str(p), narrow_encoding="nope")


def test_high_level_functions_take_the_encoding(p):
    euro = "\u20ac"
    openyxdb.write_yxdb(
        p,
        {"s": [euro]},
        schema=[fi("s", "V_String", 5)],
        options=openyxdb.WriteOptions(narrow_encoding="cp1252"),
    )
    opts = openyxdb.ReadOptions(narrow_encoding="cp1252")
    assert openyxdb.read_yxdb(p, options=opts) == {"s": [euro]}
    with pytest.raises(openyxdb.ConversionError):
        openyxdb.read_yxdb(p)


@pytest.mark.parametrize("codec", ["utf-16", "utf-32", "utf-8-sig", "utf-16-le"])
def test_codecs_that_cannot_round_trip_a_byte_string_are_refused(p, codec):
    with pytest.raises(openyxdb.UnsupportedError, match="ASCII"):
        Writer(str(p), [fi("s", "V_String", 3)], narrow_encoding=codec)
    with pytest.raises(openyxdb.UnsupportedError, match="ASCII"):
        Reader(str(p), narrow_encoding=codec)


@pytest.mark.parametrize("codec", ["utf-8", "cp1252", "latin-1", "ascii", "cp437", "gbk", "shift_jis"])
def test_ascii_compatible_codecs_are_accepted(p, codec):
    with Writer(str(p), [fi("s", "V_String", 8)], narrow_encoding=codec) as w:
        w.write_record({"s": "plain"})
    with Reader(str(p), narrow_encoding=codec) as r:
        assert dict(r.read_records()[0])["s"] == "plain"
