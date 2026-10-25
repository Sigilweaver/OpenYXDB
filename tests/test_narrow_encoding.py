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
def test_default_is_latin1_both_ways(p, type_):
    with Writer(str(p), [fi("s", type_, 4)]) as w:
        w.write_record({"s": "café"})  # 4 characters, 4 bytes in latin-1
    with Reader(str(p)) as r:
        assert [dict(x) for x in r.read_records()] == [{"s": "café"}]


def test_values_are_decoded_the_same_way_in_every_row(p):
    # Each of these byte strings is also valid UTF-8 in some rows; the decode
    # must not depend on the row.
    values = ["Ã©", "é", "plain"]
    with Writer(str(p), [fi("s", "V_String", 10)]) as w:
        for v in values:
            w.write_record({"s": v})
    with Reader(str(p)) as r:
        assert [dict(x)["s"] for x in r.read_records()] == values


def test_unencodable_character_raises(p):
    with pytest.raises(openyxdb.ConversionError, match="cannot be encoded as latin-1"):
        with Writer(str(p), [fi("s", "V_String", 10)]) as w:
            w.write_record({"s": "€"})
    assert not p.exists()


def test_cp1252_round_trips_euro(p):
    with Writer(str(p), [fi("s", "V_String", 10)], narrow_encoding="cp1252") as w:
        w.write_record({"s": "€"})
    with Reader(str(p), narrow_encoding="cp1252") as r:
        assert dict(r.read_records()[0])["s"] == "€"
    with Reader(str(p)) as r:
        assert dict(r.read_records()[0])["s"] == "\x80"


def test_utf8_is_strict_on_read(p):
    with Writer(str(p), [fi("s", "V_String", 10)]) as w:
        w.write_record({"s": "café"})
    with pytest.raises(openyxdb.ConversionError, match="not valid utf-8"):
        with Reader(str(p), narrow_encoding="utf-8") as r:
            r.read_records()


def test_utf8_round_trip_counts_bytes_for_size(p):
    with Writer(str(p), [fi("s", "V_String", 3)], narrow_encoding="utf-8") as w:
        w.write_record({"s": "éa"})  # 3 bytes
    with Reader(str(p), narrow_encoding="utf-8") as r:
        assert dict(r.read_records()[0])["s"] == "éa"
    with pytest.raises(openyxdb.ConversionError, match="truncated"):
        with Writer(str(p), [fi("s", "V_String", 2)], narrow_encoding="utf-8") as w:
            w.write_record({"s": "éa"})  # 3 bytes into 2


def test_unknown_encoding_raises(p):
    with pytest.raises(openyxdb.UnsupportedError, match="narrow_encoding"):
        Writer(str(p), [fi("s", "V_String", 3)], narrow_encoding="nope")
    with pytest.raises(openyxdb.UnsupportedError, match="narrow_encoding"):
        Reader(str(p), narrow_encoding="nope")


def test_high_level_functions_take_the_encoding(p):
    openyxdb.write_yxdb(p, {"s": ["€"]}, schema=[fi("s", "V_String", 5)], narrow_encoding="cp1252")
    assert openyxdb.read_yxdb(p, narrow_encoding="cp1252") == {"s": ["€"]}
