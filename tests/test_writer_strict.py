"""The writer raises instead of truncating, nulling or renaming silently."""

from __future__ import annotations

import datetime
import decimal
import os

import pytest

import openyxdb
from openyxdb._openyxdb import FieldInfo, Reader, Writer


def fi(name: str, type_: str, size: int = 0, scale: int = 0) -> FieldInfo:
    f = FieldInfo()
    f.name = name
    f.type = type_
    f.size = size
    f.scale = scale
    return f


def write_rows(path, schema, rows):
    with Writer(str(path), schema) as w:
        for r in rows:
            w.write_record(r)


def read_rows(path):
    with Reader(str(path)) as r:
        return [dict(x) for x in r.read_records()]


@pytest.fixture
def p(tmp_path):
    return tmp_path / "out.yxdb"


# -- numeric narrowing ------------------------------------------------------- #


@pytest.mark.parametrize(
    "type_,size,value",
    [
        ("Byte", 1, 300),
        ("Byte", 1, -1),
        ("Int16", 2, 70000),
        ("Int16", 2, -40000),
        ("Float", 4, 1e300),
        ("Float", 4, -1e300),
    ],
)
def test_out_of_range_number_raises(p, type_, size, value):
    with pytest.raises(openyxdb.ConversionError, match="(?i)fit|range|conver|overflow"):
        write_rows(p, [fi("v", type_, size)], [{"v": value}])
    assert not p.exists()


def test_in_range_numbers_still_write(p):
    schema = [fi("b", "Byte", 1), fi("s", "Int16", 2), fi("f", "Float", 4)]
    write_rows(p, schema, [{"b": 255, "s": -32768, "f": 3.5}])
    assert read_rows(p) == [{"b": 255, "s": -32768, "f": 3.5}]


def test_fixed_decimal_too_wide_raises(p):
    with pytest.raises(openyxdb.ConversionError, match="(?i)fit|long"):
        write_rows(p, [fi("d", "FixedDecimal", 6, 2)], [{"d": 123456.78}])


@pytest.mark.parametrize("value", [float("nan"), float("inf")])
def test_fixed_decimal_non_finite_raises(p, value):
    with pytest.raises(openyxdb.ConversionError, match="NaN or infinity"):
        write_rows(p, [fi("d", "FixedDecimal", 12, 2)], [{"d": value}])


def lzf_decompress(data: bytes) -> bytes:
    out = bytearray()
    i = 0
    while i < len(data):
        ctrl = data[i]
        i += 1
        if ctrl < 32:
            n = ctrl + 1
            out += data[i : i + n]
            i += n
        else:
            length = ctrl >> 5
            if length == 7:
                length += data[i]
                i += 1
            ref = len(out) - ((ctrl & 0x1F) << 8) - data[i] - 1
            i += 1
            for k in range(length + 2):
                out.append(out[ref + k])
    return bytes(out)


def record_stream(path) -> bytes:
    """Decompressed record bytes of a one-block file."""
    import struct

    data = path.read_bytes()
    start = 512 + struct.unpack_from("<I", data, 0x50)[0] * 2
    size = struct.unpack_from("<I", data, start)[0]
    body = data[start + 4 : start + 4 + (size & 0x7FFFFFFF)]
    return body if size & 0x80000000 else lzf_decompress(body)


def test_fixed_decimal_keeps_exact_decimal_text(p):
    value = decimal.Decimal("12345678901234567.89")
    write_rows(p, [fi("d", "FixedDecimal", 24, 2)], [{"d": value}])
    assert b"12345678901234567.89" in record_stream(p)


def test_big_int_into_double_raises(p):
    with pytest.raises(openyxdb.ConversionError, match="exactly"):
        write_rows(p, [fi("d", "Double", 8)], [{"d": 2**53 + 1}])
    write_rows(p, [fi("d", "Double", 8)], [{"d": 2**53}])
    assert read_rows(p) == [{"d": float(2**53)}]


# -- strings ----------------------------------------------------------------- #


@pytest.mark.parametrize("type_", ["String", "WString", "V_String", "V_WString"])
def test_string_truncation_raises(p, type_):
    with pytest.raises(openyxdb.ConversionError, match="truncated"):
        write_rows(p, [fi("s", type_, 3)], [{"s": "abcd"}])


def test_string_exact_fit_writes(p):
    write_rows(p, [fi("s", "V_WString", 4)], [{"s": "abcd"}])
    assert read_rows(p) == [{"s": "abcd"}]


def test_from_pyarrow_long_string_is_not_truncated(p):
    pa = pytest.importorskip("pyarrow")
    text = "x" * 300000
    openyxdb.from_pyarrow(pa.table({"s": [text, "short"]}), p)
    assert openyxdb.to_pyarrow(p).column("s").to_pylist() == [text, "short"]


def test_write_yxdb_infers_size_from_data(p):
    text = "y" * 300000
    openyxdb.write_yxdb(p, {"s": [text]})
    assert openyxdb.read_yxdb(p) == {"s": [text]}


def test_supplementary_characters_round_trip(p):
    emoji = "\U0001F600"
    write_rows(p, [fi("a" + emoji, "V_WString", 10)], [{"a" + emoji: "v" + emoji}])
    assert read_rows(p) == [{"a" + emoji: "v" + emoji}]
    with Reader(str(p)) as r:
        assert [f.name for f in r.schema] == ["a" + emoji]


# -- schema XML -------------------------------------------------------------- #


@pytest.mark.parametrize("name", ["a&amp;b", 'say "hi"', "a<b>c", "it's", "a&b"])
def test_special_characters_in_names_round_trip(p, name):
    write_rows(p, [fi(name, "Int32", 4)], [{name: 7}])
    assert read_rows(p) == [{name: 7}]


def test_control_character_in_name_raises(p):
    with pytest.raises(openyxdb.SchemaError, match="control character"):
        write_rows(p, [fi("a\nb", "Int32", 4)], [])
    assert not p.exists()


@pytest.mark.parametrize("type_", ["Integer", "int32", "Bxxx2", "", "Int32 "])
def test_unknown_type_name_raises(p, type_):
    with pytest.raises(openyxdb.SchemaError, match="Unknown field type"):
        write_rows(p, [fi("a", type_, 4)], [])
    assert not p.exists()


# -- names and keys ---------------------------------------------------------- #


def test_names_differing_only_in_case_raise(p):
    with pytest.raises(openyxdb.SchemaError, match="not usable as written"):
        write_rows(p, [fi("Name", "Int32", 4), fi("name", "Int32", 4)], [])
    assert not p.exists()


def test_too_long_name_raises(p):
    with pytest.raises(openyxdb.SchemaError, match="not usable as written"):
        write_rows(p, [fi("n" * 300, "Int32", 4)], [])


def test_empty_name_raises(p):
    with pytest.raises(openyxdb.SchemaError, match="not usable as written"):
        write_rows(p, [fi("", "Int32", 4)], [])


def test_unknown_key_in_record_raises(p):
    with Writer(str(p), [fi("amount", "Int32", 4)]) as w:
        with pytest.raises(openyxdb.SchemaError, match="'amuont' is not a field"):
            w.write_record({"amuont": 1})
        w.abort()


def test_unknown_key_in_columns_raises(p):
    w = Writer(str(p), [fi("amount", "Int32", 4)])
    try:
        with pytest.raises(openyxdb.SchemaError, match="'amuont' is not a field"):
            w.write_columns({"amuont": [1, 2]})
    finally:
        w.abort()


def test_absent_schema_column_is_still_null(p):
    schema = [fi("a", "Int32", 4), fi("b", "Int32", 4)]
    with Writer(str(p), schema) as w:
        w.write_columns({"a": [1, 2]})
    assert read_rows(p) == [{"a": 1, "b": None}, {"a": 2, "b": None}]


# -- temporal ---------------------------------------------------------------- #


def table_with(pa, type_, value):
    return pa.table({"t": pa.array([value], type=type_)})


def test_timezone_aware_datetime_raises(p):
    pa = pytest.importorskip("pyarrow")
    ts = datetime.datetime(2023, 3, 15, 3, 4, 5, tzinfo=datetime.timezone.utc)
    with pytest.raises(openyxdb.ConversionError, match="timezone-aware"):
        openyxdb.from_pyarrow(table_with(pa, pa.timestamp("us", tz="UTC"), ts), p)
    assert not p.exists()


def test_subsecond_datetime_raises(p):
    pa = pytest.importorskip("pyarrow")
    ts = datetime.datetime(2023, 3, 15, 1, 2, 3, 456789)
    with pytest.raises(openyxdb.ConversionError, match="sub-second"):
        openyxdb.from_pyarrow(table_with(pa, pa.timestamp("us"), ts), p)


def test_subsecond_time_raises(p):
    pa = pytest.importorskip("pyarrow")
    t = datetime.time(1, 2, 3, 456789)
    with pytest.raises(openyxdb.ConversionError, match="sub-second"):
        openyxdb.from_pyarrow(table_with(pa, pa.time64("us"), t), p)


def test_date_before_1400_raises(p):
    pa = pytest.importorskip("pyarrow")
    d = datetime.date(1300, 1, 1)
    with pytest.raises(openyxdb.ConversionError, match="not a valid"):
        openyxdb.from_pyarrow(table_with(pa, pa.date32(), d), p)


def test_whole_second_values_round_trip(p):
    pa = pytest.importorskip("pyarrow")
    ts = datetime.datetime(2023, 3, 15, 1, 2, 3)
    openyxdb.from_pyarrow(table_with(pa, pa.timestamp("us"), ts), p)
    assert openyxdb.to_pyarrow(p).column("t").to_pylist() == [ts]


# -- finalization ------------------------------------------------------------ #


def test_exception_in_with_block_discards_output(tmp_path):
    out = tmp_path / "x.yxdb"
    with pytest.raises(RuntimeError, match="boom"):
        with Writer(str(out), [fi("a", "Int32", 4)]) as w:
            w.write_record({"a": 1})
            raise RuntimeError("boom")
    assert os.listdir(tmp_path) == []


def test_exception_keeps_existing_file(tmp_path):
    out = tmp_path / "x.yxdb"
    write_rows(out, [fi("a", "Int32", 4)], [{"a": 1}])
    with pytest.raises(RuntimeError, match="boom"):
        with Writer(str(out), [fi("a", "Int32", 4)]) as w:
            w.write_record({"a": 2})
            raise RuntimeError("boom")
    assert read_rows(out) == [{"a": 1}]
    assert os.listdir(tmp_path) == ["x.yxdb"]


def test_unfinished_writer_leaves_no_valid_file(tmp_path):
    out = tmp_path / "x.yxdb"
    w = Writer(str(out), [fi("a", "Int32", 4)])
    w.write_record({"a": 1})
    assert not out.exists()
    w.close()
    assert out.exists()
    assert os.listdir(tmp_path) == ["x.yxdb"]
