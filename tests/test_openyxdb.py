"""Tests for openyxdb Python bindings."""

import os
import sys
import tempfile

import pytest

from openyxdb._openyxdb import FieldInfo, Reader, Writer
from openyxdb import _openyxdb
import openyxdb


@pytest.fixture
def tmp_yxdb(tmp_path):
    """Return a path to a temporary YXDB file."""
    return str(tmp_path / "test.yxdb")


# --------------------------------------------------------------------------- #
# Low-level _openyxdb tests
# --------------------------------------------------------------------------- #

class TestLowLevelRoundtrip:
    def test_write_read_basic(self, tmp_yxdb):
        fi = _openyxdb.FieldInfo()
        fi.name = "x"
        fi.type = "Int32"
        fi.size = 4
        fi.scale = 0

        w = _openyxdb.Writer(tmp_yxdb, [fi])
        w.write_record({"x": 42})
        w.write_record({"x": 99})
        w.close()

        r = _openyxdb.Reader(tmp_yxdb)
        assert r.num_records == 2
        rows = r.read_records()
        assert len(rows) == 2
        assert dict(rows[0]) == {"x": 42}
        assert dict(rows[1]) == {"x": 99}
        r.close()

    def test_context_manager(self, tmp_yxdb):
        fi = _openyxdb.FieldInfo()
        fi.name = "val"
        fi.type = "Double"
        fi.size = 8
        fi.scale = 0

        with _openyxdb.Writer(tmp_yxdb, [fi]) as w:
            w.write_record({"val": 3.14})

        with _openyxdb.Reader(tmp_yxdb) as r:
            assert r.num_records == 1
            rows = r.read_records()
            assert abs(dict(rows[0])["val"] - 3.14) < 1e-10

    def test_null_handling(self, tmp_yxdb):
        fi1 = _openyxdb.FieldInfo()
        fi1.name = "n"
        fi1.type = "Int32"
        fi1.size = 4
        fi1.scale = 0
        fi2 = _openyxdb.FieldInfo()
        fi2.name = "s"
        fi2.type = "V_WString"
        fi2.size = 256
        fi2.scale = 0

        with _openyxdb.Writer(tmp_yxdb, [fi1, fi2]) as w:
            w.write_record({"n": 1, "s": "hello"})
            w.write_record({"n": None, "s": None})

        with _openyxdb.Reader(tmp_yxdb) as r:
            rows = r.read_records()
            assert dict(rows[0]) == {"n": 1, "s": "hello"}
            assert dict(rows[1]) == {"n": None, "s": None}

    def test_all_numeric_types(self, tmp_yxdb):
        types = [
            ("b", "Byte", 1),
            ("i16", "Int16", 2),
            ("i32", "Int32", 4),
            ("i64", "Int64", 8),
            ("f", "Float", 4),
            ("d", "Double", 8),
        ]
        schema = []
        for name, typ, size in types:
            fi = _openyxdb.FieldInfo()
            fi.name = name
            fi.type = typ
            fi.size = size
            fi.scale = 0
            schema.append(fi)

        with _openyxdb.Writer(tmp_yxdb, schema) as w:
            w.write_record({"b": 255, "i16": -1000, "i32": 100000, "i64": 2**40, "f": 1.5, "d": 2.718281828})

        with _openyxdb.Reader(tmp_yxdb) as r:
            rows = r.read_records()
            row = dict(rows[0])
            assert row["b"] == 255
            assert row["i16"] == -1000
            assert row["i32"] == 100000
            assert row["i64"] == 2**40
            assert abs(row["f"] - 1.5) < 0.01
            assert abs(row["d"] - 2.718281828) < 1e-8

    def test_string_types(self, tmp_yxdb):
        schema = []
        for name, typ in [("s", "V_String"), ("ws", "V_WString")]:
            fi = _openyxdb.FieldInfo()
            fi.name = name
            fi.type = typ
            fi.size = 256
            fi.scale = 0
            schema.append(fi)

        with _openyxdb.Writer(tmp_yxdb, schema) as w:
            w.write_record({"s": "hello", "ws": "world"})

        with _openyxdb.Reader(tmp_yxdb) as r:
            rows = r.read_records()
            assert dict(rows[0]) == {"s": "hello", "ws": "world"}

    def test_columnar_read(self, tmp_yxdb):
        fi = _openyxdb.FieldInfo()
        fi.name = "x"
        fi.type = "Int32"
        fi.size = 4
        fi.scale = 0

        with _openyxdb.Writer(tmp_yxdb, [fi]) as w:
            for i in range(100):
                w.write_record({"x": i})

        with _openyxdb.Reader(tmp_yxdb) as r:
            cols = r.read_columns()
            assert len(list(cols["x"])) == 100
            assert list(cols["x"])[0] == 0
            assert list(cols["x"])[99] == 99

    def test_columnar_write(self, tmp_yxdb):
        fi1 = _openyxdb.FieldInfo()
        fi1.name = "a"
        fi1.type = "Int32"
        fi1.size = 4
        fi1.scale = 0
        fi2 = _openyxdb.FieldInfo()
        fi2.name = "b"
        fi2.type = "V_WString"
        fi2.size = 256
        fi2.scale = 0

        with _openyxdb.Writer(tmp_yxdb, [fi1, fi2]) as w:
            w.write_columns({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        with _openyxdb.Reader(tmp_yxdb) as r:
            assert r.num_records == 3
            rows = r.read_records()
            assert dict(rows[1]) == {"a": 2, "b": "y"}

    def test_schema_info(self, tmp_yxdb):
        fi = _openyxdb.FieldInfo()
        fi.name = "test_field"
        fi.type = "Int64"
        fi.size = 8
        fi.scale = 0

        with _openyxdb.Writer(tmp_yxdb, [fi]) as w:
            w.write_record({"test_field": 1})

        with _openyxdb.Reader(tmp_yxdb) as r:
            schema = r.schema
            assert len(schema) == 1
            assert schema[0].name == "test_field"
            assert schema[0].type == "Int64"

    def test_empty_file(self, tmp_yxdb):
        fi = _openyxdb.FieldInfo()
        fi.name = "x"
        fi.type = "Int32"
        fi.size = 4
        fi.scale = 0

        with _openyxdb.Writer(tmp_yxdb, [fi]) as w:
            pass

        with _openyxdb.Reader(tmp_yxdb) as r:
            assert r.num_records == 0

    def test_field_info_repr(self):
        fi = _openyxdb.FieldInfo()
        fi.name = "x"
        fi.type = "Int32"
        fi.size = 4
        fi.scale = 0
        assert "Int32" in repr(fi)
        assert "x" in repr(fi)


# --------------------------------------------------------------------------- #
# High-level openyxdb tests
# --------------------------------------------------------------------------- #

class TestHighLevel:
    def test_read_write_yxdb(self, tmp_yxdb):
        data = {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "score": [95.5, 87.3, 91.0],
        }
        openyxdb.write_yxdb(tmp_yxdb, data)
        result = openyxdb.read_yxdb(tmp_yxdb)
        assert list(result["id"]) == [1, 2, 3]
        assert list(result["name"]) == ["alice", "bob", "carol"]
        assert [round(v, 1) for v in result["score"]] == [95.5, 87.3, 91.0]

    def test_null_roundtrip(self, tmp_yxdb):
        data = {
            "x": [1, None, 3],
            "y": ["a", None, "c"],
        }
        openyxdb.write_yxdb(tmp_yxdb, data)
        result = openyxdb.read_yxdb(tmp_yxdb)
        assert list(result["x"]) == [1, None, 3]
        assert list(result["y"]) == ["a", None, "c"]

    def test_schema_inference_int(self, tmp_yxdb):
        data = {"n": [1, 2, 3]}
        openyxdb.write_yxdb(tmp_yxdb, data)
        with _openyxdb.Reader(tmp_yxdb) as r:
            assert r.schema[0].type == "Int64"

    def test_schema_inference_float(self, tmp_yxdb):
        data = {"v": [1.1, 2.2]}
        openyxdb.write_yxdb(tmp_yxdb, data)
        with _openyxdb.Reader(tmp_yxdb) as r:
            assert r.schema[0].type == "Double"

    def test_schema_inference_string(self, tmp_yxdb):
        data = {"s": ["hello"]}
        openyxdb.write_yxdb(tmp_yxdb, data)
        with _openyxdb.Reader(tmp_yxdb) as r:
            assert r.schema[0].type == "V_WString"

    def test_schema_inference_bool(self, tmp_yxdb):
        data = {"b": [True, False]}
        openyxdb.write_yxdb(tmp_yxdb, data)
        with _openyxdb.Reader(tmp_yxdb) as r:
            assert r.schema[0].type == "Bool"


# --------------------------------------------------------------------------- #
# PyArrow tests
# --------------------------------------------------------------------------- #

class TestPyArrow:
    def test_roundtrip(self, tmp_yxdb):
        import pyarrow as pa

        table = pa.table({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["alice", "bob", None]),
            "score": pa.array([95.5, None, 91.0]),
        })
        openyxdb.from_pyarrow(table, tmp_yxdb)
        result = openyxdb.to_pyarrow(tmp_yxdb)

        assert result.num_rows == 3
        assert result.column("id").to_pylist() == [1, 2, 3]
        assert result.column("name").to_pylist() == ["alice", "bob", None]
        assert result.column("score").to_pylist()[0] == 95.5
        assert result.column("score").to_pylist()[1] is None

    def test_type_mapping(self, tmp_yxdb):
        import pyarrow as pa

        table = pa.table({
            "b": pa.array([True, False]),
            "u8": pa.array([1, 2], type=pa.uint8()),
            "i16": pa.array([1, 2], type=pa.int16()),
            "i32": pa.array([1, 2], type=pa.int32()),
            "i64": pa.array([1, 2], type=pa.int64()),
            "f32": pa.array([1.0, 2.0], type=pa.float32()),
            "f64": pa.array([1.0, 2.0], type=pa.float64()),
        })
        openyxdb.from_pyarrow(table, tmp_yxdb)
        result = openyxdb.to_pyarrow(tmp_yxdb)

        assert pa.types.is_boolean(result.schema.field("b").type)
        assert pa.types.is_uint8(result.schema.field("u8").type)
        assert pa.types.is_int16(result.schema.field("i16").type)
        assert pa.types.is_int32(result.schema.field("i32").type)
        assert pa.types.is_int64(result.schema.field("i64").type)
        assert pa.types.is_float32(result.schema.field("f32").type)
        assert pa.types.is_float64(result.schema.field("f64").type)


# --------------------------------------------------------------------------- #
# Pandas tests
# --------------------------------------------------------------------------- #

class TestPandas:
    def test_roundtrip(self, tmp_yxdb):
        import pandas as pd

        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["alice", "bob", None],
            "score": [95.5, 87.3, None],
        })
        openyxdb.from_pandas(df, tmp_yxdb)
        result = openyxdb.to_pandas(tmp_yxdb)

        assert len(result) == 3
        assert list(result["id"]) == [1, 2, 3]
        assert result["name"].iloc[0] == "alice"


# --------------------------------------------------------------------------- #
# Polars tests
# --------------------------------------------------------------------------- #

class TestPolars:
    def test_roundtrip(self, tmp_yxdb):
        import polars as pl

        df = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["alice", "bob", None],
            "score": [95.5, 87.3, None],
        })
        openyxdb.from_polars(df, tmp_yxdb)
        result = openyxdb.to_polars(tmp_yxdb)

        assert result.shape == (3, 3)
        assert result["id"].to_list() == [1, 2, 3]
        assert result["name"].to_list() == ["alice", "bob", None]
        assert result["score"].to_list()[0] == 95.5
        assert result["score"].to_list()[2] is None

    def test_large_dataframe(self, tmp_yxdb):
        import polars as pl

        n = 10000
        df = pl.DataFrame({
            "idx": list(range(n)),
            "val": [float(i) * 0.1 for i in range(n)],
        })
        openyxdb.from_polars(df, tmp_yxdb)
        result = openyxdb.to_polars(tmp_yxdb)
        assert result.shape == (n, 2)
        assert result["idx"].to_list()[0] == 0
        assert result["idx"].to_list()[-1] == n - 1


# --------------------------------------------------------------------------- #
# Polars IO plugin (scan_yxdb) tests
# --------------------------------------------------------------------------- #

class TestScanYxdb:
    def test_basic_scan(self, tmp_yxdb):
        import polars as pl

        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        openyxdb.from_polars(df, tmp_yxdb)

        result = openyxdb.scan_yxdb(tmp_yxdb).collect()
        assert result.shape == (3, 2)
        assert result["a"].to_list() == [1, 2, 3]
        assert result["b"].to_list() == ["x", "y", "z"]

    def test_projection_pushdown(self, tmp_yxdb):
        import polars as pl

        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        openyxdb.from_polars(df, tmp_yxdb)

        result = openyxdb.scan_yxdb(tmp_yxdb).select("a", "c").collect()
        assert result.columns == ["a", "c"]
        assert result["a"].to_list() == [1, 2]
        assert result["c"].to_list() == [5, 6]

    def test_predicate_pushdown(self, tmp_yxdb):
        import polars as pl

        df = pl.DataFrame({"x": [10, 20, 30, 40], "label": ["a", "b", "c", "d"]})
        openyxdb.from_polars(df, tmp_yxdb)

        result = openyxdb.scan_yxdb(tmp_yxdb).filter(pl.col("x") > 15).collect()
        assert result["x"].to_list() == [20, 30, 40]

    def test_row_limit(self, tmp_yxdb):
        import polars as pl

        df = pl.DataFrame({"n": list(range(100))})
        openyxdb.from_polars(df, tmp_yxdb)

        result = openyxdb.scan_yxdb(tmp_yxdb).head(5).collect()
        assert result.shape == (5, 1)
        assert result["n"].to_list() == [0, 1, 2, 3, 4]

    def test_returns_lazyframe(self, tmp_yxdb):
        import polars as pl

        df = pl.DataFrame({"a": [1]})
        openyxdb.from_polars(df, tmp_yxdb)

        lf = openyxdb.scan_yxdb(tmp_yxdb)
        assert isinstance(lf, pl.LazyFrame)


# --------------------------------------------------------------------------- #
# Polars namespace & monkey-patch tests
# --------------------------------------------------------------------------- #

class TestPolarsPlugin:
    def test_pl_read_yxdb(self, tmp_yxdb):
        import polars as pl

        openyxdb.from_polars(pl.DataFrame({"a": [1, 2, 3]}), tmp_yxdb)
        result = pl.read_yxdb(tmp_yxdb)
        assert isinstance(result, pl.DataFrame)
        assert result["a"].to_list() == [1, 2, 3]

    def test_pl_scan_yxdb(self, tmp_yxdb):
        import polars as pl

        openyxdb.from_polars(pl.DataFrame({"x": [10, 20]}), tmp_yxdb)
        lf = pl.scan_yxdb(tmp_yxdb)
        assert isinstance(lf, pl.LazyFrame)
        assert lf.collect()["x"].to_list() == [10, 20]

    def test_df_yxdb_write(self, tmp_yxdb):
        import polars as pl

        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        df.yxdb.write(tmp_yxdb)

        result = openyxdb.to_polars(tmp_yxdb)
        assert result["a"].to_list() == [1, 2]
        assert result["b"].to_list() == ["x", "y"]

    def test_lf_yxdb_sink(self, tmp_yxdb):
        import polars as pl

        lf = pl.DataFrame({"n": [10, 20, 30]}).lazy().filter(pl.col("n") > 15)
        lf.yxdb.sink(tmp_yxdb)

        result = openyxdb.to_polars(tmp_yxdb)
        assert result["n"].to_list() == [20, 30]
