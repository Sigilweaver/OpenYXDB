"""The exception hierarchy."""

import pytest

import openyxdb


def test_hierarchy():
    assert issubclass(openyxdb.OpenYXDBError, ValueError)
    assert issubclass(openyxdb.OpenYXDBIOError, openyxdb.OpenYXDBError)
    assert issubclass(openyxdb.OpenYXDBIOError, OSError)
    for cls in (
        openyxdb.FormatError,
        openyxdb.SchemaError,
        openyxdb.ConversionError,
        openyxdb.UnsupportedError,
    ):
        assert issubclass(cls, openyxdb.OpenYXDBError)
        assert not issubclass(cls, OSError)


def test_missing_file_is_io_error(tmp_path):
    with pytest.raises(openyxdb.OpenYXDBIOError, match="OpenForRead"):
        openyxdb.Reader(str(tmp_path / "missing.yxdb"))


def test_not_a_yxdb_file_is_catchable_as_base(tmp_path):
    p = tmp_path / "junk.yxdb"
    p.write_bytes(b"x" * 600)
    with pytest.raises(openyxdb.OpenYXDBError):
        openyxdb.Reader(str(p))
