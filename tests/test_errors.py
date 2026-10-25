"""The exception hierarchy."""

import errno
import pickle

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


def test_missing_file_is_a_file_not_found_error(tmp_path):
    path = tmp_path / "missing.yxdb"
    with pytest.raises(FileNotFoundError) as e:
        openyxdb.Reader(str(path))
    assert isinstance(e.value, openyxdb.OpenYXDBIOError)
    assert isinstance(e.value, openyxdb.OpenYXDBFileNotFoundError)
    assert e.value.errno == errno.ENOENT
    assert e.value.filename == str(path)


def test_unreadable_file_is_a_permission_error(tmp_path):
    import os

    if os.name == "nt" or os.geteuid() == 0:
        pytest.skip("permissions are not enforced here")
    p = tmp_path / "secret.yxdb"
    p.write_bytes(b"x" * 600)
    p.chmod(0)
    with pytest.raises(PermissionError) as e:
        openyxdb.Reader(str(p))
    assert isinstance(e.value, openyxdb.OpenYXDBPermissionError)


def test_not_a_yxdb_file_is_catchable_as_base(tmp_path):
    p = tmp_path / "junk.yxdb"
    p.write_bytes(b"x" * 600)
    with pytest.raises(openyxdb.OpenYXDBError):
        openyxdb.Reader(str(p))


def test_errors_pickle():
    e = pickle.loads(pickle.dumps(openyxdb.ConversionError("boom")))
    assert isinstance(e, openyxdb.ConversionError) and str(e) == "boom"
