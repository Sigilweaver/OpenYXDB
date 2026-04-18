"""python -m openyxdb — show package help and version info."""

from __future__ import annotations

import argparse
import sys


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m openyxdb",
        description="OpenYXDB — Read and write Alteryx YXDB files from Python.",
    )
    parser.add_argument(
        "--version", action="store_true", help="Print the package version and exit."
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.version:
        from importlib.metadata import version

        print(f"openyxdb {version('openyxdb')}")
        sys.exit(0)

    # Default: print a comprehensive help message.
    _print_help()


def _print_help() -> None:
    help_text = """\
OpenYXDB — Read and write Alteryx YXDB files from Python.

Quick start:
  import openyxdb

  # Read
  table = openyxdb.to_pyarrow("data.yxdb")
  df    = openyxdb.to_pandas("data.yxdb")
  df    = openyxdb.to_polars("data.yxdb")

  # Write
  openyxdb.from_pyarrow(table, "out.yxdb")
  openyxdb.from_pandas(df,     "out.yxdb")
  openyxdb.from_polars(df,     "out.yxdb")

  # Lazy scan (Polars IO plugin)
  lf = openyxdb.scan_yxdb("data.yxdb")

  # Low-level API
  with openyxdb.Reader("data.yxdb") as r:
      columns = r.read_columns()
  with openyxdb.Writer("out.yxdb", schema) as w:
      w.write_columns(columns)

Polars integration (automatic on import):
  import polars as pl
  import openyxdb

  df = pl.read_yxdb("data.yxdb")       # eager
  lf = pl.scan_yxdb("data.yxdb")       # lazy
  df.yxdb.write("out.yxdb")            # namespace
  lf.yxdb.sink("out.yxdb")             # namespace

Supported field types:
  Bool, Byte, Int16, Int32, Int64, FixedDecimal, Float, Double,
  String, WString, V_String, V_WString, Date, Time, DateTime,
  Blob, SpatialObj

Run 'python -m openyxdb --version' to print the installed version.
Docs: https://github.com/Sigilweaver/OpenYXDB
"""
    print(help_text, end="")


if __name__ == "__main__":
    main()
