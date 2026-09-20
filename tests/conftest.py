"""Shared corpus discovery for the OpenYXDB test suite.

The real-world YXDB corpus lives outside the repository. Set ``YXDB_CORPUS_DIR``
to point at a directory containing ``e1/`` and ``e2/`` subdirectories; tests that
need the corpus skip when it is absent.
"""

from __future__ import annotations

import os
from pathlib import Path

DEFAULT_CORPUS_DIR = Path("/workspaces/Projects/Data/YXDB")


def corpus_dir(layout: str) -> Path:
    """Return the corpus directory for ``layout`` (``"e1"`` or ``"e2"``)."""
    root = Path(os.environ.get("YXDB_CORPUS_DIR", DEFAULT_CORPUS_DIR))
    return root / layout


def corpus_files(layout: str) -> list[Path]:
    """Return every ``.yxdb`` file for ``layout``, sorted; empty if unavailable."""
    directory = corpus_dir(layout)
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.glob("*.yxdb") if p.is_file())
