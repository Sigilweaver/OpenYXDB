"""Options, warnings and the lenient-mode flag set."""

from __future__ import annotations

import dataclasses
import enum


class Leniency(enum.Flag):
    """Checks that can be relaxed. Strict (``Leniency.NONE``) is the default.

    Each member relaxes one specific check, and every relaxed case is recorded
    as a :class:`Diagnostic` on the reader (and on each :class:`Table` it
    returns). Members may be added over time; combine them with ``|``.
    """

    NONE = 0
    #: Keep the library's renaming of duplicate, empty or over-long E1 field
    #: names (``v``, ``v2``, ...) instead of raising :class:`SchemaError`.
    RENAME_DUPLICATE_FIELDS = enum.auto()


class WarningKind(str, enum.Enum):
    """What a :class:`Diagnostic` is about.

    An open set: new kinds may be added, and a kind this version does not know
    is reported as ``OTHER`` with the specific name in ``Diagnostic.code``.
    """

    RENAMED_FIELD = "renamed_field"
    OTHER = "other"


class OpenYXDBWarning(UserWarning):
    """Category used with :func:`warnings.warn` for every :class:`Diagnostic`."""


@dataclasses.dataclass(frozen=True, kw_only=True)
class Diagnostic:
    """One thing that was relaxed or repaired because a lenient option allowed it."""

    kind: WarningKind
    #: The specific code. Equals ``kind.value`` except for ``OTHER``.
    code: str
    message: str
    #: Field the diagnostic is about, if any.
    field: str | None = None
    #: Zero-based record index, if it is about one record.
    record: int | None = None


@dataclasses.dataclass(frozen=True, kw_only=True)
class ReadOptions:
    """Options for reading. All fields are keyword-only and may grow."""

    #: Checks to relax. Strict by default.
    leniency: Leniency = Leniency.NONE
    #: Codec for String and V_String values of E1 files. Any Python codec name.
    #: The default decodes strict UTF-8; single-byte text needs ``"cp1252"`` or
    #: ``"latin-1"``.
    narrow_encoding: str = "utf-8"
    #: E2 files only: decode field types whose encoding has not been verified.
    allow_unverified_types: bool = False


@dataclasses.dataclass(frozen=True, kw_only=True)
class WriteOptions:
    """Options for writing. All fields are keyword-only and may grow."""

    #: Checks to relax. No writer check can be relaxed yet.
    leniency: Leniency = Leniency.NONE
    #: Codec for String and V_String values. Characters it cannot encode raise
    #: :class:`ConversionError`, and field sizes count encoded bytes.
    narrow_encoding: str = "utf-8"
