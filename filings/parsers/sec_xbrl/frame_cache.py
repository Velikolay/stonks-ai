"""A disk cache of the statement tables edgartools builds for a filing.

Turning a statement into a table (``Statement.to_dataframe`` and its
dimension metadata) is most of what parsing a filing costs: about two seconds
of CPU per 10-Q, against a fraction of a second for our own rules. The table
depends on the filing, the statement, the edgartools version and
:func:`.statement.statement_frame`, never on the parser's rules, so a parser
change re-ingested with ``--override`` reads the tables back instead of
rebuilding them.

One pickle per accession under ``~/.edgar/_frames`` (beside edgartools' own
HTTP cache). ``FILINGS_FRAME_CACHE=0`` turns the cache off; any other value
is the directory to use. The file name carries the edgartools version and a
hash of the source of the functions that build a table (:data:`BUILDERS`),
so an upgrade or a change to them misses the cache by itself. Each table is
kept serialized, so no caller can change what a later parse reads. The
pickles are this machine's own cache; do not point the cache at files from
elsewhere.
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import inspect
import logging
import os
import pickle
import re
from functools import lru_cache
from importlib import metadata
from pathlib import Path
from typing import Callable, Iterator, Optional

import pandas as pd

logger = logging.getLogger(__name__)

#: The functions whose source decides what a cached table holds.
BUILDERS = (
    "statement_frame",
    "attach_dimension_metadata",
    "cell_text",
    "flag",
)
#: Bump to drop every cached table without a code change.
FRAME_VERSION = 1
_ACCESSION = re.compile(r"^\d{10}-\d{2}-\d{6}$")

_ENV = "FILINGS_FRAME_CACHE"
_current: contextvars.ContextVar[Optional["FilingFrames"]] = contextvars.ContextVar(
    "filing_frames", default=None
)


def cache_directory() -> Optional[Path]:
    """Where tables are kept, or None when the cache is off."""
    setting = os.environ.get(_ENV, "")
    if setting.strip().lower() in ("0", "false", "off", "no"):
        return None
    if setting:
        return Path(setting).expanduser()
    return Path(os.environ.get("EDGAR_LOCAL_DATA_DIR", "~/.edgar")).expanduser() / (
        "_frames"
    )


def _edgartools_version() -> str:
    try:
        return metadata.version("edgartools")
    except metadata.PackageNotFoundError:
        return "unknown"


@lru_cache(maxsize=1)
def builders_hash() -> str:
    """A short hash of the source of :data:`BUILDERS`."""
    from . import statement, values

    digest = hashlib.sha256()
    for name in BUILDERS:
        function = getattr(statement, name, None) or getattr(values, name)
        digest.update(inspect.getsource(function).encode())
    return digest.hexdigest()[:12]


class FilingFrames:
    """The cached tables of one filing, read once and written back if new."""

    def __init__(self, path: Path):
        self.path = path
        self.frames: dict[tuple, bytes] = {}
        self.changed = False
        if path.exists():
            try:
                with path.open("rb") as handle:
                    self.frames = pickle.load(handle)
            except Exception:
                logger.warning("Ignoring unreadable frame cache %s", path)
                self.frames = {}

    def get(self, key: tuple, build: Callable[[], pd.DataFrame]) -> pd.DataFrame:
        if key in self.frames:
            return pickle.loads(self.frames[key])
        frame = build()
        self.frames[key] = pickle.dumps(frame, protocol=pickle.HIGHEST_PROTOCOL)
        self.changed = True
        return pickle.loads(self.frames[key])

    def save(self) -> None:
        if not self.changed:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(f".{os.getpid()}.tmp")
        with tmp.open("wb") as handle:
            pickle.dump(self.frames, handle, protocol=pickle.HIGHEST_PROTOCOL)
        tmp.replace(self.path)


@contextlib.contextmanager
def filing_frames(accession: str) -> Iterator[None]:
    """Cache the tables built while parsing ``accession``, if the cache is on."""
    directory = cache_directory()
    if directory is None or not isinstance(accession, str):
        yield
        return
    if not _ACCESSION.match(accession):
        yield
        return
    name = (
        f"{accession}-v{FRAME_VERSION}-{builders_hash()}"
        f"-edgartools{_edgartools_version()}.pkl"
    )
    frames = FilingFrames(directory / name)
    token = _current.set(frames)
    try:
        yield
    finally:
        _current.reset(token)
        try:
            frames.save()
        except Exception:
            logger.warning("Could not write frame cache %s", frames.path)


def cached_frame(statement: object, variant: str, build: Callable[[], pd.DataFrame]):
    """``build()`` for ``statement``, from the current filing's cache when on.

    ``variant`` names how the table was built (with or without dimensions);
    the statement's role and its dimension setting identify the statement.
    """
    frames = _current.get()
    role = getattr(statement, "role_or_type", None)
    if frames is None or not isinstance(role, str) or not role:
        return build()
    key = (
        role,
        str(getattr(statement, "_include_dimensions", "")),
        variant,
    )
    return frames.get(key, build)
