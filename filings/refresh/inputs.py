"""The frames one company's refresh reads, and the outputs it has produced.

Stages are pure functions of a :class:`CompanyData`. The pipeline fills it
from the database, or a test fills it from hand-written frames, and a stage
asks for tables by name without knowing which.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import polars as pl

from .io.schemas import TABLES, Table


@dataclass
class Trace:
    """Named intermediate frames a stage chose to expose, for debugging.

    Disabled by default; ``--dump-dir`` turns it on and writes each frame to
    ``<dump-dir>/<company>/<stage>.<name>.parquet``.
    """

    enabled: bool = False
    frames: dict[str, pl.DataFrame] = field(default_factory=dict)

    def keep(self, name: str, frame: pl.DataFrame) -> pl.DataFrame:
        if self.enabled:
            self.frames[name] = frame
        return frame

    def write(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        for name, frame in self.frames.items():
            frame.write_parquet(directory / f"{name}.parquet")


@dataclass
class CompanyData:
    """Everything a stage can read for one company.

    ``tables`` holds the company's rows of raw and derived tables, and the
    full override tables, which are shared by every company.
    """

    company_id: int
    tables: dict[str, pl.DataFrame] = field(default_factory=dict)
    trace: Trace = field(default_factory=Trace)
    stage: Optional[str] = None

    def __contains__(self, name: str) -> bool:
        return name in self.tables

    def table(self, name: str) -> pl.DataFrame:
        """A table's frame; an absent table reads as empty, like an empty table."""
        frame = self.tables.get(name)
        if frame is None:
            return TABLES[name].empty()
        return frame

    def put(self, table: Table | str, frame: pl.DataFrame) -> None:
        name = table if isinstance(table, str) else table.name
        self.tables[name] = frame

    def keep(self, name: str, frame: pl.DataFrame) -> pl.DataFrame:
        """Expose an intermediate frame under ``<stage>.<name>`` when tracing."""
        prefix = f"{self.stage}." if self.stage else ""
        return self.trace.keep(prefix + name, frame)
