"""Recompute the derived financial tables from raw facts and override tables.

``refresh_companies`` runs every stage (or a chosen subset) for a list of
companies and writes each company in its own transaction. See
``filings/refresh/README.md`` for the package layout and debugging tools.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional, Sequence

from .pipeline import STAGE_NAMES, STAGES, StageResult, refresh_companies

__all__ = [
    "STAGES",
    "STAGE_NAMES",
    "StageResult",
    "refresh_companies",
    "refresh_companies_sync",
]


def refresh_companies_sync(
    company_ids: Sequence[int],
    database_url: Optional[str] = None,
    stages: Optional[Sequence[str]] = None,
    jobs: int = 4,
    dump_dir: Optional[Path] = None,
) -> list[StageResult]:
    """:func:`refresh_companies` for synchronous callers such as tests."""
    return asyncio.run(
        refresh_companies(
            company_ids,
            database_url=database_url,
            stages=stages,
            jobs=jobs,
            dump_dir=dump_dir,
        )
    )
