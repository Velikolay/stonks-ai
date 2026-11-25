"""Run the refresh stages for a set of companies.

Per company: load the tables the selected stages read, run the stages in
order in memory (each one's output feeds the next), then replace the
company's rows in every written table inside one transaction. A stage run on
its own reads its upstream tables from the database instead, which is how a
single stage is rerun or debugged. Companies never read each other's rows, so
several run at once.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Sequence

import asyncpg
import polars as pl

from .inputs import CompanyData, Trace
from .io import db
from .io import schemas as s
from .io.schemas import OVERRIDE_TABLES, TABLES, Table
from .stages import (
    concept_normalization,
    dimension_breakdowns,
    dimension_normalization,
    facts_normalized,
    facts_overridden,
    hierarchy_normalization,
    yearly,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Stage:
    """One step of the refresh: reads company tables, returns one table."""

    name: str
    output: Table
    reads: tuple[str, ...]
    run: Callable[[CompanyData], pl.DataFrame]


#: Every stage, in the order they must run.
STAGES: tuple[Stage, ...] = (
    Stage(
        "facts_overridden",
        s.FINANCIAL_FACTS_OVERRIDDEN,
        ("financial_facts", "filings"),
        facts_overridden.run,
    ),
    Stage(
        "concept_normalization",
        s.CONCEPT_NORMALIZATION,
        ("financial_facts", "financial_facts_overridden"),
        concept_normalization.run,
    ),
    Stage(
        "hierarchy_normalization",
        s.HIERARCHY_NORMALIZATION,
        ("concept_normalization",),
        hierarchy_normalization.run,
    ),
    Stage(
        "dimension_normalization",
        s.DIMENSION_NORMALIZATION,
        ("financial_facts", "financial_facts_overridden", "concept_normalization"),
        dimension_normalization.run,
    ),
    Stage(
        "facts_normalized",
        s.FINANCIAL_FACTS_NORMALIZED,
        (
            "financial_facts",
            "filings",
            "financial_facts_overridden",
            "concept_normalization",
            "hierarchy_normalization",
            "dimension_normalization",
        ),
        facts_normalized.run,
    ),
    Stage(
        "dimension_breakdowns",
        s.DIMENSION_BREAKDOWNS,
        ("financial_facts_normalized", "filings", "financial_fact_dimensions"),
        dimension_breakdowns.run,
    ),
    Stage(
        "yearly_financials",
        s.YEARLY_FINANCIALS,
        ("financial_facts_normalized", "filings"),
        yearly.run,
    ),
)

STAGE_NAMES = tuple(stage.name for stage in STAGES)


@dataclass(frozen=True)
class StageResult:
    company_id: int
    stage: str
    rows: int
    seconds: float


def select_stages(names: Optional[Sequence[str]]) -> list[Stage]:
    """The named stages in pipeline order, or all of them."""
    if not names:
        return list(STAGES)
    unknown = set(names) - set(STAGE_NAMES)
    if unknown:
        raise ValueError(f"Unknown stages {sorted(unknown)}; choose from {STAGE_NAMES}")
    return [stage for stage in STAGES if stage.name in names]


async def load_overrides(conn: asyncpg.Connection) -> dict[str, pl.DataFrame]:
    """The override tables, whole: every company reads the global rows."""
    return {table.name: await db.load(conn, table) for table in OVERRIDE_TABLES}


def run_stages(data: CompanyData, stages: Sequence[Stage]) -> list[StageResult]:
    """Run ``stages`` on loaded data, storing each output back into ``data``."""
    results = []
    for stage in stages:
        data.stage = stage.name
        started = time.perf_counter()
        output = stage.run(data).select(stage.output.columns)
        data.put(stage.output, output)
        results.append(
            StageResult(
                data.company_id,
                stage.name,
                output.height,
                time.perf_counter() - started,
            )
        )
    data.stage = None
    return results


async def refresh_company(
    pool: asyncpg.Pool,
    company_id: int,
    stages: Sequence[Stage],
    overrides: dict[str, pl.DataFrame],
    dump_dir: Optional[Path] = None,
) -> list[StageResult]:
    """Load, compute and write one company."""
    data = CompanyData(company_id, dict(overrides), Trace(enabled=dump_dir is not None))
    produced = {stage.output.name for stage in stages}
    async with pool.acquire() as conn:
        for stage in stages:
            for name in stage.reads:
                if name not in data and name not in produced:
                    data.put(name, await db.load(conn, TABLES[name], company_id))
            produced.discard(stage.output.name)

        results = await asyncio.to_thread(run_stages, data, stages)

        async with conn.transaction():
            for stage in stages:
                await db.replace_company_rows(
                    conn, stage.output, company_id, data.table(stage.output.name)
                )

    if dump_dir is not None:
        company_dir = dump_dir / str(company_id)
        data.trace.write(company_dir)
        for stage in stages:
            data.table(stage.output.name).write_parquet(
                company_dir / f"{stage.output.name}.parquet"
            )
    for r in results:
        logger.info(
            "company %s %-28s %7d rows %6.2fs", r.company_id, r.stage, r.rows, r.seconds
        )
    return results


async def refresh_companies(
    company_ids: Sequence[int],
    database_url: Optional[str] = None,
    stages: Optional[Sequence[str]] = None,
    jobs: int = 4,
    dump_dir: Optional[Path] = None,
) -> list[StageResult]:
    """Refresh ``company_ids``; each company is written in its own transaction."""
    if not company_ids:
        return []
    selected = select_stages(stages)
    dsn = db.to_dsn(database_url or db.database_url_from_env())
    async with asyncpg.create_pool(dsn, min_size=1, max_size=max(1, jobs)) as pool:
        async with pool.acquire() as conn:
            overrides = await load_overrides(conn)
        gate = asyncio.Semaphore(max(1, jobs))

        async def one(company_id: int) -> list[StageResult]:
            async with gate:
                return await refresh_company(
                    pool, company_id, selected, overrides, dump_dir
                )

        batches = await asyncio.gather(*(one(c) for c in company_ids))
    return [r for batch in batches for r in batch]


async def all_company_ids(database_url: Optional[str] = None) -> list[int]:
    """Every company with an id above 0, the global override owner."""
    dsn = db.to_dsn(database_url or db.database_url_from_env())
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch("SELECT id FROM companies WHERE id > 0 ORDER BY id")
    finally:
        await conn.close()
    return [r["id"] for r in rows]
