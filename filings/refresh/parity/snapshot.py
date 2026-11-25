"""Snapshot the derived tables and the diagnostics scorecard to Parquet.

A snapshot taken before and after a change is what ``compare`` diffs::

    python -m filings.refresh.parity.snapshot --out backups/parity/sql
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
from pathlib import Path
from typing import Optional, Sequence

import asyncpg
import polars as pl

from ..io import db
from ..io.schemas import DERIVED_TABLES, TABLES

logger = logging.getLogger(__name__)

DIAGNOSTICS_DIR = Path(__file__).resolve().parents[3] / "sql" / "diagnostics"
SCORECARD_FILE = "diagnostics.parquet"
_SECTION = re.compile(r"^\\echo\s+'(?P<title>[^']*)'\s*$", re.MULTILINE)
_TRAILING_LIMIT = re.compile(r"\s+LIMIT\s+\d+\s*;?\s*$", re.IGNORECASE)


def diagnostic_queries(directory: Path = DIAGNOSTICS_DIR) -> list[tuple[str, str, str]]:
    """Every ``(file, section title, sql)`` in the diagnostics, without LIMIT."""
    queries = []
    for path in sorted(directory.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        marks = list(_SECTION.finditer(text))
        for i, mark in enumerate(marks):
            end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
            body = text[mark.end() : end].strip()
            body = _TRAILING_LIMIT.sub("", body).rstrip().rstrip(";")
            if body:
                queries.append((path.name, mark.group("title").strip("- "), body))
    return queries


async def scorecard(conn: asyncpg.Connection) -> pl.DataFrame:
    """Row count of every diagnostic query, per company where it has one."""
    rows: list[tuple[str, str, Optional[int], int]] = []
    for file, title, sql in diagnostic_queries():
        statement = await conn.prepare(f"SELECT * FROM ({sql}) q LIMIT 0")
        has_company = any(a.name == "company_id" for a in statement.get_attributes())
        if has_company:
            counted = await conn.fetch(
                f"SELECT company_id, COUNT(*) FROM ({sql}) q GROUP BY 1"
            )
            rows.extend((file, title, r[0], r[1]) for r in counted)
        else:
            total = await conn.fetchval(f"SELECT COUNT(*) FROM ({sql}) q")
            rows.append((file, title, None, total))
    return pl.DataFrame(
        rows,
        schema={
            "file": pl.String,
            "query": pl.String,
            "company_id": pl.Int32,
            "rows": pl.Int64,
        },
        orient="row",
    )


async def take_snapshot(
    database_url: str,
    out: Path,
    tables: Optional[Sequence[str]] = None,
    diagnostics: bool = True,
) -> None:
    """Write each derived table, and optionally the scorecard, under ``out``."""
    out.mkdir(parents=True, exist_ok=True)
    conn = await asyncpg.connect(db.to_dsn(database_url))
    try:
        for table in DERIVED_TABLES:
            if tables and table.name not in tables:
                continue
            frame = await db.load(conn, TABLES[table.name])
            frame.write_parquet(out / f"{table.name}.parquet")
            logger.info("%s: %d rows", table.name, frame.height)
        if diagnostics:
            card = await scorecard(conn)
            card.write_parquet(out / SCORECARD_FILE)
            logger.info("diagnostics: %d query/company counts", card.height)
    finally:
        await conn.close()


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--tables", help="Comma-separated table names (default all)")
    parser.add_argument("--no-diagnostics", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(
        take_snapshot(
            db.database_url_from_env(),
            args.out,
            tables=args.tables.split(",") if args.tables else None,
            diagnostics=not args.no_diagnostics,
        )
    )


if __name__ == "__main__":
    main()
