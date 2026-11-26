"""Run the diagnostics for some companies only, every section without LIMIT.

``make diagnose`` prints the first page of every query over all companies,
which on a populated database belongs to other filers. This runs each
section of ``sql/diagnostics/*.sql`` filtered to ``--companies`` and prints
only sections that return rows::

    python -m filings.refresh.parity.diagnose --companies 4
    python -m filings.refresh.parity.diagnose --companies 2,4 --files 14,16
"""

from __future__ import annotations

import argparse
import asyncio
from typing import Optional, Sequence

import asyncpg
import polars as pl

from ..io import db
from .snapshot import diagnostic_queries


async def run(
    database_url: str,
    companies: list[int],
    files: Optional[list[str]] = None,
    limit: int = 200,
    search_path: Optional[str] = None,
) -> int:
    """Print every non-empty section; return the number of sections printed."""
    settings = {"search_path": search_path} if search_path else None
    conn = await asyncpg.connect(db.to_dsn(database_url), server_settings=settings)
    printed = 0
    try:
        for file, title, sql in diagnostic_queries():
            if files and not any(file.startswith(prefix) for prefix in files):
                continue
            statement = await conn.prepare(f"SELECT * FROM ({sql}) q LIMIT 0")
            names = [a.name for a in statement.get_attributes()]
            if "company_id" in names:
                rows = await conn.fetch(
                    f"SELECT * FROM ({sql}) q WHERE company_id = ANY($1::int[])"
                    f" LIMIT {limit + 1}",
                    companies,
                )
            else:
                rows = await conn.fetch(f"SELECT * FROM ({sql}) q LIMIT {limit + 1}")
            if not rows:
                continue
            printed += 1
            frame = pl.DataFrame(
                [[_plain(v) for v in r.values()] for r in rows[:limit]],
                schema=names,
                orient="row",
            )
            more = " (more rows not shown)" if len(rows) > limit else ""
            print(f"\n=== {file} | {title} | {min(len(rows), limit)} rows{more}")
            with pl.Config(
                tbl_rows=-1,
                tbl_cols=-1,
                tbl_width_chars=400,
                fmt_str_lengths=80,
                tbl_hide_dataframe_shape=True,
                tbl_hide_column_data_types=True,
            ):
                print(frame)
    finally:
        await conn.close()
    return printed


def _plain(value):
    """Decimals, dates and arrays as text so one frame holds any column."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value if not isinstance(value, bool) else str(value)
    if isinstance(value, (list, tuple)):
        return "; ".join(str(v) for v in value)
    return str(value)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--companies", required=True, help="Comma-separated ids")
    parser.add_argument(
        "--files", help="Comma-separated file prefixes, e.g. 04,14 (default all)"
    )
    parser.add_argument("--limit", type=int, default=200, help="Rows per section")
    parser.add_argument(
        "--search-path", help="Read derived tables from another schema first"
    )
    args = parser.parse_args(argv)
    printed = asyncio.run(
        run(
            db.database_url_from_env(),
            [int(c) for c in args.companies.split(",")],
            args.files.split(",") if args.files else None,
            args.limit,
            args.search_path,
        )
    )
    print(f"\n{printed} sections with rows")


if __name__ == "__main__":
    main()
