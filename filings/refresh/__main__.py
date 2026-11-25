"""Command line: ``python -m filings.refresh [--companies 1,2] [--stages ...]``."""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections import defaultdict
from pathlib import Path
from typing import Optional, Sequence

from .pipeline import STAGE_NAMES, StageResult, all_company_ids, refresh_companies


def _summary(results: Sequence[StageResult]) -> None:
    seconds: dict[str, float] = defaultdict(float)
    rows: dict[str, int] = defaultdict(int)
    for r in results:
        seconds[r.stage] += r.seconds
        rows[r.stage] += r.rows
    for stage in STAGE_NAMES:
        if stage in seconds:
            print(f"{stage:<28} {rows[stage]:>9} rows {seconds[stage]:>8.2f}s compute")


async def _main(args: argparse.Namespace) -> None:
    ids = (
        [int(i) for i in args.companies.split(",")]
        if args.companies
        else await all_company_ids()
    )
    results = await refresh_companies(
        ids,
        stages=args.stages.split(",") if args.stages else None,
        jobs=args.jobs,
        dump_dir=args.dump_dir,
    )
    _summary(results)


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Recompute the derived financial tables."
    )
    parser.add_argument("--companies", help="Comma-separated company ids (default all)")
    parser.add_argument(
        "--stages", help=f"Comma-separated subset of: {','.join(STAGE_NAMES)}"
    )
    parser.add_argument(
        "--jobs", type=int, default=4, help="Companies refreshed at once"
    )
    parser.add_argument(
        "--dump-dir",
        type=Path,
        help="Write each stage's output and traced intermediates as Parquet here",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING, format="%(message)s"
    )
    asyncio.run(_main(args))


if __name__ == "__main__":
    main()
