"""Check a parser change: parse a fixed sample of filings and diff two runs.

The sample is a few filings per company and form from the database: each
company's first, middle and latest 10-K or 20-F, two 10-Qs, and its latest
amendment. edgartools caches the downloads, so a second run is local.

    python -m filings.scripts.parser_parity snapshot backups/parser/before
    # change the parser
    python -m filings.scripts.parser_parity snapshot backups/parser/after \\
        --sample backups/parser/before/sample.txt
    python -m filings.scripts.parser_parity compare backups/parser/before backups/parser/after

Each filing is written as JSON lines: its fiscal focus, then every fact,
sorted. Two runs match when every file is identical.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

SAMPLE_SQL = """
WITH ranked AS (
    SELECT number, company_id, form_type, fiscal_period_end,
           row_number() OVER w AS first_n,
           row_number() OVER (PARTITION BY company_id, form_type
                              ORDER BY fiscal_period_end DESC) AS last_n,
           count(*) OVER (PARTITION BY company_id, form_type) AS n
    FROM filings
    WINDOW w AS (PARTITION BY company_id, form_type ORDER BY fiscal_period_end)
)
SELECT number FROM ranked
WHERE (form_type IN ('10-K', '20-F') AND (first_n = 1 OR last_n = 1 OR first_n = n / 2))
   OR (form_type = '10-Q' AND (last_n = 1 OR first_n = n / 2 OR first_n = 2))
   OR (form_type IN ('10-K/A', '10-Q/A', '20-F/A') AND last_n = 1)
ORDER BY company_id, fiscal_period_end, number
"""


def _sample_numbers() -> list[str]:
    import asyncpg

    from filings.refresh.io.db import database_url_from_env, to_dsn

    async def fetch() -> list[str]:
        conn = await asyncpg.connect(to_dsn(database_url_from_env()))
        try:
            return [r["number"] for r in await conn.fetch(SAMPLE_SQL)]
        finally:
            await conn.close()

    return asyncio.run(fetch())


def _fact_row(fact: Any) -> dict:
    data: dict = fact.model_dump(mode="json")
    data["dimensions"] = sorted(
        (d["axis"], d["member"], d["member_label"], d["is_default"])
        for d in data.get("dimensions") or []
    )
    return data


def snapshot(out: Path, sample: Optional[Path]) -> None:
    """Parse every filing of the sample into ``out``."""
    from edgar import get_by_accession_number, set_identity

    from filings.parsers.sec_xbrl import SECXBRLParser

    identity = os.getenv("EDGAR_IDENTITY")
    if not identity:
        sys.exit("EDGAR_IDENTITY is not set")
    set_identity(identity)
    logging.disable(logging.WARNING)

    numbers = sample.read_text().split() if sample else _sample_numbers()
    out.mkdir(parents=True, exist_ok=True)
    (out / "sample.txt").write_text("\n".join(numbers) + "\n")
    parser = SECXBRLParser()
    started = time.time()
    for number in numbers:
        parsed = parser.parse_filing(get_by_accession_number(number))
        lines = sorted(
            json.dumps(_fact_row(f), sort_keys=True, default=str) for f in parsed.facts
        )
        header = json.dumps({"fiscal_focus": list(parsed.fiscal_focus)})
        (out / f"{number}.jsonl").write_text("\n".join([header, *lines]) + "\n")
        print(f"{number}: {len(lines)} facts", flush=True)
    print(f"{len(numbers)} filings in {time.time() - started:.0f}s")


def compare(base: Path, new: Path, examples: int = 3) -> bool:
    """Print every filing whose facts differ; True when none do."""
    files = sorted(base.glob("*.jsonl"))
    changed = 0
    for path in files:
        other = new / path.name
        if not other.exists():
            print(f"{path.stem}: missing in {new}")
            changed += 1
            continue
        a, b = path.read_text().splitlines(), other.read_text().splitlines()
        if a == b:
            continue
        changed += 1
        only_a, only_b = sorted(set(a) - set(b)), sorted(set(b) - set(a))
        print(
            f"{path.stem}: {len(only_a)} lines only in base, {len(only_b)} only in new"
        )
        for line in only_a[:examples]:
            print("   - " + line[:300])
        for line in only_b[:examples]:
            print("   + " + line[:300])
    print(f"{len(files) - changed} of {len(files)} filings identical")
    return changed == 0


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    snap = sub.add_parser("snapshot", help="Parse the sample into a directory")
    snap.add_argument("out", type=Path)
    snap.add_argument("--sample", type=Path, help="Reuse another run's sample.txt")
    cmp = sub.add_parser("compare", help="Diff two snapshots")
    cmp.add_argument("base", type=Path)
    cmp.add_argument("new", type=Path)
    args = parser.parse_args()
    if args.command == "snapshot":
        snapshot(args.out, args.sample)
    else:
        sys.exit(0 if compare(args.base, args.new) else 1)


if __name__ == "__main__":
    main()
