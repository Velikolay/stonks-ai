"""Sync the normalization override CSVs in ``migrations/data`` into the database.

The CSVs are the source of truth: each table is replaced with the contents of
its CSV, so removing a row from a CSV removes it from the database. This makes
the edit-override / refresh loop repeatable without a full ``make db-reset``.

Usage:
    python -m filings.scripts.seed_overrides [--tables concept,dimension,facts]
                                             [--dry-run]
"""

import argparse
import asyncio
import csv
import logging
import os
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set

from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, select
from sqlalchemy import text as sa_text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from filings.db import _to_async_url

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parents[2] / "migrations" / "data"

CONCEPT_CSV = DATA_DIR / "concept-normalization-overrides.csv"
DIMENSION_CSV = DATA_DIR / "dimension-normalization-overrides.csv"
FACTS_CSV = DATA_DIR / "financial-facts-overrides.csv"

CONCEPT_TABLE = "concept_normalization_overrides"
DIMENSION_TABLE = "dimension_normalization_overrides"
FACTS_TABLE = "financial_facts_overrides"

TABLE_CHOICES = {
    "concept": (CONCEPT_TABLE, CONCEPT_CSV),
    "dimension": (DIMENSION_TABLE, DIMENSION_CSV),
    "facts": (FACTS_TABLE, FACTS_CSV),
}

# CSV encoding (must match api/admin.py): empty -> None, __EMPTY__ -> ''
_CSV_EMPTY = "__EMPTY__"


class SeedError(Exception):
    """Raised when a CSV cannot be parsed or the database is unreachable."""


def _optional(value: Optional[str]) -> Optional[str]:
    """Return a stripped value, or None when the CSV cell is blank."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _optional_empty_sentinel(value: Optional[str]) -> Optional[str]:
    """Parse a match column: blank -> None (wildcard), ``__EMPTY__`` -> ''."""
    parsed = _optional(value)
    if parsed == _CSV_EMPTY:
        return ""
    return parsed


def _parse_bool(value: Optional[str], row_num: int, column: str) -> bool:
    """Parse a CSV boolean cell."""
    parsed = (value or "").strip().lower()
    if parsed in ("true", "1", "yes", "t"):
        return True
    if parsed in ("false", "0", "no", "f", ""):
        return False
    raise SeedError(f"Row {row_num}: invalid boolean in {column!r}: {value!r}")


def _parse_decimal(
    value: Optional[str], row_num: int, column: str
) -> Optional[Decimal]:
    """Parse a numeric CSV cell."""
    parsed = _optional(value)
    if parsed is None:
        return None
    try:
        return Decimal(parsed)
    except InvalidOperation as e:
        raise SeedError(
            f"Row {row_num}: invalid number in {column!r}: {value!r}"
        ) from e


def _parse_date(value: Optional[str], row_num: int, column: str) -> Optional[date]:
    """Parse an ISO date CSV cell."""
    parsed = _optional(value)
    if parsed is None:
        return None
    try:
        return datetime.strptime(parsed, "%Y-%m-%d").date()
    except ValueError as e:
        raise SeedError(
            f"Row {row_num}: invalid date in {column!r} (expected YYYY-MM-DD): {value!r}"
        ) from e


def _parse_int(value: Optional[str], row_num: int, column: str) -> int:
    """Parse an integer CSV cell."""
    parsed = _optional(value)
    try:
        return int(parsed)  # type: ignore[arg-type]
    except (TypeError, ValueError) as e:
        raise SeedError(
            f"Row {row_num}: invalid integer in {column!r}: {value!r}"
        ) from e


def _read_csv(path: Path) -> List[tuple[int, Dict[str, str]]]:
    """Read a CSV into (row_number, row) pairs, skipping fully blank lines."""
    if not path.is_file():
        raise SeedError(f"CSV not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [
            (row_num, row)
            for row_num, row in enumerate(reader, start=2)
            if any((value or "").strip() for value in row.values())
        ]


def _require(row: Dict[str, str], fields: Sequence[str], row_num: int) -> None:
    """Raise when a required column is blank."""
    missing = [field for field in fields if not (row.get(field) or "").strip()]
    if missing:
        raise SeedError(
            f"Row {row_num}: missing required column value(s): {', '.join(missing)}"
        )


def parse_concept_rows(path: Path = CONCEPT_CSV) -> List[dict]:
    """Parse the concept normalization overrides CSV."""
    parsed = []
    for row_num, row in _read_csv(path):
        _require(
            row,
            ["company_id", "concept", "statement", "normalized_label"],
            row_num,
        )
        parsed.append(
            {
                "company_id": _parse_int(row["company_id"], row_num, "company_id"),
                "concept": row["concept"].strip(),
                "statement": row["statement"].strip(),
                "normalized_label": row["normalized_label"].strip(),
                "is_abstract": _parse_bool(
                    row.get("is_abstract"), row_num, "is_abstract"
                ),
                "is_global": _parse_bool(row.get("is_global"), row_num, "is_global"),
                "description": _optional(row.get("description")),
                "unit": _optional(row.get("unit")),
                "weight": _parse_decimal(row.get("weight"), row_num, "weight"),
                "parent_concept": _optional(row.get("parent_concept")),
                "abstract_concept": _optional(row.get("abstract_concept")),
            }
        )
    return parsed


def parse_dimension_rows(path: Path = DIMENSION_CSV) -> List[dict]:
    """Parse the dimension normalization overrides CSV."""
    parsed = []
    for row_num, row in _read_csv(path):
        _require(row, ["company_id", "axis", "normalized_axis_label"], row_num)
        tags = _optional(row.get("tags"))
        parsed.append(
            {
                "company_id": _parse_int(row["company_id"], row_num, "company_id"),
                "axis": row["axis"].strip(),
                "member": _optional(row.get("member")),
                "member_label": _optional(row.get("member_label")),
                "is_global": _parse_bool(row.get("is_global"), row_num, "is_global"),
                "normalized_axis_label": row["normalized_axis_label"].strip(),
                "normalized_member_label": _optional(
                    row.get("normalized_member_label")
                ),
                "tags": tags.split(";") if tags else None,
            }
        )
    return parsed


def parse_facts_rows(path: Path = FACTS_CSV) -> List[dict]:
    """Parse the financial facts overrides CSV."""
    parsed = []
    for row_num, row in _read_csv(path):
        _require(row, ["company_id", "concept", "statement", "to_concept"], row_num)
        to_axis = _optional(row.get("to_axis"))
        to_member = _optional(row.get("to_member"))
        to_member_label = _optional(row.get("to_member_label"))
        targets = [to_axis, to_member, to_member_label]
        if any(target is not None for target in targets) and not all(
            target is not None for target in targets
        ):
            raise SeedError(
                f"Row {row_num}: to_axis, to_member and to_member_label must be "
                "either all set or all blank"
            )

        record = {
            "company_id": _parse_int(row["company_id"], row_num, "company_id"),
            "concept": row["concept"].strip(),
            "statement": row["statement"].strip(),
            "axis": _optional_empty_sentinel(row.get("axis")),
            "member": _optional_empty_sentinel(row.get("member")),
            "label": _optional(row.get("label")),
            "form_type": _optional(row.get("form_type")),
            "from_period": _parse_date(row.get("from_period"), row_num, "from_period"),
            "to_period": _parse_date(row.get("to_period"), row_num, "to_period"),
            "is_global": _parse_bool(row.get("is_global"), row_num, "is_global"),
            "to_concept": row["to_concept"].strip(),
            "to_axis": to_axis,
            "to_member": to_member,
            "to_member_label": to_member_label,
            "to_weight": _parse_decimal(row.get("to_weight"), row_num, "to_weight"),
        }
        # The CSV carries stable ids so admin exports round-trip; keep them.
        override_id = _optional(row.get("id"))
        if override_id is not None:
            record["id"] = _parse_int(override_id, row_num, "id")
        parsed.append(record)
    return parsed


async def _known_company_ids(conn: AsyncConnection, companies: Table) -> Set[int]:
    """Return the set of company ids that exist in the database."""
    result = await conn.execute(select(companies.c.id))
    return {row[0] for row in result}


def _filter_known_companies(
    rows: List[dict], known: Set[int], label: str
) -> List[dict]:
    """Drop rows for companies that have not been ingested yet.

    Overrides are often authored before a company is loaded; skipping keeps the
    sync usable instead of failing the whole transaction on a foreign key.
    """
    kept = [row for row in rows if row["company_id"] in known]
    skipped = {row["company_id"] for row in rows if row["company_id"] not in known}
    if skipped:
        logger.warning(
            "%s: skipped %s row(s) for unknown company_id(s) %s; ingest those "
            "companies first",
            label,
            len(rows) - len(kept),
            sorted(skipped),
        )
    return kept


async def _sync_concept(conn: AsyncConnection, table: Table, rows: List[dict]) -> None:
    """Replace concept overrides, deferring self-referencing columns.

    ``parent_concept`` and ``abstract_concept`` are foreign keys back into this
    same table, so rows are inserted without them and wired up afterwards. That
    avoids having to order the inserts parent-first.
    """
    await conn.execute(table.delete())
    if not rows:
        return

    await conn.execute(
        table.insert(),
        [{**row, "parent_concept": None, "abstract_concept": None} for row in rows],
    )

    links = [
        {
            "key_company_id": row["company_id"],
            "key_concept": row["concept"],
            "key_statement": row["statement"],
            "parent_concept": row["parent_concept"],
            "abstract_concept": row["abstract_concept"],
        }
        for row in rows
        if row["parent_concept"] or row["abstract_concept"]
    ]
    if links:
        await conn.execute(
            sa_text(
                f"UPDATE {CONCEPT_TABLE} SET parent_concept = :parent_concept, "
                "abstract_concept = :abstract_concept "
                "WHERE company_id = :key_company_id AND concept = :key_concept "
                "AND statement = :key_statement"
            ),
            links,
        )


async def _sync_simple(conn: AsyncConnection, table: Table, rows: List[dict]) -> None:
    """Replace a table that has no self-referencing foreign keys."""
    await conn.execute(table.delete())
    if rows:
        await conn.execute(table.insert(), rows)


async def _reset_sequence(conn: AsyncConnection, table_name: str) -> None:
    """Move the id sequence past any explicitly inserted ids."""
    await conn.execute(
        sa_text(
            f"SELECT setval(pg_get_serial_sequence('{table_name}', 'id'), "
            "COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) "
            f"FROM {table_name}"
        )
    )


class _Rollback(Exception):
    """Internal signal used to roll back the transaction after a dry run."""


async def seed(tables: Sequence[str], dry_run: bool) -> None:
    """Sync the selected override CSVs into the database."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SeedError("DATABASE_URL is not set (see env.example)")

    parsers = {
        "concept": (CONCEPT_TABLE, parse_concept_rows),
        "dimension": (DIMENSION_TABLE, parse_dimension_rows),
        "facts": (FACTS_TABLE, parse_facts_rows),
    }
    parsed = {name: (parsers[name][0], parsers[name][1]()) for name in tables}

    engine = create_async_engine(_to_async_url(database_url))
    try:
        async with engine.begin() as conn:
            metadata = MetaData()
            names = ["companies"] + [table_name for table_name, _ in parsed.values()]
            await conn.run_sync(
                lambda sync_conn: metadata.reflect(bind=sync_conn, only=names)
            )
            known = await _known_company_ids(conn, metadata.tables["companies"])

            for name, (table_name, rows) in parsed.items():
                table = metadata.tables[table_name]
                rows = _filter_known_companies(rows, known, table_name)

                existing = await conn.execute(
                    sa_text(f"SELECT COUNT(*) FROM {table_name}")
                )
                current = existing.scalar_one()

                if dry_run:
                    logger.info(
                        "[dry-run] %s: would replace %s existing row(s) with %s "
                        "row(s) from CSV",
                        table_name,
                        current,
                        len(rows),
                    )
                    continue

                if name == "concept":
                    await _sync_concept(conn, table, rows)
                else:
                    await _sync_simple(conn, table, rows)
                    await _reset_sequence(conn, table_name)

                logger.info(
                    "%s: replaced %s row(s) with %s row(s) from CSV",
                    table_name,
                    current,
                    len(rows),
                )

            if dry_run:
                raise _Rollback()
    except _Rollback:
        logger.info("[dry-run] no changes committed")
    finally:
        await engine.dispose()

    if not dry_run:
        logger.info(
            "Overrides synced. Run 'make refresh' to recompute derived financials."
        )


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Sync normalization override CSVs into the database."
    )
    parser.add_argument(
        "--tables",
        default=",".join(TABLE_CHOICES),
        help=(
            "Comma-separated subset of " f"{{{','.join(TABLE_CHOICES)}}} (default: all)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without committing",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point. Returns a process exit code."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    load_dotenv()
    args = _parse_args(argv)

    tables = [name.strip() for name in args.tables.split(",") if name.strip()]
    unknown = [name for name in tables if name not in TABLE_CHOICES]
    if unknown:
        logger.error(
            "Unknown table(s) %s; choose from %s",
            ", ".join(unknown),
            ", ".join(TABLE_CHOICES),
        )
        return 2

    try:
        asyncio.run(seed(tables, args.dry_run))
    except SeedError as e:
        logger.error("%s", e)
        return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
