"""Read tables into polars and write derived frames back, through asyncpg.

Loading uses plain ``SELECT`` per company. Writing replaces a company's rows
in one statement pair: ``DELETE`` then binary ``COPY``. The derived tables have
no foreign keys between them, so nothing else needs to be kept in step.
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any, Iterable, Iterator, Optional

import asyncpg
import polars as pl
from dotenv import load_dotenv

from .schemas import Table

_DRIVER_PREFIXES = (
    "postgresql+asyncpg://",
    "postgresql+pg8000://",
    "postgresql+psycopg2://",
)


def database_url_from_env() -> str:
    """``DATABASE_URL`` from the environment or ``.env``, as the other scripts read it."""
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set (see env.example)")
    return url


def to_dsn(database_url: str) -> str:
    """Turn a SQLAlchemy URL into a libpq DSN asyncpg accepts."""
    for prefix in _DRIVER_PREFIXES:
        if database_url.startswith(prefix):
            return "postgresql://" + database_url[len(prefix) :]
    return database_url


def frame_from_records(table: Table, records: Iterable[Any]) -> pl.DataFrame:
    """Build a frame with the table's schema from asyncpg records or tuples."""
    rows = [tuple(r) for r in records]
    if not rows:
        return table.empty()
    return pl.DataFrame(rows, schema=table.schema, orient="row")


async def load(
    conn: asyncpg.Connection,
    table: Table,
    company_id: Optional[int] = None,
    where: Optional[str] = None,
) -> pl.DataFrame:
    """Load ``table`` for one company, or whole when ``company_id`` is None."""
    columns = ", ".join(f'"{c}"' for c in table.columns)
    sql = f"SELECT {columns} FROM {table.name}"
    clauses = []
    args: list[Any] = []
    if company_id is not None and table.company_column:
        clauses.append(f"{table.company_column} = $1")
        args.append(company_id)
    if where:
        clauses.append(where)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    return frame_from_records(table, await conn.fetch(sql, *args))


def _clean_decimal(value: Decimal) -> Decimal:
    """Drop the trailing zeros Decimal(38, 10) adds, without exponent notation."""
    normalized = value.normalize()
    if normalized == 0:
        return Decimal(0)
    exponent = normalized.as_tuple().exponent
    if isinstance(exponent, int) and exponent > 0:
        return normalized.quantize(Decimal(1))
    return normalized


def _records(frame: pl.DataFrame) -> Iterator[tuple[Any, ...]]:
    decimal_positions = [
        i for i, dtype in enumerate(frame.dtypes) if isinstance(dtype, pl.Decimal)
    ]
    for row in frame.iter_rows():
        if decimal_positions:
            values = list(row)
            for i in decimal_positions:
                if values[i] is not None:
                    values[i] = _clean_decimal(values[i])
            yield tuple(values)
        else:
            yield row


async def replace_company_rows(
    conn: asyncpg.Connection, table: Table, company_id: int, frame: pl.DataFrame
) -> int:
    """Replace one company's rows in ``table`` with ``frame``. Returns rows written.

    Call inside a transaction so readers never see the company half-written.
    """
    frame = frame.select(table.columns)
    await conn.execute(
        f"DELETE FROM {table.name} WHERE {table.company_column} = $1", company_id
    )
    if frame.height:
        await conn.copy_records_to_table(
            table.name, records=_records(frame), columns=table.columns
        )
    return frame.height
