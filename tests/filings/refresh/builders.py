"""Small frames for refresh unit tests; unspecified columns take neutral defaults."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Iterable, Mapping

import polars as pl

from filings.refresh.inputs import CompanyData
from filings.refresh.io.schemas import TABLES

#: Defaults for NOT NULL columns, so a test only states what it is about.
DEFAULTS: dict[str, Any] = {
    "company_id": 1,
    "statement": "Income Statement",
    "axis": "",
    "member": "",
    "member_label": "",
    "is_abstract": False,
    "is_global": False,
    "is_duplicate": False,
    "is_synthetic": False,
    "weight": Decimal(1),
    "unit": "usd",
}


def _value(column: str, value: Any) -> Any:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if column in (
            "value",
            "comparative_value",
            "weight",
            "to_weight",
            "print_sign",
        ):
            return Decimal(str(value))
    if isinstance(value, str) and column.endswith(("period_end", "period", "_date")):
        if len(value) == 10 and value[4] == "-":
            return date.fromisoformat(value)
    return value


def frame(table: str, rows: Iterable[Mapping[str, Any]]) -> pl.DataFrame:
    """Rows of ``table`` with defaults filled in and the table's schema.

    Override tables get no defaults: a blank cell there is NULL, which means
    "match anything", not the empty string.
    """
    schema = TABLES[table].schema
    is_override = table.endswith("_overrides") or table == "financial_rewrites"
    defaults = {"company_id": 1, "is_global": False} if is_override else DEFAULTS
    filled = []
    for row in rows:
        full = {c: defaults.get(c) for c in schema.names()}
        full.update({k: _value(k, v) for k, v in row.items()})
        filled.append(full)
    if not filled:
        return TABLES[table].empty()
    return pl.DataFrame(filled, schema=schema)


def company(**tables: Iterable[Mapping[str, Any]]) -> CompanyData:
    """A company 1 with the given tables, every other one empty."""
    data = CompanyData(company_id=1)
    for name, rows in tables.items():
        data.put(name, frame(name, rows))
    return data
