"""Pinned company ids for tickers that the override CSVs depend on.

``migrations/data/company-ids.csv`` is the lock. Ingest and the companies
migration both read it, so a ticker listed there is inserted at that id
instead of taking the next sequence value.
"""

import csv
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

CSV_PATH = (
    Path(__file__).resolve().parents[1] / "migrations" / "data" / "company-ids.csv"
)


@lru_cache(maxsize=1)
def _locked_rows() -> Tuple[Tuple[int, str, str], ...]:
    """Return ``(company_id, ticker, name)`` rows from the lock file."""
    rows = []
    with open(CSV_PATH, encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            ticker = (row.get("ticker") or "").strip().upper()
            raw_id = (row.get("company_id") or "").strip()
            name = (row.get("name") or "").strip()
            if not ticker or not raw_id or not name:
                continue
            rows.append((int(raw_id), ticker, name))
    return tuple(rows)


def company_id_by_ticker() -> Dict[str, int]:
    """Return the uppercase ticker to pinned company id."""
    return {ticker: company_id for company_id, ticker, _name in _locked_rows()}


def locked_companies() -> Dict[int, str]:
    """Return pinned company id to display name, one entry per company."""
    companies: Dict[int, str] = {}
    for company_id, _ticker, name in _locked_rows():
        companies.setdefault(company_id, name)
    return companies


def locked_company_id(tickers: Sequence[str]) -> Optional[int]:
    """Return the pinned company id when any ticker is locked.

    Args:
        tickers: Tickers reported for one EDGAR company.

    Returns:
        The pinned id, or None when none of the tickers are locked.

    Raises:
        ValueError: The tickers are locked to more than one company id.
    """
    mapping = company_id_by_ticker()
    found = {
        mapping[ticker.strip().upper()]
        for ticker in tickers
        if ticker and ticker.strip().upper() in mapping
    }
    if not found:
        return None
    if len(found) > 1:
        raise ValueError(
            f"Tickers {list(tickers)} are locked to company ids {sorted(found)}"
        )
    return next(iter(found))
