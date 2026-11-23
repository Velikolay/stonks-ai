"""Attach XBRL dimensions to facts that are already stored.

Re-parses each filing and writes ``financial_fact_dimensions`` for the facts
that match the stored rows. Display axis and member are left as they are.

Usage:
    python -m filings.scripts.backfill_fact_dimensions --company-ids 16
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from decimal import Decimal
from typing import Dict, Iterable, List, Sequence, Tuple

from dotenv import load_dotenv
from edgar import Company, set_identity
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from filings.models import FinancialFactCreate
from filings.parsers.sec_xbrl import SECXBRLParser

logger = logging.getLogger(__name__)

FORMS = ["10-K", "10-Q", "10-K/A", "10-Q/A", "20-F", "20-F/A"]
MatchKey = Tuple[str, str, str, str, str, str, str, str, str]


def _amount(value: object) -> str:
    """Return a fact amount as a stable decimal string."""
    if value is None:
        return ""
    return format(Decimal(str(value)).quantize(Decimal("0.00001")), "f")


def _key(fact: FinancialFactCreate) -> MatchKey:
    """Return the identity shared by a parsed fact and its stored row."""
    period = fact.period.value if fact.period is not None else ""
    period_end = fact.period_end.isoformat() if fact.period_end is not None else ""
    return (
        fact.concept or "",
        fact.label or "",
        fact.statement or "",
        period_end,
        period,
        fact.axis or "",
        fact.member or "",
        fact.member_label or "",
        _amount(fact.value),
    )


def _row_key(row: object) -> MatchKey:
    """Return the match key of a stored fact."""
    period_end = row.period_end.isoformat() if row.period_end is not None else ""
    return (
        row.concept or "",
        row.label or "",
        row.statement or "",
        period_end,
        row.period or "",
        row.axis or "",
        row.member or "",
        row.member_label or "",
        _amount(row.value),
    )


def _index_filings(cik: str) -> Dict[str, object]:
    """Return EDGAR filings for one CIK, keyed by accession number."""
    filings = Company(cik).get_filings(form=FORMS, is_xbrl=True)
    indexed: Dict[str, object] = {}
    if not filings:
        return indexed
    for item in filings:
        accession = getattr(item, "accession_number", None)
        if accession:
            indexed[str(accession)] = item
    return indexed


def _filing_rows(conn: Connection, company_ids: Sequence[int]) -> List[object]:
    """Return stored filings for the requested companies."""
    forms = ", ".join(f"'{form}'" for form in FORMS)
    company_filter = ""
    if company_ids:
        ids = ", ".join(str(int(company_id)) for company_id in company_ids)
        company_filter = f"AND c.id IN ({ids})"
    return list(
        conn.execute(
            text(
                "SELECT f.id, f.number AS accession, fe.number AS cik, "
                "c.id AS company_id, c.name "
                "FROM filings f "
                "JOIN filing_entities fe ON fe.id = f.filing_entity_id "
                "JOIN companies c ON c.id = f.company_id "
                f"WHERE f.form_type IN ({forms}) {company_filter} "
                "ORDER BY c.id, f.filing_date, f.id"
            )
        )
    )


def _stored_facts(conn: Connection, filing_id: int) -> Dict[MatchKey, List[int]]:
    """Return stored fact ids grouped by the parse match key."""
    rows = conn.execute(
        text(
            "SELECT id, concept, label, statement, period_end, period::text AS period, "
            "axis, member, member_label, value FROM financial_facts "
            "WHERE filing_id = :filing_id"
        ),
        {"filing_id": filing_id},
    )
    grouped: Dict[MatchKey, List[int]] = defaultdict(list)
    for row in rows:
        grouped[_row_key(row)].append(row.id)
    return grouped


def _dimension_rows(
    parsed: Iterable[FinancialFactCreate], stored: Dict[MatchKey, List[int]]
) -> Tuple[List[dict], int, int]:
    """Pair parsed coordinates with stored fact ids.

    Returns:
        Rows to insert, the number of dimensional facts that matched, and the
        number of dimensional facts that did not.
    """
    rows: List[dict] = []
    matched = 0
    missed = 0
    for fact in parsed:
        if not fact.axis or not fact.dimensions:
            continue
        fact_ids = stored.get(_key(fact))
        if not fact_ids:
            missed += 1
            continue
        fact_id = fact_ids.pop(0)
        matched += 1
        for ordinal, dimension in enumerate(fact.dimensions):
            rows.append(
                {
                    "fact_id": fact_id,
                    "ordinal": ordinal,
                    "axis": dimension.axis,
                    "member": dimension.member,
                    "member_label": dimension.member_label,
                    "is_default": dimension.is_default,
                }
            )
    return rows, matched, missed


def _write_filing(conn: Connection, filing_id: int, rows: Sequence[dict]) -> None:
    """Replace one filing's dimension rows."""
    conn.execute(
        text(
            "DELETE FROM financial_fact_dimensions d USING financial_facts f "
            "WHERE d.fact_id = f.id AND f.filing_id = :filing_id"
        ),
        {"filing_id": filing_id},
    )
    if rows:
        conn.execute(
            text(
                "INSERT INTO financial_fact_dimensions (fact_id, ordinal, axis, "
                "member, member_label, is_default) VALUES (:fact_id, :ordinal, "
                ":axis, :member, :member_label, :is_default)"
            ),
            list(rows),
        )


def backfill(engine: Engine, company_ids: Sequence[int]) -> None:
    """Re-parse the requested filings and store their XBRL dimensions."""
    parser = SECXBRLParser()
    indexes: Dict[str, Dict[str, object]] = {}
    parsed_filings = 0
    missing_filings = 0
    matched = 0
    missed = 0
    with engine.connect() as conn:
        filings = _filing_rows(conn, company_ids)
    logger.info("Filings to read: %s", len(filings))
    for filing in filings:
        cik = str(filing.cik)
        if cik not in indexes:
            indexes[cik] = _index_filings(cik)
            logger.info(
                "Indexed %s filings for CIK %s (%s)",
                len(indexes[cik]),
                cik,
                filing.name,
            )
        edgar_filing = indexes[cik].get(str(filing.accession))
        if edgar_filing is None:
            missing_filings += 1
            logger.warning("No EDGAR filing for %s", filing.accession)
            continue
        try:
            parsed = parser.parse_filing(edgar_filing)  # type: ignore[arg-type]
        except Exception:
            logger.exception("Could not parse %s", filing.accession)
            continue
        with engine.begin() as conn:
            stored = _stored_facts(conn, filing.id)
            rows, filing_matched, filing_missed = _dimension_rows(parsed, stored)
            dimensional = filing_matched + filing_missed
            if parsed_filings == 0 and dimensional and filing_matched < dimensional / 2:
                raise SystemExit(
                    f"{filing.accession} matched {filing_matched} of "
                    f"{dimensional} dimensional facts"
                )
            if dimensional and filing_matched == 0:
                logger.error(
                    "No dimensional facts matched %s; leaving it unchanged",
                    filing.accession,
                )
                missed += filing_missed
                continue
            _write_filing(conn, filing.id, rows)
        parsed_filings += 1
        matched += filing_matched
        missed += filing_missed
        logger.info(
            "%s matched %s dimensional facts, missed %s",
            filing.accession,
            filing_matched,
            filing_missed,
        )
    logger.info(
        "Parsed %s filings, %s missing from EDGAR, %s facts matched, %s missed",
        parsed_filings,
        missing_filings,
        matched,
        missed,
    )


def _parse_args() -> argparse.Namespace:
    """Return the command line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--company-ids",
        default="",
        help="Comma-separated company ids. Default: every company.",
    )
    return parser.parse_args()


def main() -> None:
    """Load settings and backfill fact dimensions."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    load_dotenv()
    identity = os.environ.get("EDGAR_IDENTITY")
    if not identity:
        raise SystemExit("EDGAR_IDENTITY is required")
    set_identity(identity)
    database_url = os.environ.get(
        "DATABASE_URL", "postgresql://rag_user:rag_password@localhost:5432/rag_db"
    )
    args = _parse_args()
    company_ids = [int(item) for item in args.company_ids.split(",") if item.strip()]
    backfill(create_engine(database_url), company_ids)


if __name__ == "__main__":
    main()
