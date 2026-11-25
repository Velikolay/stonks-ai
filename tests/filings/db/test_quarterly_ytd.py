"""Quarterly derivation when a statement header shares the revenue series."""

from typing import Callable

from sqlalchemy import text
from sqlalchemy.engine import Engine


def test_revenue_header_does_not_block_ytd_subtraction(
    test_engine: Engine, refresh: Callable[..., None]
) -> None:
    """A header on the same date is not the previous quarter.

    The header's id is lower than the total, which is the order that used to
    leave the six- and nine-month figures unreduced and the year-end quarter
    negative.
    """
    with test_engine.begin() as conn:
        conn.execute(text("INSERT INTO companies (id, name) VALUES (1, 'Test Co')"))
        conn.execute(
            text(
                "INSERT INTO filing_entities (id, registry, number, status, "
                "company_id) VALUES (1, 'SEC', '1', 'active', 1)"
            )
        )
        filings = [
            (1, "10-Q", "2016-09-30", 2016, 3),
            (2, "10-Q", "2016-12-31", 2016, 4),
            (3, "10-Q", "2017-03-31", 2017, 1),
            (4, "10-K", "2017-06-30", 2017, 2),
        ]
        for filing_id, form, period_end, year, quarter in filings:
            conn.execute(
                text(
                    "INSERT INTO filings (id, company_id, filing_entity_id, "
                    "registry, number, form_type, filing_date, fiscal_period_end, "
                    "fiscal_year, fiscal_quarter) VALUES (:id, 1, 1, 'SEC', "
                    ":number, :form, :period_end, :period_end, :year, :quarter)"
                ),
                {
                    "id": filing_id,
                    "number": f"{form}-{period_end}",
                    "form": form,
                    "period_end": period_end,
                    "year": year,
                    "quarter": quarter,
                },
            )
        facts = [
            (1, 1, "test:RevenueAbstract", True, None, "Q", "2016-09-30"),
            (2, 1, "test:Revenue", False, 100, "Q", "2016-09-30"),
            (3, 2, "test:RevenueAbstract", True, None, "YTD", "2016-12-31"),
            (4, 2, "test:Revenue", False, 250, "YTD", "2016-12-31"),
            (5, 3, "test:RevenueAbstract", True, None, "YTD", "2017-03-31"),
            (6, 3, "test:Revenue", False, 400, "YTD", "2017-03-31"),
            (7, 4, "test:Revenue", False, 550, "YTD", "2017-06-30"),
        ]
        for (
            fact_id,
            filing_id,
            concept,
            is_abstract,
            value,
            period,
            period_end,
        ) in facts:
            conn.execute(
                text(
                    "INSERT INTO financial_facts_normalized (id, company_id, "
                    "filing_id, fiscal_filing_id, form_type, concept, label, "
                    "normalized_label, is_abstract, value, unit, axis, member, "
                    "statement, period_end, period, is_synthetic, is_duplicate) "
                    "SELECT :id, 1, :filing_id, :filing_id, form_type, :concept, "
                    "'Revenue', 'Revenue', :is_abstract, :value, 'usd', '', '', "
                    "'Income Statement', :period_end, :period, false, false "
                    "FROM filings WHERE id = :filing_id"
                ),
                {
                    "id": fact_id,
                    "filing_id": filing_id,
                    "concept": concept,
                    "is_abstract": is_abstract,
                    "value": value,
                    "period": period,
                    "period_end": period_end,
                },
            )
    refresh(["quarterly_financials"])
    with test_engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT period_end, value, source_type FROM quarterly_financials "
                "WHERE company_id = 1 AND NOT is_abstract "
                "ORDER BY period_end"
            )
        ).all()

    assert [
        (row.period_end.isoformat(), row.value, row.source_type) for row in rows
    ] == [
        ("2016-09-30", 100, "10-Q"),
        ("2016-12-31", 150, "10-Q"),
        ("2017-03-31", 150, "10-Q"),
        ("2017-06-30", 150, "calculated"),
    ]
