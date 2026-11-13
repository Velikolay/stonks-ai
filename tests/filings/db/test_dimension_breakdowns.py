"""Integration tests for the breakdowns column on the financials tables."""

from typing import Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

AXIS = "test:SegmentAxis"

# fiscal year -> member -> value; "" is the undimensioned line. Products is a
# subtotal, and Domestic + International is a second split of the same line.
REVENUE = {
    2023: {
        "": 1000,
        "Products": 800,
        "iPhone": 530,
        "iPad": 270,
        "Services": 200,
        "Domestic": 610,
        "International": 390,
    },
    2024: {
        "": 1200,
        "Products": 950,
        "iPhone": 640,
        "iPad": 310,
        "Services": 250,
        "Domestic": 730,
        "International": 470,
    },
}

Breakdowns = List[Tuple[str, Tuple[str, ...]]]


def _seed(conn: Connection, revenue: Dict[int, Dict[str, int]]) -> None:
    """Insert one company with a 10-K of segmented revenue per fiscal year."""
    conn.execute(text("INSERT INTO companies (id, name) VALUES (1, 'Test Co')"))
    conn.execute(
        text(
            "INSERT INTO filing_entities (id, registry, number, status, company_id) "
            "VALUES (1, 'SEC', '1', 'active', 1)"
        )
    )
    for year, members in revenue.items():
        conn.execute(
            text(
                "INSERT INTO filings (id, company_id, filing_entity_id, registry, "
                "number, form_type, filing_date, fiscal_period_end, fiscal_year, "
                "fiscal_quarter) VALUES (:id, 1, 1, 'SEC', :number, '10-K', "
                ":period_end, :period_end, :year, 4)"
            ),
            {
                "id": year,
                "number": f"10-K-{year}",
                "period_end": f"{year}-09-30",
                "year": year,
            },
        )
        for member, value in members.items():
            conn.execute(
                text(
                    "INSERT INTO financial_facts (filing_id, company_id, form_type, "
                    "concept, label, is_abstract, value, unit, axis, member, "
                    "member_label, statement, period_end, period) VALUES (:filing, "
                    "1, '10-K', 'test:Revenue', 'Revenue', false, :value, 'usd', "
                    ":axis, :member, :member_label, 'Income Statement', "
                    ":period_end, 'YTD')"
                ),
                {
                    "filing": year,
                    "value": value,
                    "axis": AXIS if member else "",
                    "member": f"test:{member}Member" if member else "",
                    "member_label": member,
                    "period_end": f"{year}-09-30",
                },
            )


def _breakdowns(conn: Connection) -> Dict[Tuple[int, str], Breakdowns]:
    """Return (axis, members) per (fiscal year, member) from yearly_financials."""
    rows = conn.execute(
        text(
            "SELECT fiscal_year, member, breakdowns FROM yearly_financials "
            "WHERE company_id = 1 AND breakdowns IS NOT NULL"
        )
    ).all()
    return {
        (row.fiscal_year, row.member): [
            (b["axis"], tuple(b["members"])) for b in row.breakdowns
        ]
        for row in rows
    }


def _expected(line: Breakdowns, products: Optional[Breakdowns] = None) -> Dict:
    """Expect the same breakdowns on the line and on Products in both years."""
    expected = {}
    for year in REVENUE:
        expected[(year, "")] = line
        if products:
            expected[(year, "Products")] = products
    return expected


def test_refresh_infers_subtotal_and_alternative_breakdowns(
    test_engine: Engine,
) -> None:
    """Sets that add up to a member or to the line in two 10-Ks are kept."""
    with test_engine.begin() as conn:
        _seed(conn, REVENUE)
        conn.execute(text("CALL refresh_financials(ARRAY[1]::int[])"))
        breakdowns = _breakdowns(conn)

    assert breakdowns == _expected(
        line=[(AXIS, ("Domestic", "International")), (AXIS, ("Products", "Services"))],
        products=[(AXIS, ("iPad", "iPhone"))],
    )


def test_refresh_skips_breakdown_seen_in_one_year(test_engine: Engine) -> None:
    """A single matching year is not enough evidence."""
    with test_engine.begin() as conn:
        _seed(conn, {2023: REVENUE[2023]})
        conn.execute(text("CALL refresh_financials(ARRAY[1]::int[])"))
        assert _breakdowns(conn) == {}


def test_overrides_replace_inferred_breakdowns(test_engine: Engine) -> None:
    """A parent's override rows are its only breakdowns, where they add up."""
    with test_engine.begin() as conn:
        _seed(conn, REVENUE)
        conn.execute(
            text(
                "INSERT INTO dimension_breakdown_overrides (company_id, "
                "normalized_label, axis, parent_member, members) VALUES "
                "(1, NULL, :axis, '', ARRAY['Services', 'Products']), "
                "(1, 'Revenue', :axis, '', ARRAY['Services', 'iPhone', 'iPad']), "
                "(1, 'Revenue', :axis, 'Products', ARRAY[]::varchar[]), "
                "(1, 'Revenue', :axis, 'Services', ARRAY['iPhone', 'iPad'])"
            ),
            {"axis": AXIS},
        )
        conn.execute(text("CALL refresh_financials(ARRAY[1]::int[])"))
        breakdowns = _breakdowns(conn)

    assert breakdowns == _expected(
        line=[(AXIS, ("Products", "Services")), (AXIS, ("iPad", "iPhone", "Services"))]
    )
