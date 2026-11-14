"""Integration tests for refresh_dimension_breakdowns."""

from typing import Dict, List, Tuple

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

Breakdown = Tuple[str, Tuple[str, ...], Tuple[int, ...], str]


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


def _breakdowns(conn: Connection) -> List[Breakdown]:
    """Return (parent, members, fiscal years, source) for company 1."""
    rows = conn.execute(
        text(
            "SELECT parent_member, members, fiscal_years, source "
            "FROM dimension_breakdowns WHERE company_id = 1 "
            "ORDER BY parent_member, members"
        )
    ).all()
    return [
        (row.parent_member, tuple(row.members), tuple(row.fiscal_years), row.source)
        for row in rows
    ]


def test_refresh_infers_subtotal_and_alternative_breakdowns(
    test_engine: Engine,
) -> None:
    """Sets that add up to a member or to the line in two 10-Ks are kept."""
    with test_engine.begin() as conn:
        _seed(conn, REVENUE)
        conn.execute(text("CALL refresh_financials(ARRAY[1]::int[])"))
        breakdowns = _breakdowns(conn)

    assert breakdowns == [
        ("", ("Domestic", "International"), (2023, 2024), "inferred"),
        ("", ("Products", "Services"), (2023, 2024), "inferred"),
        ("Products", ("iPad", "iPhone"), (2023, 2024), "inferred"),
    ]


def test_refresh_skips_breakdown_seen_in_one_year(test_engine: Engine) -> None:
    """A single matching year is not enough evidence."""
    with test_engine.begin() as conn:
        _seed(conn, {2023: REVENUE[2023]})
        conn.execute(text("CALL refresh_financials(ARRAY[1]::int[])"))
        assert _breakdowns(conn) == []


def test_refresh_omits_a_year_where_a_known_set_falls_short(
    test_engine: Engine,
) -> None:
    """A year that falls short stays out of fiscal_years; the set is kept."""
    short_year = {
        "": 1500,
        "Products": 1000,
        "iPhone": 600,
        "iPad": 400,
        "Services": 300,
        "Domestic": 1100,
        "International": 400,
    }
    with test_engine.begin() as conn:
        _seed(conn, {**REVENUE, 2025: short_year})
        conn.execute(text("CALL refresh_financials(ARRAY[1]::int[])"))
        by_members = {row[1]: row for row in _breakdowns(conn)}

    assert by_members[("Domestic", "International")][2:] == (
        (2023, 2024, 2025),
        "inferred",
    )
    assert by_members[("Products", "Services")][2:] == ((2023, 2024), "inferred")


def test_refresh_ignores_years_where_a_tied_member_could_swap_in(
    test_engine: Engine,
) -> None:
    """A set that only adds up because two members share a value is not kept."""
    tied = {**REVENUE[2024], "Region": REVENUE[2024]["Domestic"]}
    with test_engine.begin() as conn:
        _seed(conn, {**REVENUE, 2025: tied, 2026: tied})
        conn.execute(text("CALL refresh_financials(ARRAY[1]::int[])"))
        members = {row[1] for row in _breakdowns(conn) if row[0] == ""}

    assert members == {("Domestic", "International"), ("Products", "Services")}


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

    assert breakdowns == [
        ("", ("iPad", "iPhone", "Services"), (2023, 2024), "override"),
        ("", ("Products", "Services"), (2023, 2024), "override"),
    ]
