"""Integration tests for refresh_dimension_breakdowns."""

from typing import Callable, Dict, List, Tuple

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
    refresh: Callable[..., None],
) -> None:
    """Sets that add up to a member or to the line in two 10-Ks are kept."""
    with test_engine.begin() as conn:
        _seed(conn, REVENUE)
    refresh()
    with test_engine.connect() as conn:
        breakdowns = _breakdowns(conn)

    assert breakdowns == [
        ("", ("Domestic", "International"), (2023, 2024), "inferred"),
        ("", ("Products", "Services"), (2023, 2024), "inferred"),
        ("Products", ("iPad", "iPhone"), (2023, 2024), "inferred"),
    ]


def test_refresh_skips_breakdown_seen_in_one_year(
    test_engine: Engine, refresh: Callable[..., None]
) -> None:
    """A single matching year is not enough evidence."""
    with test_engine.begin() as conn:
        _seed(conn, {2023: REVENUE[2023]})
    refresh()
    with test_engine.connect() as conn:
        assert _breakdowns(conn) == []


def test_refresh_omits_a_year_where_a_known_set_falls_short(
    test_engine: Engine,
    refresh: Callable[..., None],
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
    refresh()
    with test_engine.connect() as conn:
        by_members = {row[1]: row for row in _breakdowns(conn)}

    assert by_members[("Domestic", "International")][2:] == (
        (2023, 2024, 2025),
        "inferred",
    )
    assert by_members[("Products", "Services")][2:] == ((2023, 2024), "inferred")


def test_refresh_ignores_years_where_a_tied_member_could_swap_in(
    test_engine: Engine,
    refresh: Callable[..., None],
) -> None:
    """A set that only adds up because two members share a value is not kept."""
    tied = {**REVENUE[2024], "Region": REVENUE[2024]["Domestic"]}
    with test_engine.begin() as conn:
        _seed(conn, {**REVENUE, 2025: tied, 2026: tied})
    refresh()
    with test_engine.connect() as conn:
        members = {row[1] for row in _breakdowns(conn) if row[0] == ""}

    assert members == {("Domestic", "International"), ("Products", "Services")}


def test_overrides_replace_inferred_breakdowns(
    test_engine: Engine, refresh: Callable[..., None]
) -> None:
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
    refresh()
    with test_engine.connect() as conn:
        breakdowns = _breakdowns(conn)

    assert breakdowns == [
        ("", ("iPad", "iPhone", "Services"), (2023, 2024), "override"),
        ("", ("Products", "Services"), (2023, 2024), "override"),
    ]


def _line_revenue() -> Dict[int, Dict[str, int]]:
    """Revenue whose product lines only add up inside one dimension context.

    Seventeen lines would put the flat axis over the 16-member cap. They are
    stored as a second dimension of Products, so the flat list stays Domestic,
    International and Products.
    """
    lines_2023 = {
        f"Line {index:02d}": 48 if index == 1 else 47 for index in range(1, 18)
    }
    lines_2024 = {
        f"Line {index:02d}": 100 if index == 1 else 50 for index in range(1, 18)
    }
    return {
        2023: {
            "": 1000,
            "Products": 800,
            "Domestic": 600,
            "International": 400,
            **lines_2023,
        },
        2024: {
            "": 1200,
            "Products": 900,
            "Domestic": 700,
            "International": 500,
            **lines_2024,
        },
    }


def _mark_line_context(conn: Connection, lines: List[str]) -> None:
    """Store Products as one dimension and each line as Products plus itself."""
    conn.execute(
        text(
            "INSERT INTO financial_fact_dimensions (fact_id, ordinal, axis, member, "
            "member_label) SELECT id, 0, axis, member, member_label "
            "FROM financial_facts WHERE member_label = 'Products'"
        )
    )
    for line in lines:
        conn.execute(
            text(
                "INSERT INTO financial_fact_dimensions (fact_id, ordinal, axis, "
                "member, member_label) SELECT id, 0, :axis, 'test:ProductsMember', "
                "'Products' FROM financial_facts WHERE member_label = :line "
                "UNION ALL SELECT id, 1, 'test:LineAxis', member, member_label "
                "FROM financial_facts WHERE member_label = :line"
            ),
            {"axis": AXIS, "line": line},
        )


def test_context_breakdown_ignores_the_flat_member_cap(
    test_engine: Engine, refresh: Callable[..., None]
) -> None:
    """A cross-tab sums in full, and the flat regions still infer beside it."""
    revenue = _line_revenue()
    lines = [label for label in revenue[2023] if label.startswith("Line ")]
    with test_engine.begin() as conn:
        _seed(conn, revenue)
        _mark_line_context(conn, lines)
    refresh()
    with test_engine.connect() as conn:
        breakdowns = _breakdowns(conn)

    assert breakdowns == [
        ("", ("Domestic", "International"), (2023, 2024), "inferred"),
        ("Products", tuple(sorted(lines)), (2023, 2024), "inferred"),
    ]


def test_axis_default_stays_on_the_flat_list(
    test_engine: Engine, refresh: Callable[..., None]
) -> None:
    """The axis total beside a region does not turn that region into a cell."""
    with test_engine.begin() as conn:
        _seed(conn, REVENUE)
        conn.execute(
            text(
                "INSERT INTO financial_fact_dimensions (fact_id, ordinal, axis, "
                "member, member_label, is_default) "
                "SELECT id, 0, axis, member, member_label, false "
                "FROM financial_facts WHERE member_label IN "
                "('Domestic', 'International') "
                "UNION ALL SELECT id, 1, 'srt:ConsolidationItemsAxis', "
                "'us-gaap:OperatingSegmentsMember', 'Operating segments', true "
                "FROM financial_facts WHERE member_label IN "
                "('Domestic', 'International')"
            )
        )
    refresh()
    with test_engine.connect() as conn:
        breakdowns = _breakdowns(conn)

    assert breakdowns == [
        ("", ("Domestic", "International"), (2023, 2024), "inferred"),
        ("", ("Products", "Services"), (2023, 2024), "inferred"),
        ("Products", ("iPad", "iPhone"), (2023, 2024), "inferred"),
    ]


def test_companion_on_every_member_stays_on_the_flat_list(
    test_engine: Engine, refresh: Callable[..., None]
) -> None:
    """A total tagged on every region is not a second cut of those regions."""
    with test_engine.begin() as conn:
        _seed(conn, REVENUE)
        conn.execute(
            text(
                "INSERT INTO financial_fact_dimensions (fact_id, ordinal, axis, "
                "member, member_label, is_default) "
                "SELECT id, 0, 'srt:ProductOrServiceAxis', 'test:StreamingMember', "
                "'Streaming', false FROM financial_facts WHERE member_label IN "
                "('Domestic', 'International') "
                "UNION ALL SELECT id, 1, axis, member, member_label, false "
                "FROM financial_facts WHERE member_label IN "
                "('Domestic', 'International')"
            )
        )
    refresh()
    with test_engine.connect() as conn:
        parents = {row[1] for row in _breakdowns(conn) if row[0] == ""}

    assert ("Domestic", "International") in parents
