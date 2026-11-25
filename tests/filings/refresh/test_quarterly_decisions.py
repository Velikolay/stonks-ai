"""The quarterly decisions, called directly on plain records."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Optional

from filings.refresh.stages.quarterly.q4 import (
    AnnualMember,
    Filing,
    FirstThree,
    Q4Source,
    Quarter,
    first_three_quarters,
    member_q4,
    nine_month_source,
    rank_annual_filings,
)
from filings.refresh.stages.quarterly.restatement import YearBasis, moved_years
from filings.refresh.stages.quarterly.ytd import (
    Amount,
    Opening,
    opening_comparative,
    reduce_series,
)

D = Decimal
SERIES = (1, "Income Statement", "Revenue", "", "")
Q1, Q2, Q3, FY = (
    date(2023, 3, 31),
    date(2023, 6, 30),
    date(2023, 9, 30),
    date(2023, 12, 31),
)


# --- YTD reduction -----------------------------------------------------------


def amount(row: int, end: date, period: str, value: int, **extra: Any) -> Amount:
    fields: dict[str, Any] = dict(
        series=SERIES,
        row_nr=row,
        period_end=end,
        period=period,
        value=D(value),
        anchor_value=None,
        fiscal_quarter={Q1: 1, Q2: 2, Q3: 3}[end],
        annual_period_end=FY,
        is_non_additive=False,
        is_per_share=False,
    )
    fields.update(extra)
    return Amount(**fields)


def reduced(amounts: list[Amount]) -> list[tuple]:
    return [(r.value, r.drop_ytd, r.q3_ytd) for r in reduce_series(amounts)]


def test_ytd_totals_become_quarters() -> None:
    rows = [
        amount(0, Q1, "YTD", 100),
        amount(1, Q2, "YTD", 250),
        amount(2, Q3, "YTD", 450),
    ]
    # The opening YTD is not shown; the nine months are kept for Q4.
    assert reduced(rows) == [
        (D(100), True, None),
        (D(150), False, None),
        (D(200), False, D(450)),
    ]


def test_a_published_quarter_subtracts_its_own_ytd() -> None:
    rows = [
        amount(0, Q2, "Q", 150, anchor_value=D(250)),
        amount(1, Q3, "YTD", 450),
    ]
    assert [r.value for r in reduce_series(rows)] == [D(150), D(200)]


def test_a_ytd_without_the_quarter_before_is_dropped() -> None:
    rows = [amount(0, Q1, "Q", 100), amount(1, Q3, "YTD", 450)]
    assert reduced(rows)[1] == (D(450), True, D(450))


def test_share_counts_are_never_differenced() -> None:
    rows = [
        amount(0, Q1, "Q", 100, is_non_additive=True),
        amount(1, Q2, "YTD", 100, is_non_additive=True),
    ]
    assert reduced(rows)[1] == (D(100), False, None)


def test_opening_ytd_is_backfilled_from_a_later_comparative() -> None:
    def row(nr: int, end: date, period: str, comparative: Optional[date]) -> Opening:
        return Opening(
            SERIES, nr, nr, end, period, False, comparative is not None, comparative
        )

    rows = [row(0, Q2, "YTD", None), row(1, date(2024, 3, 31), "Q", Q1)]
    assert opening_comparative(rows) == (0, 1)
    assert opening_comparative([row(0, Q2, "Q", None), rows[1]]) is None


# --- Q1-Q3 per member-year ---------------------------------------------------


def quarter(fq: int, value: Optional[int], **extra: Any) -> Quarter:
    fields: dict[str, Any] = dict(
        member_year=(*SERIES, 10),
        filing_date=date(2023, 1, 1),
        is_comparative_backfill=False,
        fiscal_quarter=fq,
        value=None if value is None else D(value),
        restated_value=None,
        drop_ytd=False,
        q3_ytd=None,
        prefer_ytd_anchor=False,
    )
    fields.update(extra)
    return Quarter(**fields)


def test_first_three_quarters_sum() -> None:
    assert (
        first_three_quarters([quarter(1, 1), quarter(2, 2), quarter(3, 3)]).value == 6
    )


def test_a_missing_quarter_falls_back_to_the_nine_months() -> None:
    rows = [quarter(1, 1), quarter(3, 3, q3_ytd=D(7))]
    assert first_three_quarters(rows).value == 7


def test_restated_quarters_win() -> None:
    rows = [quarter(q, q, restated_value=D(10)) for q in (1, 2, 3)]
    assert first_three_quarters(rows).value == 30


# --- Restatement -------------------------------------------------------------


def year(
    label: str, annual: int, own: int, comparative: int, **extra: Any
) -> YearBasis:
    fields: dict[str, Any] = dict(
        line=(1, "Income Statement", label),
        fiscal_tag=10,
        annual=D(annual),
        next_annual=None,
        own=D(own),
        comparative=D(comparative),
        replacements={},
    )
    fields.update(extra)
    return YearBasis(**fields)


def test_overshooting_quarters_move_and_a_reprinted_line_follows() -> None:
    revenue = year("Revenue", 1000, 1100, 700)
    cost = year("Cost", 600, 450, 420, next_annual=D(600))
    unchanged = year("Tax", 100, 75, 75, next_annual=D(100))
    moved = moved_years([revenue, cost, unchanged])
    assert [y.line[2] for y in moved] == ["Revenue", "Cost"]


def test_nothing_follows_without_an_overshooting_line() -> None:
    assert moved_years([year("Cost", 600, 450, 420, next_annual=D(600))]) == []


def test_comparatives_that_reverse_the_year_do_not_move_it() -> None:
    assert moved_years([year("Revenue", 1000, 1100, 1200)]) == []


# --- Choosing the 10-K for Q4 -----------------------------------------------


def annual_filing(
    id: int, filed: date, members: dict[str, int], **extra: Any
) -> Filing:
    f = Filing(id, filed, extra.get("is_amendment", False))
    for i, (member, value) in enumerate(members.items()):
        f.rows.append(
            AnnualMember(
                (),
                id * 10 + i,
                id * 10 + i,
                id,
                filed,
                f.is_amendment,
                False,
                member,
                D(value),
                True,
                False,
            )
        )
        f.values[member] = [D(value)]
    return f


def nine_month_filing(id: int, filed: date, members: dict[str, int]) -> Filing:
    return Filing(id, filed, values={m: [D(v)] for m, v in members.items()})


QUARTERS = {
    "A": FirstThree(D(60), date(2023, 11, 1)),
    "B": FirstThree(D(30), date(2023, 11, 1)),
}


def test_the_years_own_10k_ranks_before_a_later_recast() -> None:
    own = annual_filing(10, date(2024, 2, 1), {"A": 80, "B": 40})
    recast = annual_filing(20, date(2025, 2, 1), {"A": 70, "C": 50})
    ranked = rank_annual_filings(10, [recast, own], QUARTERS)
    assert [f.filing_id for f in ranked] == [10, 20]


def test_an_amendment_ranks_first() -> None:
    own = annual_filing(10, date(2024, 2, 1), {"A": 80, "B": 40})
    amended = annual_filing(11, date(2024, 3, 1), {"A": 80, "B": 40}, is_amendment=True)
    assert rank_annual_filings(10, [own, amended], QUARTERS)[0].filing_id == 11


def test_a_year_without_quarters_leaves_the_split_test_out() -> None:
    """Neither 10-K can be on a split no quarter was filed on; filing date decides."""
    early = annual_filing(10, date(2024, 2, 1), {"A": 80})
    late = annual_filing(20, date(2025, 2, 1), {"B": 0})
    ranked = rank_annual_filings(30, [early, late], {})
    assert [f.filing_id for f in ranked] == [20, 10]


def test_a_10k_pairs_with_the_10q_on_its_members() -> None:
    annual = annual_filing(10, date(2024, 2, 1), {"A": 80, "B": 40})
    same = nine_month_filing(5, date(2023, 11, 1), {"A": 60, "B": 30})
    other = nine_month_filing(6, date(2023, 11, 1), {"A": 60})
    assert nine_month_source(annual, [annual], [other, same]) is same


def test_only_the_first_ranked_10k_uses_nine_months_without_quarters() -> None:
    first = annual_filing(10, date(2024, 2, 1), {"C": 50})
    second = annual_filing(20, date(2025, 2, 1), {"D": 90})
    nines = nine_month_filing(5, date(2024, 11, 1), {"C": 35, "D": 60})
    sources = [Q4Source(first, 1, nines), Q4Source(second, 2, nines)]
    assert list(member_q4(sources, {})) == [(100, D(35))]
