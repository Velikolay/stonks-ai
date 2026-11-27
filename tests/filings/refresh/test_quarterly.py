"""Quarterly derivation: YTD differencing, Q4, backfill, restatement, rewrites."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Optional

import polars as pl

from filings.refresh.stages import quarterly, yearly

from .builders import company

D = Decimal
QUARTER_ENDS = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}


def filings(*years: int) -> list[dict]:
    """A 10-Q per quarter and a 10-K per year; ids are ``year * 10 + quarter``."""
    out = []
    for year in years:
        for q, end in QUARTER_ENDS.items():
            out.append(
                {
                    "id": year * 10 + q,
                    "filing_entity_id": 1,
                    "registry": "SEC",
                    "number": f"{year}-{q}",
                    "form_type": "10-K" if q == 4 else "10-Q",
                    "filing_date": f"{year + (q == 4)}-{'02-15' if q == 4 else '11-01' if q == 3 else end}",
                    "fiscal_period_end": f"{year}-{end}",
                    "fiscal_year": year,
                    "fiscal_quarter": q,
                }
            )
    return out


def row(
    id: int,
    year: int,
    quarter: int,
    value: Optional[int],
    period: Optional[str] = "YTD",
    filed_in: Optional[tuple[int, int]] = None,
    **extra: Any,
) -> dict:
    """A normalized row for ``year``/``quarter``, filed in its own period by default."""
    fy, fq = filed_in or (year, quarter)
    filing = fy * 10 + fq
    return {
        "id": id,
        "filing_id": filing,
        "fiscal_filing_id": year * 10 + quarter,
        "form_type": "10-K" if fq == 4 else "10-Q",
        "concept": extra.pop("concept", "us-gaap:Revenues"),
        "label": "Revenue",
        "normalized_label": extra.pop("normalized_label", "Revenue"),
        "value": value,
        "period_end": f"{year}-{QUARTER_ENDS[quarter]}",
        "period": period,
        **extra,
    }


def series(frame: pl.DataFrame, label: str = "Revenue") -> list[tuple]:
    rows = frame.filter((pl.col("normalized_label") == label) & ~pl.col("is_abstract"))
    return (
        rows.sort("period_end")
        .select(pl.col("period_end").cast(pl.String), "value", "source_type")
        .rows()
    )


def test_ytd_reduces_to_quarters_and_q4_is_the_remainder() -> None:
    """Filings tag the three-month column as a quarter, later ones as YTD."""
    data = company(
        filings=filings(2023),
        financial_facts_normalized=[
            row(1, 2023, 1, 100, period="Q"),
            row(2, 2023, 2, 250),
            row(3, 2023, 3, 400),
            row(4, 2023, 4, 600),
        ],
    )
    assert series(quarterly.run(data)) == [
        ("2023-03-31", D(100), "10-Q"),
        ("2023-06-30", D(150), "10-Q"),
        ("2023-09-30", D(150), "10-Q"),
        ("2023-12-31", D(200), "calculated"),
    ]


def test_published_quarters_beside_their_ytd_use_the_nine_months_for_q4() -> None:
    data = company(
        filings=filings(2023),
        financial_facts_normalized=[
            row(1, 2023, 1, 100, period="Q"),
            row(2, 2023, 2, 150, period="Q"),
            row(3, 2023, 3, 150, period="Q"),
            row(13, 2023, 3, 400, is_duplicate=True),
            row(4, 2023, 4, 640),
        ],
    )
    assert series(quarterly.run(data))[-1] == ("2023-12-31", D(240), "calculated")


def test_no_q4_without_the_first_three_quarters() -> None:
    """A member tagged only in Q1 and the 10-K is left without a Q4."""
    member = {"axis": "Geographical", "member": "Japan"}
    data = company(
        filings=filings(2023),
        financial_facts_normalized=[
            row(1, 2023, 1, 10, **member),
            row(4, 2023, 4, 90, **member),
        ],
    )
    out = quarterly.run(data)
    assert out.filter(pl.col("source_type") == "calculated").is_empty()


def test_balance_sheet_lines_pass_through_from_the_10k() -> None:
    bs = {
        "statement": "Balance Sheet",
        "concept": "us-gaap:Assets",
        "normalized_label": "Total Assets",
    }
    data = company(
        filings=filings(2023),
        financial_facts_normalized=[
            row(1, 2023, 1, 500, period=None, **bs),
            row(4, 2023, 4, 800, period=None, **bs),
        ],
    )
    assert series(quarterly.run(data), "Total Assets") == [
        ("2023-03-31", D(500), "10-Q"),
        ("2023-12-31", D(800), "10-K"),
    ]


def test_opening_ytd_reduces_with_next_years_comparative() -> None:
    """A series that starts at a six-month YTD is not shown, unless next year's
    filing prints the missing quarter as a comparative."""
    data = company(
        filings=filings(2023, 2024),
        financial_facts_normalized=[
            row(2, 2023, 2, 250),
            row(
                21,
                2024,
                1,
                120,
                comparative_value=100,
                comparative_period_end="2023-03-31",
            ),
        ],
    )
    values = dict((p, v) for p, v, _ in series(quarterly.run(data)))
    assert values["2023-06-30"] == D(150)
    assert "2023-03-31" not in values


def test_overshooting_quarters_move_onto_next_years_comparatives() -> None:
    """Own Q1-Q3 sum past the 10-K by more than 5%; the comparatives fit."""
    data = company(
        filings=filings(2023, 2024),
        financial_facts_normalized=[
            row(1, 2023, 1, 400, period="Q"),
            row(2, 2023, 2, 400, period="Q"),
            row(3, 2023, 3, 400, period="Q"),
            row(4, 2023, 4, 1000),
            *(
                row(
                    20 + q,
                    2024,
                    q,
                    300,
                    period="Q",
                    comparative_value=200,
                    comparative_period_end=f"2023-{QUARTER_ENDS[q]}",
                )
                for q in (1, 2, 3)
            ),
        ],
    )
    values = {
        p: (v, s) for p, v, s in series(quarterly.run(data)) if p.startswith("2023")
    }
    assert values["2023-03-31"] == (D(200), "calculated")
    assert values["2023-12-31"] == (D(400), "calculated")


def test_members_follow_a_moved_year_onto_the_reprint_that_adds_up() -> None:
    """The line's 2023 quarters move onto 2024's comparatives; so do its segments.

    The filed segments include a business the 10-K later discontinued (Old,
    200 a quarter). Next year's 10-Qs reprint each 2023 quarter without it,
    A 120 + B 80 = 200, the restated line, so those members replace the
    filed ones, and Q4 is the 10-K's members less the three restated quarters.
    """
    seg = {"axis": "Segment"}
    rows = [
        row(1, 2023, 1, 400, period="Q"),
        row(2, 2023, 2, 400, period="Q"),
        row(3, 2023, 3, 400, period="Q"),
        row(4, 2023, 4, 1000),
        row(5, 2023, 4, 600, member="A", **seg),
        row(6, 2023, 4, 400, member="B", **seg),
    ]
    for q in (1, 2, 3):
        end = f"2023-{QUARTER_ENDS[q]}"
        rows += [
            row(10 + q, 2023, q, 120, period="Q", member="A", **seg),
            row(20 + q, 2023, q, 80, period="Q", member="B", **seg),
            row(30 + q, 2023, q, 200, period="Q", member="Old", **seg),
            row(
                40 + q,
                2024,
                q,
                300,
                period="Q",
                comparative_value=200,
                comparative_period_end=end,
            ),
            row(
                50 + q,
                2023,
                q,
                120,
                period="Q",
                member="A",
                filed_in=(2024, q),
                is_duplicate=True,
                **seg,
            ),
            row(
                60 + q,
                2023,
                q,
                80,
                period="Q",
                member="B",
                filed_in=(2024, q),
                is_duplicate=True,
                **seg,
            ),
        ]
    data = company(filings=filings(2023, 2024), financial_facts_normalized=rows)
    out = quarterly.run(data).filter(pl.col("axis") == "Segment")
    got = {
        (r["member"], str(r["period_end"])): r["value"]
        for r in out.iter_rows(named=True)
        if str(r["period_end"]).startswith("2023")
    }
    assert ("Old", "2023-03-31") not in got
    assert got[("A", "2023-06-30")] == D(120)
    assert got[("B", "2023-09-30")] == D(80)
    assert got[("A", "2023-12-31")] == D(600 - 360)
    assert got[("B", "2023-12-31")] == D(400 - 240)


def test_quarterly_rewrites_delete_set_and_add() -> None:
    member = {"axis": "Geographical"}
    data = company(
        filings=filings(2023),
        financial_facts_normalized=[
            row(1, 2023, 1, 70, period="Q", member="US", **member),
            row(2, 2023, 1, 30, period="Q", member="Other", **member),
        ],
        financial_rewrites=[
            {
                "id": 1,
                "statement": "Income Statement",
                "normalized_label": "Revenue",
                "axis": "Geographical",
                "member": "Other",
                "period_end": "2023-03-31",
                "granularity": "quarterly",
                "action": "delete",
            },
            {
                "id": 2,
                "statement": "Income Statement",
                "normalized_label": "Revenue",
                "axis": "Geographical",
                "member": "US",
                "period_end": "2023-03-31",
                "granularity": "quarterly",
                "action": "set",
                "value": 71,
            },
            {
                "id": 3,
                "statement": "Income Statement",
                "normalized_label": "Revenue",
                "axis": "Geographical",
                "member": "Japan",
                "period_end": "2023-03-31",
                "granularity": "quarterly",
                "action": "add",
                "value": 5,
            },
        ],
    )
    out = quarterly.run(data)
    assert sorted(out.select("member", "value", "source_type").rows()) == [
        ("Japan", D(5), "calculated"),
        ("US", D(71), "calculated"),
    ]


def test_yearly_layer_keeps_annual_rows_in_their_fiscal_year() -> None:
    data = company(
        filings=filings(2022, 2023),
        financial_facts_normalized=[
            row(1, 2023, 4, 600),
            # 2022 printed as a comparative column of the 2023 10-K.
            row(2, 2022, 4, 500, filed_in=(2023, 4)),
        ],
    )
    out = yearly.run(data).sort("period_end")
    assert out.select("fiscal_year", "value").rows() == [(2022, D(500)), (2023, D(600))]
