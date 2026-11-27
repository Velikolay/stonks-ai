"""Periods before the first filing, built from the earliest filings' comparatives."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Optional

import polars as pl

from filings.refresh.rules.prior_periods import attached_early_quarters
from filings.refresh.stages import facts_normalized, facts_overridden, quarterly, yearly

from .builders import company, frame

D = Decimal
ENDS = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}


def filing(year: int, quarter: int, form: Optional[str] = None, **extra: Any) -> dict:
    """The filing for ``year``/``quarter``; ids are ``year * 10 + quarter``."""
    return {
        "id": extra.pop("id", year * 10 + quarter),
        "filing_entity_id": 1,
        "registry": "SEC",
        "number": str(year * 10 + quarter),
        "form_type": form or ("10-K" if quarter == 4 else "10-Q"),
        "filing_date": extra.pop(
            "filed", f"{year + 1}-02-15" if quarter == 4 else f"{year}-{ENDS[quarter]}"
        ),
        "fiscal_period_end": f"{year}-{ENDS[quarter]}",
        "fiscal_year": year,
        "fiscal_quarter": quarter,
        **extra,
    }


def fact(
    id: int,
    filing: int,
    year: int,
    quarter: int,
    value: int,
    comparative: Optional[int],
    period: Optional[str] = "YTD",
    **extra: Any,
) -> dict:
    """A fact for the filing's own period, with its comparative a year back."""
    end = f"{year}-{ENDS[quarter]}"
    return {
        "id": id,
        "filing_id": filing,
        "form_type": extra.pop("form_type", "10-K" if quarter == 4 else "10-Q"),
        "concept": extra.pop("concept", "us-gaap:Revenues"),
        "label": extra.pop("label", "Revenue"),
        "value": value,
        "comparative_value": comparative,
        "period_end": end,
        "comparative_period_end": (
            f"{year - 1}-{ENDS[quarter]}" if comparative is not None else None
        ),
        "period": period,
        **extra,
    }


def run(**tables: Any) -> Any:
    data = company(**tables)
    data.put("financial_facts_overridden", facts_overridden.run(data))
    data.put("financial_facts_normalized", facts_normalized.run(data))
    return data


def revenue(frame: pl.DataFrame, label: str = "Revenue") -> list[tuple]:
    rows = frame.filter(
        (pl.col("normalized_label") == label)
        & (pl.col("axis") == "")
        & ~pl.col("is_abstract")
    )
    return (
        rows.sort("period_end")
        .select(pl.col("period_end").cast(pl.String), "value")
        .rows()
    )


def first_year_filed_from_q1() -> list[dict]:
    """Quarterly 10-Qs of 2010 and its 10-K, each printing 2009 beside it."""
    return [
        fact(1, 20101, 2010, 1, 30, 20, period="Q"),
        fact(2, 20102, 2010, 2, 31, 21, period="Q"),
        fact(3, 20102, 2010, 2, 61, 41),
        fact(4, 20103, 2010, 3, 32, 22, period="Q"),
        fact(5, 20103, 2010, 3, 93, 63),
        fact(6, 20104, 2010, 4, 130, 90),
    ]


def test_the_first_10k_comparative_is_the_prior_year() -> None:
    """The 10-K's prior-year column is placed in its own fiscal year."""
    data = run(
        filings=[filing(2010, 4)],
        financial_facts=[
            fact(1, 20104, 2010, 4, 130, 90, parent_id=2),
            fact(
                2,
                20104,
                2010,
                4,
                150,
                100,
                concept="us-gaap:GrossProfit",
                label="Gross",
            ),
        ],
    )
    out = yearly.run(data)
    assert revenue(out) == [("2009-12-31", D(90)), ("2010-12-31", D(130))]
    prior = out.filter(pl.col("period_end") == date(2009, 12, 31))
    assert set(prior["fiscal_year"]) == {2009}
    assert set(prior["filing_id"]) == {-20104}
    # The copy hangs below the copy of its parent, not the 2010 row.
    by_label = {r["normalized_label"]: r for r in prior.iter_rows(named=True)}
    assert by_label["Revenue"]["parent_id"] == by_label["Gross"]["id"]


def test_a_year_of_prior_quarters_closes_on_the_prior_10k() -> None:
    """A company first filing in Q1 gets the year before: Q4 is the 10-K less nine months."""
    data = run(
        filings=[filing(2010, q) for q in (1, 2, 3, 4)],
        financial_facts=first_year_filed_from_q1(),
    )
    assert revenue(quarterly.run(data)) == [
        ("2009-03-31", D(20)),
        ("2009-06-30", D(21)),
        ("2009-09-30", D(22)),
        ("2009-12-31", D(90 - 63)),
        ("2010-03-31", D(30)),
        ("2010-06-30", D(31)),
        ("2010-09-30", D(32)),
        ("2010-12-31", D(130 - 93)),
    ]


def test_a_comparative_that_is_not_a_year_back_is_no_prior_period() -> None:
    late = fact(1, 20104, 2010, 4, 130, 90)
    late["comparative_period_end"] = "2010-06-30"
    data = run(filings=[filing(2010, 4)], financial_facts=[late])
    assert revenue(yearly.run(data)) == [("2010-12-31", D(130))]


def first_10q_in_q2(q1_reprint: int) -> list[dict]:
    """2010's first 10-Q is the second quarter; 2011's first quarter reprints Q1."""
    return [
        fact(2, 20102, 2010, 2, 31, None, period="Q"),
        fact(3, 20102, 2010, 2, 61, None),
        fact(4, 20103, 2010, 3, 32, None, period="Q"),
        fact(5, 20103, 2010, 3, 93, None),
        fact(6, 20104, 2010, 4, 130, None),
        fact(7, 20111, 2011, 1, 40, q1_reprint, period="Q"),
    ]


def q2_filer(q1_reprint: int) -> Any:
    return run(
        filings=[filing(2010, q) for q in (2, 3, 4)] + [filing(2011, 1)],
        financial_facts=first_10q_in_q2(q1_reprint),
    )


def test_a_prior_quarter_that_agrees_with_the_year_completes_it() -> None:
    out = quarterly.run(q2_filer(30))
    assert revenue(out)[:4] == [
        ("2010-03-31", D(30)),
        ("2010-06-30", D(31)),
        ("2010-09-30", D(32)),
        ("2010-12-31", D(37)),
    ]


def test_a_prior_quarter_on_another_basis_is_left_out() -> None:
    """Next year reprinted the quarter at 45; the year's own 10-Qs imply 30."""
    out = quarterly.run(q2_filer(45))
    assert revenue(out)[0] == ("2010-06-30", D(31))


def test_a_prior_quarter_rounded_coarser_takes_the_years_digits() -> None:
    """A reprint in millions keeps the year's own digits (30.4 from 61.4 - 31)."""
    facts = first_10q_in_q2(30_000_000)
    facts[0]["value"] = D(31_000_000)
    facts[1]["value"] = D(61_400_000)
    facts[2]["value"] = D(32_000_000)
    facts[3]["value"] = D(93_400_000)
    facts[4]["value"] = D(130_000_000)
    facts[5]["value"] = D(40_000_000)
    data = run(
        filings=[filing(2010, q) for q in (2, 3, 4)] + [filing(2011, 1)],
        financial_facts=facts,
    )
    assert revenue(quarterly.run(data))[0] == ("2010-03-31", D(30_400_000))


def test_quarters_printed_before_a_restating_amendment_stay_out() -> None:
    """The 10-K/A restated 2009 to 100, so the 10-Qs' reprints of 2009 are on the old basis.

    Without the rule, 2009's Q4 would be 100 less nine months filed before the
    restatement.
    """
    facts = [
        *first_year_filed_from_q1(),
        fact(7, 2010400, 2010, 4, 140, 100, form_type="10-K/A"),
    ]
    data = run(
        filings=[filing(2010, q) for q in (1, 2, 3, 4)]
        + [filing(2010, 4, "10-K/A", id=2010400, filed="2011-05-01")],
        financial_facts=facts,
    )
    assert revenue(yearly.run(data))[0] == ("2009-12-31", D(100))
    early = quarterly.run(data).filter(pl.col("period_end") < date(2010, 1, 1))
    assert early.is_empty()


def test_a_line_printed_twice_in_one_10k_is_no_restatement() -> None:
    """Cash at the start and end of the year share a concept (Microsoft's FY2010 10-K)."""
    cash = dict(concept="us-gaap:Cash", label="Cash", statement="Cash Flow Statement")
    facts = [
        *first_year_filed_from_q1(),
        fact(8, 20104, 2010, 4, 500, 400, period=None, **cash),
        fact(9, 20104, 2010, 4, 600, 450, period=None, **cash),
    ]
    data = run(filings=[filing(2010, q) for q in (1, 2, 3, 4)], financial_facts=facts)
    assert revenue(quarterly.run(data))[0] == ("2009-03-31", D(20))


def test_an_amendment_that_corrects_a_share_count_keeps_the_quarters() -> None:
    """Tesla's FY2011 10-K/A changed only the options exercised in 2010."""
    shares = dict(concept="us-gaap:Options", label="Options", unit="shares")
    facts = [
        *first_year_filed_from_q1(),
        fact(7, 2010400, 2010, 4, 130, 90, form_type="10-K/A"),
        fact(8, 20104, 2010, 4, 5, 721, **shares),
        fact(9, 2010400, 2010, 4, 5, 712, form_type="10-K/A", **shares),
    ]
    data = run(
        filings=[filing(2010, q) for q in (1, 2, 3, 4)]
        + [filing(2010, 4, "10-K/A", id=2010400, filed="2011-05-01")],
        financial_facts=facts,
    )
    assert revenue(quarterly.run(data))[0] == ("2009-03-31", D(20))


def test_an_amendment_that_restated_nothing_keeps_the_quarters() -> None:
    facts = [
        *first_year_filed_from_q1(),
        fact(7, 2010400, 2010, 4, 130, 90, form_type="10-K/A"),
    ]
    data = run(
        filings=[filing(2010, q) for q in (1, 2, 3, 4)]
        + [filing(2010, 4, "10-K/A", id=2010400, filed="2011-05-01")],
        financial_facts=facts,
    )
    assert revenue(quarterly.run(data))[3] == ("2009-12-31", D(27))


def test_a_balance_without_the_next_quarter_is_not_kept() -> None:
    """The prior 10-K's balance sheet is Q4 only when the first filing is Q1."""
    data = run(
        filings=[filing(2010, 2), filing(2010, 4)],
        financial_facts=[
            fact(1, 20102, 2010, 2, 500, None, statement="Balance Sheet", period=None),
            fact(2, 20104, 2010, 4, 600, 400, statement="Balance Sheet", period=None),
        ],
    )
    assert revenue(yearly.run(data))[0] == ("2009-12-31", D(400))
    assert revenue(quarterly.run(data)) == [
        ("2010-06-30", D(500)),
        ("2010-12-31", D(600)),
    ]


# --- The gap filter ------------------------------------------------------------


def quarter_row(id: int, year: int, quarter: int, **extra: Any) -> dict:
    return {
        "id": id,
        "filing_id": extra.pop("filing_id", year * 10 + quarter),
        "concept": "us-gaap:Revenues",
        "label": "Revenue",
        "normalized_label": "Revenue",
        "value": 1,
        "period_end": f"{year}-{extra.pop('end', ENDS[quarter])}",
        "fiscal_year": year,
        "fiscal_quarter": quarter,
        "source_type": "10-Q",
        **extra,
    }


def test_early_quarters_are_counted_not_measured_in_days() -> None:
    """A 16-week fourth quarter still follows the third (Costco)."""
    rows = frame(
        "quarterly_financials",
        [
            quarter_row(1, 2009, 3, end="05-10", filing_id=-1),
            quarter_row(2, 2009, 4, end="08-30", filing_id=-2),
            quarter_row(3, 2010, 1, end="11-22"),
        ],
    )
    filings = frame("filings", [filing(2010, 1) | {"fiscal_period_end": "2010-11-22"}])
    kept = attached_early_quarters(rows, filings, ["company_id", "normalized_label"])
    assert sorted(kept["id"]) == [1, 2, 3]


def test_an_early_quarter_behind_a_gap_is_dropped() -> None:
    rows = frame(
        "quarterly_financials",
        [
            quarter_row(1, 2009, 2, filing_id=-1),
            quarter_row(2, 2009, 4, filing_id=-2),
            quarter_row(3, 2010, 1),
        ],
    )
    filings = frame("filings", [filing(2010, 1)])
    kept = attached_early_quarters(rows, filings, ["company_id", "normalized_label"])
    assert sorted(kept["id"]) == [2, 3]
