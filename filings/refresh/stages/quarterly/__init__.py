"""Stage 7: the quarterly presentation layer.

Reads ``financial_facts_normalized``, ``filings`` and ``financial_rewrites``;
writes ``quarterly_financials``.

1. :mod:`.rows`: 10-K and 10-Q rows tagged with the 10-K closing their year.
2. :mod:`.ytd`: year-to-date totals reduced to discrete quarters.
3. :mod:`.restatement`: a year whose quarters overshoot its 10-K moves onto
   next year's comparatives.
4. :mod:`.q4`: Q4 as the 10-K minus Q1-Q3, one 10-K per axis and year.

Balance-sheet lines, share counts and other point-in-time amounts come from
the 10-K as Q4 instead of being summed. Quarterly rewrites apply last.
"""

from __future__ import annotations

import polars as pl

from ...inputs import CompanyData
from ...io.schemas import QUARTERLY_FINANCIALS
from ...rules.rewrites import apply_rewrites
from .q4 import (
    annual_members,
    annual_split,
    line_q4,
    member_q4,
    nine_month_split,
    nine_months,
    quarters_by_year,
)
from .restatement import (
    comparative_basis,
    comparative_quarters,
    quarter_basis,
    quarter_pairs,
    with_restated_values,
)
from .rows import (
    WEIGHTED_AVERAGE_SHARES,
    annual_rows,
    fiscal_tags,
    quarter_rows,
    tagged_rows,
    ytd_anchors,
)
from .ytd import comparative_backfill, discrete_quarters

COLUMNS = QUARTERLY_FINANCIALS.columns


def shown_quarters(restated: pl.DataFrame) -> pl.DataFrame:
    """The 10-Q rows that are quarters, restated where a restatement applies.

    An unreduced YTD is not shown; a header always is, so lines have a row
    to point at. Backfilled comparatives only fed the differencing.
    """
    restated_flag = pl.col("restated_value").is_not_null()
    return restated.filter(
        (~pl.col("drop_ytd") | pl.col("is_abstract"))
        & ~pl.col("is_comparative_backfill")
    ).with_columns(
        pl.coalesce("restated_value", "value").alias("value"),
        pl.when(restated_flag)
        .then(pl.lit("calculated"))
        .otherwise(pl.col("source_type"))
        .alias("source_type"),
    )


def point_in_time(annual: pl.DataFrame) -> pl.DataFrame:
    """10-K rows that pass through as Q4: balances, share counts and headers.

    A header filed only on the 10-K is kept because lines point at it.
    Year-start balances ("beginning ...") are not Q4 and stay out.
    """
    beginning = pl.col("normalized_label").fill_null("").str.contains(
        r"(?i)beginning"
    ) | pl.col("label").fill_null("").str.contains(r"(?i)beginning")
    return annual.filter(
        (pl.col("statement") == "Balance Sheet")
        | pl.col("is_abstract")
        | pl.col("concept").is_in(WEIGHTED_AVERAGE_SHARES)
        | (pl.col("period").is_null() & ~beginning)
    )


def run(data: CompanyData) -> pl.DataFrame:
    filings = data.table("filings")
    normalized = data.table("financial_facts_normalized")
    tags = fiscal_tags(filings)
    tagged = tagged_rows(data, tags)
    quarters = quarter_rows(tagged)
    annual = annual_rows(tagged)

    backfill = comparative_backfill(quarters)
    discrete = data.keep(
        "discrete", discrete_quarters(quarters, backfill, ytd_anchors(data), filings)
    )
    pairs = quarter_pairs(discrete, comparative_quarters(quarters))
    moved = data.keep("restated_years", comparative_basis(quarter_basis(pairs, tagged)))
    restated = with_restated_values(discrete, pairs, moved)

    by_year = data.keep("quarters_by_year", quarters_by_year(restated, filings))
    members = annual_members(normalized, filings)
    nines = nine_months(normalized, filings, tags)
    splits = data.keep("annual_split", annual_split(members, by_year))
    nine_split = data.keep("nine_month_split", nine_month_split(members, nines))

    frames = [
        shown_quarters(restated),
        point_in_time(annual),
        member_q4(members, by_year, splits, nine_split, nines),
        line_q4(annual, by_year),
    ]
    result = pl.concat([QUARTERLY_FINANCIALS.conform(f) for f in frames])
    return apply_rewrites(
        result, data.table("financial_rewrites"), "quarterly", data.company_id
    )
