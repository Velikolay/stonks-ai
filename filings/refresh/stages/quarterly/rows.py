"""The rows quarterly derivation starts from, tagged with their fiscal year.

Every 10-Q is tagged with the 10-K that closes its fiscal year (the first
10-K filed after it); a 10-K is tagged with itself. Q4 is derived per tag.
"""

from __future__ import annotations

import polars as pl

from ...inputs import CompanyData
from ...rules import sql
from ...rules.periods import ANNUAL_FORMS, QUARTERLY_FORMS, fiscal_year_of

SERIES = ["company_id", "statement", "normalized_label", "axis", "member"]
#: Weighted-average share counts: durations, but they do not add up.
WEIGHTED_AVERAGE_SHARES = (
    "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
    "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
)


def is_per_share(unit: pl.Expr) -> pl.Expr:
    return unit.fill_null("").str.contains(r"(?i)pershare$")


def is_non_additive() -> pl.Expr:
    """Stocks, share counts: never differenced across sub-periods.

    A company extension of the period-end count is still a balance. Per-share
    amounts are not additive either, but a filing that only stored the
    year-to-date column still reduces on the differencing path.
    """
    concept = pl.col("concept")
    return (
        pl.col("period").is_null()
        | concept.is_in(WEIGHTED_AVERAGE_SHARES)
        | (
            (pl.col("unit").fill_null("") == "shares")
            & concept.str.contains(r"(?i)SharesOutstanding")
            & ~concept.str.contains(r"(?i)WeightedAverage")
        )
    )


def fiscal_tags(filings: pl.DataFrame) -> pl.DataFrame:
    """10-K and 10-Q filings with the 10-K that closes their fiscal year."""
    ordered = sql.sort(
        filings.filter(pl.col("form_type").is_in([*ANNUAL_FORMS, *QUARTERLY_FORMS])),
        ["company_id", "fiscal_period_end", "id"],
    )
    annual_id = pl.when(pl.col("form_type") == "10-K").then(pl.col("id"))
    next_annual = annual_id.shift(-1).backward_fill().over("company_id")
    return ordered.with_columns(
        pl.when(pl.col("form_type") == "10-K")
        .then(pl.col("id"))
        .otherwise(next_annual)
        .alias("fiscal_tag")
    ).select(
        "id",
        "company_id",
        "fiscal_period_end",
        "fiscal_year",
        "fiscal_quarter",
        "fiscal_tag",
    )


def tagged_rows(data: CompanyData, tags: pl.DataFrame) -> pl.DataFrame:
    """Non-duplicate 10-K and 10-Q rows placed in the fiscal period that closed them.

    A row for an earlier period printed in a later filing gets no tag when
    its own filing was never ingested: that year's 10-K is another year, so
    the row stays out of the Q4 residual. A series takes its header and
    position from its latest period; headers and totals that share a label
    are separate series.
    """
    rows = data.table("financial_facts_normalized").filter(~pl.col("is_duplicate"))
    joined = rows.join(
        tags.select(
            pl.col("id").alias("fiscal_filing_id"),
            pl.col("fiscal_period_end").alias("filing_period_end"),
            pl.col("fiscal_year").alias("filing_fiscal_year"),
            "fiscal_quarter",
            pl.col("fiscal_tag").alias("filing_fiscal_tag"),
        ),
        on="fiscal_filing_id",
        how="inner",
    )
    ordered = sql.sort(joined, [sql.desc("period_end"), sql.asc("id")])
    tree = [*SERIES, "is_abstract"]
    return ordered.with_columns(
        fiscal_year_of(
            pl.col("filing_fiscal_year"),
            pl.col("filing_period_end"),
            pl.col("period_end"),
        ).alias("fiscal_year"),
        pl.when(pl.col("filing_period_end") == pl.col("period_end"))
        .then(pl.col("filing_fiscal_tag"))
        .alias("fiscal_tag"),
        pl.col("form_type").alias("source_type"),
        pl.col("abstract_id").first().over(tree).alias("abstract_id"),
        pl.col("position").first().over(tree).alias("position"),
    )


def quarter_rows(tagged: pl.DataFrame) -> pl.DataFrame:
    """10-Q rows, the raw material of the quarterly table."""
    return tagged.filter(pl.col("source_type").is_in(QUARTERLY_FORMS)).with_columns(
        pl.lit(False).alias("is_comparative_backfill")
    )


def annual_rows(tagged: pl.DataFrame) -> pl.DataFrame:
    """10-K rows: point-in-time lines pass through as Q4; flows give Q4 by subtraction."""
    return tagged.filter(pl.col("source_type").is_in(ANNUAL_FORMS))


def ytd_anchors(data: CompanyData) -> pl.DataFrame:
    """Year-to-date totals dedup hid behind a published quarter of the same date.

    It is a duplicate of that quarter, so it is not shown; Q4 still subtracts
    it from the annual figure. It must be the same line of the same filing,
    since one filing can print two lines under one label.
    """
    keys = [*SERIES, "period_end", "filing_id", "concept"]
    ytd = data.table("financial_facts_normalized").filter(
        pl.col("is_duplicate") & (pl.col("period") == "YTD") & ~pl.col("is_abstract")
    )
    return sql.distinct_on(ytd, keys, [sql.desc("id")]).select(
        *keys, pl.col("value").alias("anchor_value")
    )
