"""Year-to-date totals reduced to discrete quarters.

A YTD figure becomes a quarter by subtracting the previous quarter of the
series when that quarter ended 80 to 100 days earlier, or that quarter's
own YTD total when it was published beside it. A YTD with no earlier
quarter is the opening of a series and is not shown, unless next year's
filing reports the missing period as a comparative: that comparative is
added as a backfill row so the opening YTD reduces to one quarter, and the
backfill itself is filtered out.
"""

from __future__ import annotations

import polars as pl

from ...rules import sql
from ...rules.ids import abs_hash_expr
from ...rules.periods import QUARTER_GAP_DAYS, days_between, quarter_apart, within
from .rows import SERIES, is_non_additive, is_per_share


def comparative_backfill(quarters: pl.DataFrame) -> pl.DataFrame:
    """The period before each YTD-opened series, from a later comparative."""
    starts = sql.distinct_on(
        quarters, SERIES, [sql.asc("period_end"), sql.asc("is_abstract"), sql.asc("id")]
    ).filter(pl.col("period") == "YTD")
    later = quarters.filter(pl.col("comparative_value").is_not_null()).select(
        *SERIES,
        pl.col("filing_id").alias("n_filing_id"),
        pl.col("fiscal_quarter").alias("n_fiscal_quarter"),
        pl.col("comparative_value").alias("n_comparative_value"),
        pl.col("comparative_period_end").alias("n_comparative_period_end"),
        pl.col("period").alias("n_period"),
        pl.col("period_end").alias("n_period_end"),
        pl.col("id").alias("n_id"),
    )
    pairs = starts.join(later, on=SERIES).filter(
        within(
            days_between(pl.col("period_end"), pl.col("n_comparative_period_end")),
            QUARTER_GAP_DAYS,
        )
    )
    first = sql.distinct_on(pairs, SERIES, [sql.asc("n_period_end"), sql.asc("n_id")])
    return first.with_columns(
        pl.col("n_filing_id").alias("filing_id"),
        abs_hash_expr(
            "quarterly-prior",
            pl.col("company_id"),
            pl.col("statement"),
            pl.col("normalized_label"),
            pl.col("axis"),
            pl.col("member"),
            pl.col("n_comparative_period_end"),
        ).alias("id"),
        pl.col("n_fiscal_quarter").alias("fiscal_quarter"),
        pl.col("n_comparative_value").alias("value"),
        pl.col("n_comparative_period_end").alias("period_end"),
        pl.col("n_period").alias("period"),
        pl.lit(None, pl.Date).alias("comparative_period_end"),
        pl.lit(None, pl.Decimal(38, 10)).alias("comparative_value"),
        pl.lit(False).alias("is_abstract"),
        pl.lit(True).alias("is_synthetic"),
        pl.lit("calculated").alias("source_type"),
        pl.lit(True).alias("is_comparative_backfill"),
    ).select(quarters.columns)


def discrete_quarters(
    quarters: pl.DataFrame,
    backfill: pl.DataFrame,
    anchors: pl.DataFrame,
    filings: pl.DataFrame,
) -> pl.DataFrame:
    """Every 10-Q row with its YTD reduced, its drop flag and its nine-month total.

    Added columns: ``is_non_additive``, ``drop_ytd`` (an unreduced YTD that
    is not shown), ``q3_ytd`` (the nine months before the 10-K, kept for Q4)
    and ``prefer_ytd_anchor`` (a published Q3 whose nine-month total exists).
    """
    keys = [*SERIES, "period_end", "filing_id", "concept"]
    annual = filings.select(
        pl.col("id").alias("fiscal_tag"),
        pl.col("fiscal_period_end").alias("annual_period_end"),
    )
    amounts = pl.concat([quarters.filter(~pl.col("is_abstract")), backfill])
    amounts = amounts.join(anchors, on=keys, how="left").join(
        annual, on="fiscal_tag", how="left"
    )
    prev_anchor = pl.when(pl.col("period") == "Q").then(pl.col("anchor_value"))
    ordered = sql.sort(amounts, ["period_end"])
    prev_period = pl.col("period_end").shift(1).over(SERIES)
    reduced = ordered.with_columns(
        is_non_additive().alias("is_non_additive"),
        prev_period.alias("prev_period_end"),
        pl.when(quarter_apart(pl.col("period_end"), prev_period))
        .then(pl.coalesce(prev_anchor, pl.col("value")).shift(1).over(SERIES))
        .alias("prev_value"),
    )

    headers = quarters.filter(pl.col("is_abstract")).with_columns(
        pl.lit(None, pl.Decimal(38, 10)).alias("anchor_value"),
        pl.lit(None, pl.Date).alias("annual_period_end"),
        pl.lit(False).alias("is_non_additive"),
        pl.lit(None, pl.Date).alias("prev_period_end"),
        pl.lit(None, pl.Decimal(38, 10)).alias("prev_value"),
    )
    rows = pl.concat([reduced, headers.select(reduced.columns)])

    additive = ~pl.col("is_non_additive")
    ytd = pl.col("period") == "YTD"
    before_annual = pl.col("annual_period_end").is_not_null() & quarter_apart(
        pl.col("annual_period_end"), pl.col("period_end")
    )
    counts_for_q4 = additive & ~is_per_share(pl.col("unit")) & before_annual
    published_q3 = (
        (pl.col("period") == "Q") & pl.col("anchor_value").is_not_null() & counts_for_q4
    )
    return rows.with_columns(
        pl.when(ytd & additive & pl.col("prev_value").is_not_null())
        .then(pl.col("value") - pl.col("prev_value"))
        .otherwise(pl.col("value"))
        .alias("value"),
        # An unreduced YTD is still a six- or nine-month total: the opening
        # of a series, or a Q2/Q3 whose prior quarter is missing. An
        # unreduced Q1 is that quarter.
        (
            ytd
            & additive
            & (
                pl.col("prev_period_end").is_null()
                | (
                    pl.col("prev_value").is_null()
                    & pl.col("fiscal_quarter").is_in([2, 3])
                )
            )
        ).alias("drop_ytd"),
        pl.when(published_q3)
        .then(pl.col("anchor_value"))
        .when(ytd & counts_for_q4)
        .then(pl.col("value"))
        .alias("q3_ytd"),
        published_q3.alias("prefer_ytd_anchor"),
    )
