"""Moving a year's quarters onto next year's comparatives.

When a line's own first three quarters overshoot its 10-K by more than 5%,
Q4 would reverse the year. If next year's 10-Q comparatives of those
quarters still fit under the 10-K, they replace the three quarters and Q4
becomes the 10-K minus them. Other undimensioned income-statement lines of
that year follow when next year's 10-K reprinted this year's figure
unchanged, so subtotals stay on one basis. Per-share amounts and share
counts stay on the filed quarters.
"""

from __future__ import annotations

from decimal import Decimal

import polars as pl

from ...rules import sql
from ...rules.periods import (
    ANNUAL_FORMS,
    YEAR_GAP_DAYS,
    days_between,
    quarter_apart,
    within,
)
from .rows import is_per_share

LINE = ["company_id", "statement", "normalized_label"]
#: Own quarters this far past the 10-K would reverse the year in Q4.
OVERSHOOT = Decimal("1.05")


def _income_lines() -> pl.Expr:
    """Undimensioned income-statement amounts: the lines a restatement moves."""
    return (
        (pl.col("statement") == "Income Statement")
        & (pl.col("axis") == "")
        & ~pl.col("is_abstract")
    )


def comparative_quarters(quarters: pl.DataFrame) -> pl.DataFrame:
    """This year's quarters as next year's 10-Qs printed them.

    A published quarter's comparative is that quarter; a year-to-date one
    subtracts the cumulative comparative of the quarter before it in the
    same filing year.
    """
    comparatives = quarters.filter(
        _income_lines()
        & pl.col("comparative_value").is_not_null()
        & within(
            days_between(pl.col("period_end"), pl.col("comparative_period_end")),
            YEAR_GAP_DAYS,
        )
    )
    per_period = (
        comparatives.group_by(
            *LINE, "fiscal_year", "fiscal_quarter", "comparative_period_end"
        )
        .agg(
            pl.col("comparative_value")
            .filter(pl.col("period") == "Q")
            .max()
            .alias("quarter_value"),
            pl.col("comparative_value")
            .filter(pl.col("period") == "YTD")
            .max()
            .alias("ytd_value"),
        )
        .rename({"comparative_period_end": "period_end"})
    )

    year = [*LINE, "fiscal_year"]
    ordered = sql.sort(per_period, ["period_end"])
    # A running SUM skips NULLs, and is NULL until the first value.
    running = pl.col("quarter_value").fill_null(0).cum_sum().over(year)
    seen = pl.col("quarter_value").is_not_null().cum_sum().over(year) > 0
    cumulative = ordered.with_columns(
        pl.coalesce("ytd_value", pl.when(seen).then(running)).alias("cumulative")
    )
    prev_period = pl.col("period_end").shift(1).over(year)
    return cumulative.select(
        *LINE,
        "period_end",
        pl.when(pl.col("quarter_value").is_not_null())
        .then(pl.col("quarter_value"))
        .when(pl.col("fiscal_quarter") == 1)
        .then(pl.col("cumulative"))
        .when(quarter_apart(pl.col("period_end"), prev_period))
        .then(pl.col("cumulative") - pl.col("cumulative").shift(1).over(year))
        .alias("value"),
    )


def quarter_pairs(discrete: pl.DataFrame, comparatives: pl.DataFrame) -> pl.DataFrame:
    """A line's own Q1-Q3 beside the comparative of the same quarter."""
    own = discrete.filter(
        _income_lines()
        & pl.col("fiscal_quarter").is_in([1, 2, 3])
        & pl.col("fiscal_tag").is_not_null()
        & pl.col("value").is_not_null()
        & ~pl.col("drop_ytd")
        & ~pl.col("is_comparative_backfill")
        & ~pl.col("is_non_additive")
        & ~is_per_share(pl.col("unit"))
    ).select(
        *LINE,
        "period_end",
        "fiscal_tag",
        "fiscal_quarter",
        pl.col("value").alias("own_value"),
        pl.lit(None, pl.Decimal(38, 10)).alias("comparative_value"),
    )
    comps = comparatives.filter(pl.col("value").is_not_null()).select(
        *LINE,
        "period_end",
        pl.lit(None, pl.Int32).alias("fiscal_tag"),
        pl.lit(None, pl.Int32).alias("fiscal_quarter"),
        pl.lit(None, pl.Decimal(38, 10)).alias("own_value"),
        pl.col("value").alias("comparative_value"),
    )
    both = pl.concat([own, comps], how="vertical_relaxed")
    return (
        both.group_by(*LINE, "period_end")
        .agg(
            pl.col("fiscal_tag").max(),
            pl.col("fiscal_quarter").max(),
            pl.col("own_value").sum(),
            pl.col("comparative_value").sum(),
            pl.col("own_value").count().alias("own_n"),
            pl.col("comparative_value").count().alias("comparative_n"),
        )
        .filter((pl.col("own_n") == 1) & (pl.col("comparative_n") == 1))
        .drop("own_n", "comparative_n")
    )


def quarter_basis(pairs: pl.DataFrame, tagged: pl.DataFrame) -> pl.DataFrame:
    """Per line and year: the three pairs, this 10-K, and next 10-K's reprint of it."""
    annual = sql.sort(
        tagged.filter(
            pl.col("source_type").is_in(ANNUAL_FORMS)
            & _income_lines()
            & (pl.col("period") == "YTD")
        ),
        ["period_end"],
    )
    following = pl.col("comparative_period_end").shift(-1).over(LINE)
    annual = annual.with_columns(
        pl.when(following == pl.col("period_end"))
        .then(pl.col("comparative_value").shift(-1).over(LINE))
        .alias("next_annual")
    ).filter(pl.col("fiscal_tag").is_not_null() & pl.col("value").is_not_null())

    sums = pairs.group_by(*LINE, "fiscal_tag").agg(
        pl.col("own_value").sum().alias("own_sum"),
        pl.col("comparative_value").sum().alias("comp_sum"),
        pl.col("fiscal_quarter").n_unique().alias("quarters"),
        pl.len().alias("pairs"),
    )
    years = annual.group_by(*LINE, "fiscal_tag").agg(
        pl.col("value").max().alias("annual"),
        pl.col("next_annual").max(),
        pl.len().alias("annuals"),
    )
    basis = sums.join(years, on=[*LINE, "fiscal_tag"]).filter(
        (pl.col("quarters") == 3) & (pl.col("pairs") == 3) & (pl.col("annuals") == 1)
    )
    return basis.with_columns(
        pl.max_horizontal(pl.col("annual").abs() / 1000, sql.decimal(1)).alias(
            "tolerance"
        )
    )


def comparative_basis(basis: pl.DataFrame) -> pl.DataFrame:
    """Lines and years that move onto next year's comparatives.

    Either way the comparatives must fit: the 10-K minus them keeps its sign.
    """
    fits = (pl.col("annual") - pl.col("comp_sum")).sign() == pl.col("annual").sign()
    fits = fits | ((pl.col("annual") - pl.col("comp_sum")).abs() <= pl.col("tolerance"))
    candidates = basis.filter(fits).with_columns(
        (
            (pl.col("own_sum").sign() == pl.col("annual").sign())
            & (
                pl.col("own_sum").abs()
                > pl.col("annual").abs() * sql.decimal(OVERSHOOT)
            )
        ).alias("overshoots")
    )
    candidates = candidates.with_columns(
        pl.col("overshoots")
        .any()
        .over("company_id", "statement", "fiscal_tag")
        .alias("year_overshoots")
    )
    follows = (
        pl.col("year_overshoots")
        & ((pl.col("next_annual") - pl.col("annual")).abs() <= pl.col("tolerance"))
        & ((pl.col("own_sum") - pl.col("comp_sum")).abs() > pl.col("tolerance"))
    )
    return candidates.filter(pl.col("overshoots") | follows.fill_null(False)).select(
        *LINE, "fiscal_tag"
    )


def with_restated_values(
    discrete: pl.DataFrame, pairs: pl.DataFrame, moved: pl.DataFrame
) -> pl.DataFrame:
    """``restated_value``: the comparative that replaces an own quarter, if any."""
    restated = moved.join(pairs, on=[*LINE, "fiscal_tag"]).select(
        *LINE, "period_end", pl.col("comparative_value").alias("restated")
    )
    eligible = (
        (pl.col("axis") == "")
        & ~pl.col("is_abstract")
        & ~pl.col("drop_ytd")
        & ~pl.col("is_comparative_backfill")
    )
    joined = discrete.join(restated, on=[*LINE, "period_end"], how="left")
    return joined.with_columns(
        pl.when(eligible).then(pl.col("restated")).alias("restated_value")
    ).drop("restated")
