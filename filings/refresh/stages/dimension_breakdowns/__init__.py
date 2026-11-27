"""Stage 6: which sets of members add up to another member or to the line.

Reads ``financial_facts_normalized``, ``financial_fact_dimensions``,
``filings`` and ``dimension_breakdown_overrides``; writes
``dimension_breakdowns`` with the fiscal years each set held in the 10-K.

* :mod:`.flat`: subsets of an axis's flat member list (inferred).
* :mod:`.context`: complete sibling sets inside cross-tabs (inferred).
* :mod:`.declared`: sets from the override CSV, which replace inference for
  their parent.

A flat set that only the 10-Qs filed since the latest annual report add up
to, in two fiscal years (the quarter and the prior-year quarter it reprints),
is kept as ``interim`` until a 10-K prints it.
"""

from __future__ import annotations

import polars as pl

from ...inputs import CompanyData
from ...io.schemas import DIMENSION_BREAKDOWNS
from .context import context_breakdowns
from .declared import company_overrides, declared_breakdowns, without_overridden_parents
from .flat import inferred_breakdowns
from .rows import (
    BREAKDOWN,
    BREAKDOWN_KEY,
    breakdown_rows,
    interim_rows,
    real_dimensions,
    rounding,
    with_members_key,
)


def collect_years(exact: pl.DataFrame) -> pl.DataFrame:
    """One row per breakdown with its sorted years and source.

    A set declared in the overrides is ``override`` even where inference
    found it too.
    """
    keyed = with_members_key(exact)
    return (
        keyed.group_by(BREAKDOWN_KEY)
        .agg(
            pl.col("members").first(),
            pl.col("fiscal_year").unique().sort().alias("fiscal_years"),
            pl.when((pl.col("source") == "override").any())
            .then(pl.lit("override"))
            .otherwise(pl.col("source").first())
            .alias("source"),
        )
        .drop("members_key")
    )


def run(data: CompanyData) -> pl.DataFrame:
    rows = breakdown_rows(data)
    dims = real_dimensions(data, rows)
    reported = rows.filter((pl.col("axis") != "") & (pl.col("value") != 0))
    totals = rows.filter(pl.col("axis") == "")
    overrides = company_overrides(
        data.table("dimension_breakdown_overrides"), data.company_id
    )

    flat = data.keep("flat", inferred_breakdowns(reported, dims, totals))
    context = data.keep("context", context_breakdowns(rows, dims))
    declared = data.keep("declared", declared_breakdowns(overrides, reported, totals))
    columns = [*BREAKDOWN, "fiscal_year"]
    exact = pl.concat(
        [
            without_overridden_parents(flat.select(columns), overrides).with_columns(
                pl.lit("inferred").alias("source")
            ),
            without_overridden_parents(context.select(columns), overrides).with_columns(
                pl.lit("inferred").alias("source")
            ),
            declared.select(columns).with_columns(pl.lit("override").alias("source")),
        ],
        how="vertical_relaxed",
    )
    annual = collect_years(exact)
    interim = data.keep("interim", interim_breakdowns(data, overrides, annual))
    return (
        pl.concat([annual, interim], how="vertical_relaxed")
        .select(DIMENSION_BREAKDOWNS.columns)
        .cast(DIMENSION_BREAKDOWNS.dtypes)
    )


def interim_breakdowns(
    data: CompanyData, overrides: pl.DataFrame, annual: pl.DataFrame
) -> pl.DataFrame:
    """Flat sets of the pending 10-Qs that no 10-K breakdown already lists.

    A set a 10-K already found needs no interim years: the chart draws a
    split in any period its members add up in.
    """
    rows = interim_rows(data)
    if rows.is_empty():
        return annual.clear()
    dims = real_dimensions(data, rows)
    reported = rows.filter((pl.col("axis") != "") & (pl.col("value") != 0))
    totals = line_quarters(data, rows)
    found = without_overridden_parents(
        inferred_breakdowns(reported, dims, totals).select(*BREAKDOWN, "fiscal_year"),
        overrides,
    ).with_columns(pl.lit("interim").alias("source"))
    new = collect_years(found)
    return (
        with_members_key(new)
        .join(
            with_members_key(annual).select(BREAKDOWN_KEY), on=BREAKDOWN_KEY, how="anti"
        )
        .drop("members_key")
    )


def line_quarters(data: CompanyData, rows: pl.DataFrame) -> pl.DataFrame:
    """The printed quarter of each line on the dates of ``rows``, from any filing.

    A 10-Q stores the prior-year quarter of its face lines only as a
    comparative, so that quarter's line comes from its own filing.
    """
    years = rows.select("company_id", "period_end", "fiscal_year").unique()
    lines = data.table("financial_facts_normalized").filter(
        (pl.col("axis") == "")
        & ~pl.col("is_duplicate")
        & ~pl.col("is_abstract")
        & (pl.col("period") == "Q")
        & pl.col("value").is_not_null()
    )
    return (
        lines.join(years, on=["company_id", "period_end"], how="inner")
        .with_columns(rounding(pl.col("value")).alias("rounding"))
        .select(rows.columns)
    )
