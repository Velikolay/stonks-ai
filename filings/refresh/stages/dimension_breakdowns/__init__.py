"""Stage 6: which sets of members add up to another member or to the line.

Reads ``financial_facts_normalized``, ``financial_fact_dimensions``,
``filings`` and ``dimension_breakdown_overrides``; writes
``dimension_breakdowns`` with the fiscal years each set held in the 10-K.

* :mod:`.flat`: subsets of an axis's flat member list (inferred).
* :mod:`.context`: complete sibling sets inside cross-tabs (inferred).
* :mod:`.declared`: sets from the override CSV, which replace inference for
  their parent.
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
    real_dimensions,
    with_members_key,
)


def collect_years(exact: pl.DataFrame) -> pl.DataFrame:
    """One row per breakdown with its sorted years; a declared one is ``override``."""
    keyed = with_members_key(exact)
    return (
        keyed.group_by(BREAKDOWN_KEY)
        .agg(
            pl.col("members").first(),
            pl.col("fiscal_year").unique().sort().alias("fiscal_years"),
            pl.when((pl.col("source") == "override").any())
            .then(pl.lit("override"))
            .otherwise(pl.lit("inferred"))
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
    return (
        collect_years(exact)
        .select(DIMENSION_BREAKDOWNS.columns)
        .cast(DIMENSION_BREAKDOWNS.schema)
    )
