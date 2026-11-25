"""Breakdowns inside cross-tabs: facts stored with several XBRL dimensions.

Such a fact is a cell. Vary one of its dimensions and keep the others as
the context: the cells that share the context, on the same display axis,
are a breakdown of the fact whose dimensions are exactly the context, when
they add up to it. That set is not searched and is not capped. Like an
inferred flat breakdown, it must add up in at least two 10-Ks.
"""

from __future__ import annotations

import polars as pl

from ...rules import sql
from .rows import BREAKDOWN_KEY, with_members_key, within_tolerance_expr

SEP_PART = "\x1f"
SEP_DIM = "\x1e"


def _signature(dims: pl.DataFrame, by: list[str]) -> pl.DataFrame:
    """``axis\\x1fmember`` per dimension, joined in collation order, per ``by``."""
    ordered = sql.sort(
        dims, [sql.collate("dim_axis"), sql.collate("dim_member"), "ordinal"]
    )
    return ordered.group_by(by, maintain_order=True).agg(
        pl.concat_str(["dim_axis", "dim_member"], separator=SEP_PART)
        .str.join(SEP_DIM)
        .alias("signature"),
        pl.len().alias("dim_count"),
    )


def context_breakdowns(rows: pl.DataFrame, dims: pl.DataFrame) -> pl.DataFrame:
    """Breakdowns found by varying one dimension of each cross-tab cell."""
    dims = dims.select(
        "fact_id",
        "ordinal",
        pl.col("axis").alias("dim_axis"),
        pl.col("member").alias("dim_member"),
    )
    signatures = _signature(dims, ["fact_id"])
    facts = rows.filter((pl.col("axis") != "") & (pl.col("value") != 0)).join(
        signatures, left_on="id", right_on="fact_id"
    )

    # Each cell once per dimension it can vary, with the context it leaves.
    cells = facts.filter(pl.col("dim_count") > 1).select(
        "id",
        "company_id",
        "statement",
        "normalized_label",
        "axis",
        "member",
        "period_end",
        "fiscal_year",
        "value",
        "rounding",
    )
    dropped = dims.rename({"ordinal": "dropped", "dim_axis": "varying_axis"}).drop(
        "dim_member"
    )
    kept = (
        cells.join(dropped, left_on="id", right_on="fact_id")
        .join(dims, left_on="id", right_on="fact_id")
        .filter(pl.col("ordinal") != pl.col("dropped"))
    )
    children = _signature(
        kept,
        [
            "company_id",
            "statement",
            "normalized_label",
            "axis",
            "member",
            "period_end",
            "fiscal_year",
            "value",
            "rounding",
            "id",
            "dropped",
            "varying_axis",
        ],
    ).rename({"signature": "parent_signature"})

    # A context is a parent only when exactly one fact has it.
    parents = (
        facts.group_by(
            "company_id", "statement", "normalized_label", "period_end", "signature"
        )
        .agg(
            pl.col("axis").n_unique().alias("axes"),
            pl.col("member").n_unique().alias("members_n"),
            pl.col("value").n_unique().alias("values"),
            pl.col("axis").first().alias("parent_axis"),
            pl.col("member").first().alias("parent_member"),
            pl.col("fiscal_year").min().alias("parent_fiscal_year"),
            pl.col("value").first().alias("parent_value"),
            pl.col("rounding").min().alias("parent_rounding"),
        )
        .filter(
            (pl.col("axes") == 1) & (pl.col("members_n") == 1) & (pl.col("values") == 1)
        )
    )

    paired = children.join(
        parents,
        left_on=[
            "company_id",
            "statement",
            "normalized_label",
            "period_end",
            "parent_signature",
        ],
        right_on=[
            "company_id",
            "statement",
            "normalized_label",
            "period_end",
            "signature",
        ],
    ).filter(
        (pl.col("axis") == pl.col("parent_axis"))
        & (pl.col("member") != pl.col("parent_member"))
    )

    group = [
        "company_id",
        "statement",
        "normalized_label",
        "parent_axis",
        "parent_member",
        "period_end",
        "parent_fiscal_year",
        "parent_value",
        "parent_rounding",
        "varying_axis",
    ]
    grouped = (
        sql.sort(paired, [sql.collate("member")])
        .group_by(group, maintain_order=True)
        .agg(
            pl.col("member").alias("members"),
            pl.col("value").sum().alias("members_total"),
            pl.col("rounding").max().alias("members_rounding"),
            pl.len().alias("n"),
            pl.col("member").n_unique().alias("distinct_members"),
        )
        .filter((pl.col("n") >= 2) & (pl.col("n") == pl.col("distinct_members")))
    )
    matched = grouped.filter(
        within_tolerance_expr(
            pl.col("parent_value"),
            pl.col("parent_rounding"),
            pl.col("members_total"),
            pl.col("members_rounding"),
            pl.col("n"),
        )
    ).select(
        "company_id",
        "statement",
        "normalized_label",
        pl.col("parent_axis").alias("axis"),
        "parent_member",
        "members",
        pl.col("parent_fiscal_year").alias("fiscal_year"),
    )
    years = pl.col("fiscal_year").n_unique().over(BREAKDOWN_KEY)
    return with_members_key(matched).filter(years >= 2).drop("members_key")
