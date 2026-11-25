"""Breakdowns declared in ``dimension-breakdown-overrides.csv``.

The override rows for one parent on one axis are its complete list of
breakdowns: they replace everything inferred for that parent. A declared
breakdown skips the two-year and expansion rules, but like an inferred one
it only lands on years where its members add up. A row with no members
leaves the parent without breakdowns.
"""

from __future__ import annotations

import polars as pl

from ...rules import ordering
from .rows import AXIS_PERIOD, PERIOD, within_tolerance


def company_overrides(overrides: pl.DataFrame, company_id: int) -> pl.DataFrame:
    """The company's rows, members in text order (``[]`` when blank)."""
    rows = overrides.filter(pl.col("company_id") == company_id)
    sorted_members = [
        sorted(m, key=ordering.text_key) if m is not None else []
        for m in rows["members"].to_list()
    ]
    return rows.with_columns(
        pl.Series("members", sorted_members, dtype=pl.List(pl.String))
    )


def without_overridden_parents(
    found: pl.DataFrame, overrides: pl.DataFrame
) -> pl.DataFrame:
    """Drop inferred breakdowns of any parent an override row speaks for."""
    claims = overrides.select(
        "company_id",
        "axis",
        "parent_member",
        pl.col("normalized_label").alias("o_label"),
    ).unique()
    claimed = found.join(claims, on=["company_id", "axis", "parent_member"]).filter(
        pl.col("o_label").is_null() | (pl.col("o_label") == pl.col("normalized_label"))
    )
    return found.join(
        claimed.select(found.columns).unique(),
        on=found.columns,
        how="anti",
        nulls_equal=True,
    )


def _reported_groups(reported: pl.DataFrame) -> pl.DataFrame:
    ordered = ordering.sort(reported, [ordering.by_text("member")])
    return ordered.group_by(*AXIS_PERIOD, "fiscal_year", maintain_order=True).agg(
        pl.col("member").alias("names"),
        pl.col("value").alias("amounts"),
        pl.col("rounding").alias("roundings"),
    )


def _reported_targets(
    reported: pl.DataFrame, groups: pl.DataFrame, totals: pl.DataFrame
) -> pl.DataFrame:
    members = reported.join(
        totals.select(*PERIOD, "value"), on=[*PERIOD, "value"], how="anti"
    ).select(
        *AXIS_PERIOD,
        "fiscal_year",
        pl.col("member").alias("parent_member"),
        "value",
        "rounding",
    )
    line = (
        groups.select(*AXIS_PERIOD, "fiscal_year")
        .join(totals.select(*PERIOD, "value", "rounding"), on=PERIOD)
        .with_columns(pl.lit("").alias("parent_member"))
    )
    return pl.concat([members, line.select(members.columns)])


def declared_breakdowns(
    overrides: pl.DataFrame, reported: pl.DataFrame, totals: pl.DataFrame
) -> pl.DataFrame:
    """Each declared set on every 10-K year where its members add up."""
    groups = _reported_groups(reported)
    targets = _reported_targets(reported, groups, totals)
    out = []
    for o in overrides.filter(pl.col("members").list.len() >= 2).iter_rows(named=True):
        wanted = set(o["members"])
        candidates = groups.filter(
            (pl.col("company_id") == o["company_id"]) & (pl.col("axis") == o["axis"])
        )
        if o["normalized_label"] is not None:
            candidates = candidates.filter(
                pl.col("normalized_label") == o["normalized_label"]
            )
        for g in candidates.iter_rows(named=True):
            if not wanted <= set(g["names"]):
                continue
            picked = [
                (amount, rounding)
                for name, amount, rounding in zip(
                    g["names"], g["amounts"], g["roundings"]
                )
                if name in wanted
            ]
            total = sum(a for a, _ in picked)
            coarsest = max(r for _, r in picked)
            hits = targets.filter(
                pl.all_horizontal(pl.col(c) == g[c] for c in AXIS_PERIOD)
                & (pl.col("parent_member") == o["parent_member"])
            )
            for t in hits.iter_rows(named=True):
                if within_tolerance(
                    t["value"], t["rounding"], total, coarsest, len(o["members"])
                ):
                    out.append(
                        (
                            t["company_id"],
                            t["statement"],
                            t["normalized_label"],
                            t["axis"],
                            t["parent_member"],
                            o["members"],
                            t["fiscal_year"],
                        )
                    )
    return pl.DataFrame(
        out,
        schema={
            "company_id": pl.Int32,
            "statement": pl.String,
            "normalized_label": pl.String,
            "axis": pl.String,
            "parent_member": pl.String,
            "members": pl.List(pl.String),
            "fiscal_year": pl.Int32,
        },
        orient="row",
    )
