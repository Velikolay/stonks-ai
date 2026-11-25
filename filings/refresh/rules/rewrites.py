"""``financial_rewrites``: the last-resort edits to the presentation layers.

One row edits one member of one line for one period on the quarterly or
yearly layer, after everything else is derived. ``delete`` removes it,
``set`` replaces its value, and ``add`` inserts it, copied from another
member of that axis and period, or from the undimensioned line when the axis
has no row yet. The copy takes the first such member in the statement, by
position, so the new member sits where the axis starts.
"""

from __future__ import annotations

import polars as pl

from ..io.schemas import dtypes_of
from . import ordering
from .ids import abs_hash_expr

KEY = ["company_id", "statement", "normalized_label", "axis", "member", "period_end"]


def apply_rewrites(
    frame: pl.DataFrame, rewrites: pl.DataFrame, granularity: str, company_id: int
) -> pl.DataFrame:
    """Apply the company's ``granularity`` rewrites to a presentation frame."""
    mine = rewrites.filter(
        (pl.col("granularity") == granularity) & (pl.col("company_id") == company_id)
    )
    if mine.is_empty():
        return frame
    by_action = {
        a: mine.filter(pl.col("action") == a) for a in ("delete", "set", "add")
    }

    frame = frame.join(by_action["delete"].select(KEY), on=KEY, how="anti")

    updates = by_action["set"].select(*KEY, pl.col("value").alias("new_value"))
    frame = (
        frame.join(updates, on=KEY, how="left")
        .with_columns(
            pl.coalesce("new_value", "value").alias("value"),
            pl.when(pl.col("new_value").is_not_null())
            .then(True)
            .otherwise(pl.col("is_synthetic"))
            .alias("is_synthetic"),
            pl.when(pl.col("new_value").is_not_null())
            .then(pl.lit("calculated"))
            .otherwise(pl.col("source_type"))
            .alias("source_type"),
        )
        .drop("new_value")
    )
    added = _added_rows(frame, by_action["add"], granularity)
    return pl.concat([frame, added.select(frame.columns).cast(dtypes_of(frame.schema))])


def _added_rows(
    frame: pl.DataFrame, adds: pl.DataFrame, granularity: str
) -> pl.DataFrame:
    """New members, each copied from a sibling or from the undimensioned line."""
    adds = adds.join(frame.select(KEY), on=KEY, how="anti")
    if adds.is_empty():
        return frame.clear()
    sources = frame.filter(~pl.col("is_abstract")).rename(
        {"axis": "source_axis", "member": "source_member"}
    )
    pairs = (
        adds.select(*KEY, pl.col("value").alias("new_value"))
        .join(
            sources.drop("value"),
            on=["company_id", "statement", "normalized_label", "period_end"],
        )
        .filter(
            (pl.col("source_axis") == pl.col("axis")) | (pl.col("source_axis") == "")
        )
    )
    best = ordering.distinct_on(
        pairs,
        KEY,
        [
            ordering.asc(
                pl.when(pl.col("source_axis") == pl.col("axis")).then(0).otherwise(1)
            ),
            ordering.asc("position"),
            ordering.asc(ordering.by_text("source_member")),
        ],
    )
    return best.with_columns(
        abs_hash_expr(
            "financial-rewrite",
            granularity,
            pl.col("company_id"),
            pl.col("statement"),
            pl.col("normalized_label"),
            pl.col("axis"),
            pl.col("member"),
            pl.col("period_end"),
        ).alias("id"),
        pl.col("new_value").alias("value"),
        pl.lit(False).alias("is_abstract"),
        pl.lit(True).alias("is_synthetic"),
        pl.lit("calculated").alias("source_type"),
    )
