"""Comparative-value chains, which dedup uses to prefer well-connected rows.

From each row with a comparative, step to the row of the same series whose
value for the comparative period equals that comparative; without one, to
the same concept's row for that period. A row's chain size is the longest
chain it takes part in.
"""

from __future__ import annotations

import polars as pl

SERIES = ["company_id", "statement", "normalized_label", "axis", "member"]
#: Comparative periods only go back, so a chain longer than this is a cycle.
MAX_DEPTH = 200


def chain_targets(rows: pl.DataFrame) -> pl.DataFrame:
    """One row per series, value and period: the rows a chain can step to."""
    ordered = rows.sort([pl.col("unit").is_null(), pl.col("id")], maintain_order=True)
    return ordered.unique(
        subset=[*SERIES, "value", "period_end"], keep="first", maintain_order=True
    )


def chain_sizes(rows: pl.DataFrame) -> pl.DataFrame:
    """``id -> chain_size`` for every row that is in some chain."""
    targets = chain_targets(rows)
    starts = rows.filter(
        pl.col("comparative_value").is_not_null()
        & pl.col("comparative_period_end").is_not_null()
    )
    frontier = starts.select(
        pl.col("id").alias("root"),
        *SERIES,
        "id",
        "concept",
        "comparative_value",
        "comparative_period_end",
        pl.lit(1, pl.Int64).alias("depth"),
    ).unique()
    walked = [frontier.select("root", "id", "depth")]
    for _ in range(MAX_DEPTH):
        if not frontier.height:
            break
        frontier = _step(frontier, targets)
        walked.append(frontier.select("root", "id", "depth"))
    else:
        raise RuntimeError("Comparative chains do not terminate")

    walk = pl.concat(walked)
    size = walk.group_by("root").agg(pl.col("depth").max().alias("chain_size"))
    return walk.join(size, on="root").group_by("id").agg(pl.col("chain_size").max())


def _step(frontier: pl.DataFrame, targets: pl.DataFrame) -> pl.DataFrame:
    """Advance every chain one period: by equal value, else by the same concept."""
    nxt_cols = [
        pl.col("id_next").alias("id"),
        pl.col("concept_next").alias("concept"),
        pl.col("comparative_value_next").alias("comparative_value"),
        pl.col("comparative_period_end_next").alias("comparative_period_end"),
    ]
    tgt = targets.select(
        *SERIES,
        pl.col("value").alias("t_value"),
        pl.col("period_end").alias("t_period_end"),
        pl.col("id").alias("id_next"),
        pl.col("concept").alias("concept_next"),
        pl.col("comparative_value").alias("comparative_value_next"),
        pl.col("comparative_period_end").alias("comparative_period_end_next"),
    )
    by_value = frontier.join(
        tgt,
        left_on=[*SERIES, "comparative_value", "comparative_period_end"],
        right_on=[*SERIES, "t_value", "t_period_end"],
    )
    unmatched = frontier.join(
        by_value.select("root", "id", "depth").unique(),
        on=["root", "id", "depth"],
        how="anti",
    )
    by_concept = unmatched.join(
        tgt.with_columns(pl.col("concept_next").alias("concept")),
        left_on=[*SERIES, "concept", "comparative_period_end"],
        right_on=[*SERIES, "concept", "t_period_end"],
    )
    stepped = pl.concat(
        [
            by_value.select(
                "root", *SERIES, *nxt_cols, (pl.col("depth") + 1).alias("depth")
            ),
            by_concept.select(
                "root", *SERIES, *nxt_cols, (pl.col("depth") + 1).alias("depth")
            ),
        ]
    )
    return stepped.unique()


def chain_sizes_with_copies(rows: pl.DataFrame, ties: pl.DataFrame) -> pl.DataFrame:
    """:func:`chain_sizes` of the filed rows, and each prior copy its source's.

    A copy is its source's comparative (see :mod:`...rules.prior_periods`),
    so as a chain target it would lengthen the chains of later periods and
    could change which of two concepts sharing a label wins there. ``ties``
    maps a copy's ``id`` to its source's ``tie_id``.
    """
    sizes = chain_sizes(rows.join(ties, on="id", how="anti"))
    copied = ties.join(sizes, left_on="tie_id", right_on="id", how="inner")
    return pl.concat([sizes, copied.select(sizes.columns)])
