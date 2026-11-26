"""Synthetic subtotals and section headers, and their rolled-up values.

When an override hangs a line under a parent (or a header) the filing never
tagged, a row is invented for that parent, with an id hashed from statement,
concept, filing and company. The invented row may itself have an overridden
parent, so this repeats until every pointer lands on a real fact or on no
override. A synthetic row's value is the weighted sum of what rolls up into it.
"""

from __future__ import annotations

from decimal import Decimal

import polars as pl

from ...io.schemas import dtypes_of
from ...rules import ordering
from .resolve import RESOLVED, company_override_unit, tree_pointers, with_tree_overrides

ZERO = pl.lit(Decimal(0), dtype=pl.Decimal(38, 10))
#: Deeper than any statement tree; reaching it means the pointers form a cycle.
MAX_DEPTH = 64


def synthetic_parents(
    rows: pl.DataFrame,
    facts: pl.DataFrame,
    concept_overrides: pl.DataFrame,
    hierarchy: pl.DataFrame,
) -> pl.DataFrame:
    """The rows invented for the missing parents and headers of ``rows``.

    Only concepts with a concept override can be invented. The new row takes
    the period of the row that pointed at it.
    """
    wanted = pl.concat(
        [
            rows.filter(pl.col("parent_concept").is_not_null()).select(
                pl.all().exclude("parent_concept", "abstract_concept"),
                pl.col("parent_concept").alias("new_concept"),
                pl.col("parent_id").alias("new_id"),
            ),
            rows.filter(pl.col("abstract_concept").is_not_null()).select(
                pl.all().exclude("parent_concept", "abstract_concept"),
                pl.col("abstract_concept").alias("new_concept"),
                pl.col("abstract_id").alias("new_id"),
            ),
        ]
    ).drop("parent_id", "abstract_id")
    if wanted.is_empty():
        return pl.DataFrame(schema=rows.schema)

    joined = with_tree_overrides(
        wanted, facts, concept_overrides, hierarchy, concept_column="new_concept"
    ).filter(pl.col("cno_concept").is_not_null())
    currency_shaped = pl.col("unit").str.contains(r"(?i)^[a-z]{3}(pershare)?$")
    unit = (
        pl.when(pl.col("cno_is_abstract"))
        .then(pl.lit(None, pl.String))
        .when(company_override_unit())
        .then(pl.col("cno_unit"))
        .when(currency_shaped)
        .then(pl.col("unit"))
    )
    built = joined.select(
        pl.col("new_id").alias("id"),
        "company_id",
        "filing_id",
        "form_type",
        pl.col("cno_concept").alias("concept"),
        pl.col("cno_normalized_label").alias("label"),
        pl.col("cno_normalized_label").alias("normalized_label"),
        pl.col("cno_is_abstract").alias("is_abstract"),
        ZERO.alias("value"),
        ZERO.alias("comparative_value"),
        pl.col("cno_weight").alias("weight"),
        pl.lit(None, pl.Decimal(38, 10)).alias("print_sign"),
        unit.alias("unit"),
        pl.lit("").alias("axis"),
        pl.lit("").alias("member"),
        pl.col("cno_statement").alias("statement"),
        "period_end",
        "comparative_period_end",
        "period",
        pl.lit(99, pl.Int32).alias("position"),
        *tree_pointers(pl.lit(None, pl.Int64), pl.lit(None, pl.Int64)),
        pl.lit(True).alias("is_synthetic"),
    )
    return built.select(RESOLVED).cast(dtypes_of(rows.schema))


def with_synthetic_rows(
    rows: pl.DataFrame,
    facts: pl.DataFrame,
    concept_overrides: pl.DataFrame,
    hierarchy: pl.DataFrame,
) -> pl.DataFrame:
    """``rows`` plus every synthetic ancestor they need, distinct rows only.

    A synthetic row's id names its statement, concept and filing, so children
    that differ in their period or comparative period would make one id
    twice; the latest is kept, the same on every refresh. (A filing that
    prints one subtotal for two period ends, rare, loses the earlier one.)
    """
    result = rows.unique()
    frontier = result
    while frontier.height:
        new = ordering.distinct_on(
            synthetic_parents(frontier, facts, concept_overrides, hierarchy),
            ["id"],
            [
                ordering.desc("period_end"),
                ordering.desc("comparative_period_end", nulls_last=True),
            ],
        )
        new = new.join(result.select("id"), on="id", how="anti")
        result = pl.concat([result, new])
        frontier = new
    return result


def rolled_up(rows: pl.DataFrame) -> pl.DataFrame:
    """Synthetic rows valued by the weighted contributions that reach them.

    Every real amount contributes ``value * weight`` to its parent. A
    synthetic parent passes each contribution on to its own parent, times
    its weight. A synthetic row's value is the sum of what arrives at it.
    """
    synthetic = rows.filter(pl.col("is_synthetic") & ~pl.col("is_abstract")).select(
        pl.col("id").alias("parent_id"),
        pl.col("parent_id").alias("next_parent_id"),
        pl.col("weight").alias("parent_weight"),
    )
    level = rows.filter(~pl.col("is_abstract") & ~pl.col("is_synthetic")).select(
        "parent_id",
        (pl.col("value") * pl.col("weight")).alias("contrib"),
        (pl.col("comparative_value") * pl.col("weight")).alias("contrib_comparative"),
    )
    arrived = []
    for _ in range(MAX_DEPTH):
        if not level.height:
            break
        step = level.join(synthetic, on="parent_id", how="inner").select(
            pl.col("parent_id").alias("id"),
            pl.col("next_parent_id").alias("parent_id"),
            (pl.col("contrib") * pl.col("parent_weight").fill_null(Decimal(1))).alias(
                "contrib"
            ),
            (
                pl.col("contrib_comparative")
                * pl.col("parent_weight").fill_null(Decimal(1))
            ).alias("contrib_comparative"),
        )
        arrived.append(step.select("id", "contrib", "contrib_comparative"))
        level = step.select("parent_id", "contrib", "contrib_comparative")
    else:
        raise RuntimeError("Synthetic parent pointers form a cycle")

    if not arrived:
        return rows.select(RESOLVED)
    totals = (
        pl.concat(arrived)
        .group_by("id")
        .agg(
            pl.col("contrib").sum().alias("rolled_value"),
            pl.col("contrib_comparative").sum().alias("rolled_comparative"),
            pl.col("contrib").count().alias("contributions"),
            pl.col("contrib_comparative").count().alias("comparative_contributions"),
        )
    )
    joined = rows.join(totals, on="id", how="left")
    # SUM over nothing, or over NULLs only, is NULL.
    value = pl.when(pl.col("contributions") > 0).then(pl.col("rolled_value"))
    comparative = pl.when(pl.col("comparative_contributions") > 0).then(
        pl.col("rolled_comparative")
    )
    return joined.with_columns(
        pl.when(pl.col("is_synthetic"))
        .then(value)
        .otherwise(pl.col("value"))
        .alias("value"),
        pl.when(pl.col("is_synthetic"))
        .then(comparative)
        .otherwise(pl.col("comparative_value"))
        .alias("comparative_value"),
    ).select(RESOLVED)
