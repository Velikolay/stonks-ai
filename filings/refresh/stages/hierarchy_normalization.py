"""Stage 3: expand ``parent_concept`` overrides across concept groups.

Reads ``concept_normalization`` and the concept overrides; writes
``hierarchy_normalization``. An override that hangs concept C under parent P
also applies to every concept inferred to be the same line as C (C's group),
and points at every concept in P's group. So one override survives renames
on both ends.
"""

from __future__ import annotations

import polars as pl

from ..inputs import CompanyData
from ..io.schemas import HIERARCHY_NORMALIZATION
from ..rules import sql
from ..rules.overrides import best_concept_override

GROUP_KEY = ["company_id", "group_id"]


def child_overrides(normalized: pl.DataFrame, overrides: pl.DataFrame) -> pl.DataFrame:
    """Each normalized concept's own winning override, when it names a parent."""
    joined = best_concept_override(normalized, overrides)
    return joined.filter(pl.col("cno_parent_concept").is_not_null())


def parent_overrides(normalized: pl.DataFrame, overrides: pl.DataFrame) -> pl.DataFrame:
    """For each normalized concept, one override that names it as the parent.

    The company's row beats a global one. Among several children of the same
    parent, the first in table order is used, as the SQL's ``LIMIT 1`` did.
    """
    rules = overrides.filter(pl.col("parent_concept").is_not_null()).with_row_index(
        "order"
    )
    joined = normalized.join(
        rules.select(
            pl.col("company_id").alias("o_company_id"),
            "is_global",
            "statement",
            pl.col("concept").alias("o_concept"),
            pl.col("parent_concept").alias("concept"),
            "order",
        ),
        on=["statement", "concept"],
        how="inner",
    ).filter((pl.col("o_company_id") == pl.col("company_id")) | pl.col("is_global"))
    best = sql.distinct_on(
        joined,
        ["company_id", "statement", "concept"],
        [sql.desc(pl.col("o_company_id") == pl.col("company_id")), "order"],
    )
    return best.select(
        "company_id",
        "statement",
        "group_id",
        pl.col("o_concept").alias("child"),
        pl.col("concept").alias("parent"),
    )


def run(data: CompanyData) -> pl.DataFrame:
    normalized = data.table("concept_normalization")
    overrides = data.table("concept_normalization_overrides")
    members = normalized.select(*GROUP_KEY, pl.col("concept").alias("member"))

    # Override child C -> P applies to every concept in C's group.
    children = child_overrides(normalized, overrides).select(
        "company_id",
        "statement",
        "group_id",
        pl.col("cno_concept").alias("child"),
        pl.col("cno_parent_concept").alias("parent"),
    )
    concept_expansion = children.join(members, on=GROUP_KEY).select(
        "company_id", "statement", "child", "parent", pl.col("member").alias("concept")
    )
    # Override C -> P points at every concept in P's group.
    parent_expansion = (
        parent_overrides(normalized, overrides)
        .join(members, on=GROUP_KEY)
        .select(
            "company_id",
            "statement",
            "child",
            "parent",
            pl.col("member").alias("parent_concept"),
        )
    )
    # Both ends renamed: every member of C's group under every member of P's.
    transitive = concept_expansion.join(
        parent_expansion.select("company_id", "statement", "child", "parent_concept"),
        on=["company_id", "statement", "child"],
    ).select(
        "company_id",
        "statement",
        "concept",
        "parent_concept",
        pl.col("child").alias("concept_source"),
        pl.col("parent").alias("parent_concept_source"),
    )
    rows = pl.concat(
        [
            concept_expansion.select(
                "company_id",
                "statement",
                "concept",
                pl.col("parent").alias("parent_concept"),
                pl.col("child").alias("concept_source"),
                pl.col("parent").alias("parent_concept_source"),
            ),
            parent_expansion.select(
                "company_id",
                "statement",
                pl.col("child").alias("concept"),
                "parent_concept",
                pl.col("child").alias("concept_source"),
                pl.col("parent").alias("parent_concept_source"),
            ),
            transitive,
        ]
    )
    key = ["company_id", "statement", "concept", "parent_concept"]
    first = sql.sort(rows, [sql.asc(sql.collate("concept_source"))])
    concept_source = first.group_by(key).agg(pl.col("concept_source").first())
    first = sql.sort(rows, [sql.asc(sql.collate("parent_concept_source"))])
    parent_source = first.group_by(key).agg(pl.col("parent_concept_source").first())
    return concept_source.join(parent_source, on=key).cast(
        HIERARCHY_NORMALIZATION.schema
    )
