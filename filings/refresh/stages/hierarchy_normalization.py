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
from ..rules import ordering
from ..rules.overrides import best_concept_override

GROUP_KEY = ["company_id", "group_id"]


def child_overrides(normalized: pl.DataFrame, overrides: pl.DataFrame) -> pl.DataFrame:
    """Each normalized concept's own winning override, when it names a parent."""
    joined = best_concept_override(normalized, overrides)
    return joined.filter(pl.col("cno_parent_concept").is_not_null())


def parent_overrides(normalized: pl.DataFrame, overrides: pl.DataFrame) -> pl.DataFrame:
    """Every winning override that names a normalized concept as its parent.

    Each child concept keeps one row, the company's own over the global one,
    as everywhere else; every child of a parent is kept.
    """
    companies = normalized.select("company_id").unique()
    own = overrides.join(companies, on="company_id", how="semi")
    global_ = (
        overrides.filter(pl.col("is_global"))
        .drop("company_id")
        .join(companies, how="cross")
        .join(own, on=["company_id", "statement", "concept"], how="anti")
    )
    winning = pl.concat([own, global_.select(own.columns)]).filter(
        pl.col("parent_concept").is_not_null()
    )
    return normalized.join(
        winning.select(
            "company_id",
            "statement",
            pl.col("concept").alias("child"),
            pl.col("parent_concept").alias("concept"),
        ),
        on=["company_id", "statement", "concept"],
    ).select(
        "company_id",
        "statement",
        "group_id",
        "child",
        pl.col("concept").alias("parent"),
    )


def without_own_group(rows: pl.DataFrame, normalized: pl.DataFrame) -> pl.DataFrame:
    """Drop an edge whose parent is the same line as the child.

    Chaining can put a subtotal and one of its parts in one group when the
    part once carried the subtotal's amount; expanding an override across
    that group would hang the line under itself.
    """
    groups = normalized.select("company_id", "statement", "concept", "group_id")
    same = (
        rows.join(groups, on=["company_id", "statement", "concept"])
        .join(
            groups.rename({"concept": "parent_concept", "group_id": "parent_group"}),
            on=["company_id", "statement", "parent_concept"],
        )
        .filter(pl.col("group_id") == pl.col("parent_group"))
    )
    rows = rows.filter(pl.col("concept") != pl.col("parent_concept"))
    return rows.join(
        same.select(rows.columns), on=rows.columns, how="anti", nulls_equal=True
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
    rows = without_own_group(rows, normalized)
    key = ["company_id", "statement", "concept", "parent_concept"]
    first = ordering.sort(rows, [ordering.asc(ordering.by_text("concept_source"))])
    concept_source = first.group_by(key).agg(pl.col("concept_source").first())
    first = ordering.sort(
        rows, [ordering.asc(ordering.by_text("parent_concept_source"))]
    )
    parent_source = first.group_by(key).agg(pl.col("parent_concept_source").first())
    return concept_source.join(parent_source, on=key).cast(
        HIERARCHY_NORMALIZATION.dtypes
    )
