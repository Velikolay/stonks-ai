"""Resolve each fact's label, sign, unit, dimensions and tree pointers.

Precedence for the label and weight: the concept override, the inferred
``concept_normalization`` row, then the fact as filed. A fact is negated when
its own weight and the resolved weight disagree in sign. A ``parent_concept``
from the override (or the expanded ``hierarchy_normalization``) replaces the
filing's own parent; when that parent is not a fact of the same filing, the
pointer goes to a synthetic row built in :mod:`.synthetic`.
"""

from __future__ import annotations

import polars as pl

from ...rules.ids import abs_hash_expr
from ...rules.overrides import best_concept_override
from ...rules.units import is_currency, is_per_share_concept

#: Columns of a resolved row, real or synthetic.
RESOLVED = [
    "id",
    "company_id",
    "filing_id",
    "form_type",
    "concept",
    "label",
    "normalized_label",
    "is_abstract",
    "value",
    "comparative_value",
    "weight",
    "print_sign",
    "unit",
    "axis",
    "member",
    "statement",
    "period_end",
    "comparative_period_end",
    "period",
    "position",
    "parent_id",
    "parent_concept",
    "abstract_id",
    "abstract_concept",
    "is_synthetic",
]


def company_override_unit() -> pl.Expr:
    """The override names a unit and is the company's own (global ones never do)."""
    return (
        pl.col("cno_is_global").fill_null(False).not_()
        & pl.col("cno_unit").is_not_null()
    )


def synthetic_id(statement: pl.Expr, concept: pl.Expr) -> pl.Expr:
    """Id of the row standing in for ``concept`` in a filing that never tagged it."""
    return abs_hash_expr(statement, concept, pl.col("filing_id"), pl.col("company_id"))


def with_tree_overrides(
    rows: pl.DataFrame,
    facts: pl.DataFrame,
    concept_overrides: pl.DataFrame,
    hierarchy: pl.DataFrame,
    concept_column: str = "concept",
) -> pl.DataFrame:
    """Attach the override, hierarchy row and the parent/abstract facts.

    ``rows`` keep one row per (row, hierarchy parent, parent fact, abstract
    fact); a concept with several expanded parents is repeated. Added columns: ``cno_*``, ``hn_parent_concept``,
    ``hn_parent_concept_source``, ``parent_fact_id``, ``abstract_fact_id``.
    """
    joined = best_concept_override(rows, concept_overrides, concept_column)
    joined = joined.join(
        hierarchy.select(
            "company_id",
            "statement",
            pl.col("concept").alias(concept_column),
            pl.col("parent_concept").alias("hn_parent_concept"),
            pl.col("parent_concept_source").alias("hn_parent_concept_source"),
        ),
        on=["company_id", "statement", concept_column],
        how="left",
    )
    by_concept = facts.select(
        "company_id",
        "filing_id",
        "statement",
        "concept",
        pl.col("id").alias("found_id"),
    )
    parent = pl.coalesce("cno_parent_concept", "hn_parent_concept")
    joined = (
        joined.with_columns(parent.alias("_parent"))
        .join(
            by_concept.rename({"concept": "_parent", "found_id": "parent_fact_id"}),
            on=["company_id", "filing_id", "statement", "_parent"],
            how="left",
        )
        .join(
            by_concept.rename(
                {"concept": "cno_abstract_concept", "found_id": "abstract_fact_id"}
            ),
            on=["company_id", "filing_id", "statement", "cno_abstract_concept"],
            how="left",
        )
    )
    return joined


def tree_pointers(own_parent: pl.Expr, own_abstract: pl.Expr) -> list[pl.Expr]:
    """``parent_id``, ``parent_concept``, ``abstract_id``, ``abstract_concept``.

    ``own_parent``/``own_abstract`` apply when no override names a parent.
    """
    parent = pl.coalesce("cno_parent_concept", "hn_parent_concept")
    synthetic_parent = pl.coalesce("cno_parent_concept", "hn_parent_concept_source")
    abstract = pl.col("cno_abstract_concept")
    parent_missing = parent.is_not_null() & pl.col("parent_fact_id").is_null()
    abstract_missing = abstract.is_not_null() & pl.col("abstract_fact_id").is_null()
    return [
        pl.when(parent.is_not_null() & pl.col("parent_fact_id").is_not_null())
        .then(pl.col("parent_fact_id"))
        .when(parent_missing)
        .then(synthetic_id(pl.col("statement"), synthetic_parent))
        .otherwise(own_parent)
        .alias("parent_id"),
        pl.when(parent_missing).then(synthetic_parent).alias("parent_concept"),
        pl.when(abstract.is_not_null() & pl.col("abstract_fact_id").is_not_null())
        .then(pl.col("abstract_fact_id"))
        .when(abstract_missing)
        .then(synthetic_id(pl.col("statement"), abstract))
        .otherwise(own_abstract)
        .alias("abstract_id"),
        pl.when(abstract_missing).then(abstract).alias("abstract_concept"),
    ]


def resolve_facts(
    facts: pl.DataFrame,
    concept_overrides: pl.DataFrame,
    hierarchy: pl.DataFrame,
    concept_normalization: pl.DataFrame,
    dimension_normalization: pl.DataFrame,
    currencies: pl.DataFrame,
) -> pl.DataFrame:
    """Every fact as a resolved row (columns :data:`RESOLVED`)."""
    rows = with_tree_overrides(facts, facts, concept_overrides, hierarchy)
    rows = rows.join(currencies, on="filing_id", how="left").join(
        concept_normalization.select(
            "company_id",
            "statement",
            "concept",
            pl.col("normalized_label").alias("cn_label"),
            pl.col("weight").alias("cn_weight"),
            pl.col("unit").alias("cn_unit"),
        ),
        on=["company_id", "statement", "concept"],
        how="left",
    )
    rows = rows.with_columns(
        pl.coalesce("cno_normalized_label", "cn_label", "label").alias(
            "normalized_label"
        ),
        pl.coalesce("cno_weight", "cn_weight", "weight").alias("resolved_weight"),
    )
    rows = rows.join(
        dimension_normalization.select(
            "company_id",
            "statement",
            "normalized_label",
            "axis",
            "member",
            "member_label",
            pl.col("normalized_axis_label").alias("dn_axis"),
            pl.col("normalized_member_label").alias("dn_member"),
        ),
        on=[
            "company_id",
            "statement",
            "normalized_label",
            "axis",
            "member",
            "member_label",
        ],
        how="left",
    )
    flip = (pl.col("weight") * pl.col("resolved_weight")) < 0
    per_share = is_per_share_concept(pl.col("concept"))
    unit = (
        pl.when(company_override_unit())
        .then(pl.col("cno_unit"))
        .when(pl.col("cn_unit").is_not_null())
        .then(pl.col("cn_unit"))
        .when(per_share & is_currency(pl.col("unit")))
        .then(pl.col("unit") + "PerShare")
        .when(
            per_share
            & pl.col("unit").str.to_lowercase().is_in(["shares", "pershare"])
            & pl.col("currency").is_not_null()
        )
        .then(pl.col("currency") + "PerShare")
        .otherwise(pl.col("unit"))
    )
    return rows.select(
        "id",
        "company_id",
        "filing_id",
        "form_type",
        "concept",
        "label",
        "normalized_label",
        "is_abstract",
        pl.when(flip).then(-pl.col("value")).otherwise(pl.col("value")).alias("value"),
        pl.when(flip)
        .then(-pl.col("comparative_value"))
        .otherwise(pl.col("comparative_value"))
        .alias("comparative_value"),
        pl.col("resolved_weight").alias("weight"),
        "print_sign",
        # A company override replaces the unit; a global one keeps the fact's
        # currency, and a per-share concept keeps that shape.
        unit.alias("unit"),
        pl.coalesce("dn_axis", "axis").alias("axis"),
        pl.coalesce("dn_member", "member_label").alias("member"),
        "statement",
        "period_end",
        "comparative_period_end",
        "period",
        "position",
        *tree_pointers(pl.col("parent_id"), pl.col("abstract_id")),
        pl.lit(False).alias("is_synthetic"),
    )
