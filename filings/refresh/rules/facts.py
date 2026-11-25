"""The fact layer every normalization stage starts from.

Raw facts with their ``rewrite`` overrides applied, optionally with the
``clone`` copies beside them, and with conflicting facts removed. Stages
differ in which conflicts they drop, so the two rules are named side by side.
"""

from __future__ import annotations

from enum import Enum

import polars as pl

from ..inputs import CompanyData

#: A fact's slot: two facts here that disagree on the value conflict.
SLOT = (
    "company_id",
    "statement",
    "concept",
    "label",
    "axis",
    "member",
    "member_label",
    "period_end",
    "form_type",
    "period",
)

REWRITTEN = ("concept", "weight", "axis", "member", "member_label")


class ConflictRule(Enum):
    """Which disagreements remove a slot's facts."""

    #: Any two values for the slot, across all filings. Label inference uses
    #: this: a restated value must not chain or group.
    ANY_FILING = "any_filing"
    #: Two values inside the slot's latest filing. A restatement is the same
    #: slot reported again later, and dedup keeps the later filing.
    LATEST_FILING = "latest_filing"


def with_fact_overrides(data: CompanyData, include_clones: bool) -> pl.DataFrame:
    """Raw facts with ``financial_facts_overridden`` applied.

    A rewrite row replaces the fact's concept, weight and dimensions under the
    same id. With ``include_clones``, each clone row adds a copy of its source
    fact: not abstract, outside the filing's tree, with the clone's own id.
    """
    facts = data.table("financial_facts")
    overridden = data.table("financial_facts_overridden")
    rewrites = overridden.filter(pl.col("source_fact_id").is_null()).select(
        "id", *(pl.col(c).alias(f"o_{c}") for c in REWRITTEN), "fact_override_id"
    )
    base = facts.join(rewrites, on="id", how="left").with_columns(
        pl.coalesce(f"o_{c}", c).alias(c) for c in REWRITTEN
    )
    base = base.drop(f"o_{c}" for c in REWRITTEN)
    if not include_clones:
        return base

    clones = overridden.filter(pl.col("source_fact_id").is_not_null())
    copies = clones.rename({"id": "clone_id"}).join(
        facts.drop(*REWRITTEN, "company_id", "statement"),
        left_on="source_fact_id",
        right_on="id",
        how="inner",
    )
    copies = copies.with_columns(
        pl.col("clone_id").alias("id"),
        pl.lit(False).alias("is_abstract"),
        pl.lit(None, pl.Int64).alias("parent_id"),
        pl.lit(None, pl.Int64).alias("abstract_id"),
    ).select(base.columns)
    return pl.concat([base, copies], how="vertical")


def drop_conflicting(
    facts: pl.DataFrame, rule: ConflictRule, filings: pl.DataFrame | None = None
) -> pl.DataFrame:
    """Remove every fact of a slot whose values disagree under ``rule``.

    ``LATEST_FILING`` needs ``filings`` for each fact's filing date.
    """
    if rule is ConflictRule.ANY_FILING:
        disagrees = pl.col("value").drop_nulls().n_unique() > 1
        return facts.filter(~disagrees.over(SLOT))

    if filings is None:
        raise ValueError("LATEST_FILING needs the filings frame")
    dated = facts.join(
        filings.select(pl.col("id").alias("filing_id"), "filing_date"),
        on="filing_id",
        how="inner",
    )
    latest = pl.col("filing_date") == pl.col("filing_date").max()
    disagrees = pl.col("value").filter(latest).drop_nulls().n_unique() > 1
    return dated.filter(~disagrees.over(SLOT)).drop("filing_date")
