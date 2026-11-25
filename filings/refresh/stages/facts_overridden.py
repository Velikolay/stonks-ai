"""Stage 1: apply ``financial_facts_overrides`` to the raw facts.

Reads ``financial_facts``, ``filings`` and the fact overrides; writes
``financial_facts_overridden``. A ``rewrite`` row replaces a fact's concept,
dimensions and weight under the fact's own id, and only the most specific
matching rule applies. A ``clone`` row keeps the fact and adds a copy per
matching rule, with a negative id and ``source_fact_id`` pointing back.
Facts no override touches are not written: later stages fall back to the raw
fact.
"""

from __future__ import annotations

import polars as pl

from ..inputs import CompanyData
from ..io.schemas import FINANCIAL_FACTS_OVERRIDDEN
from ..rules import overrides
from ..rules.ids import clone_id


def facts_with_filing_period(data: CompanyData) -> pl.DataFrame:
    """The company's facts with the fiscal period of the filing they came from."""
    filings = data.table("filings").select(
        pl.col("id").alias("filing_id"),
        pl.col("fiscal_period_end").alias("filing_fiscal_period_end"),
    )
    return data.table("financial_facts").join(filings, on="filing_id", how="inner")


def rewritten(pairs: pl.DataFrame) -> pl.DataFrame:
    """One row per fact that a ``rewrite`` rule matched, under the fact's id."""
    winners = overrides.most_specific_fact_override(pairs)
    return winners.select(
        "id",
        "company_id",
        "statement",
        pl.coalesce("r_to_concept", "concept").alias("concept"),
        pl.coalesce("r_to_axis", "axis").alias("axis"),
        pl.coalesce("r_to_member", "member").alias("member"),
        pl.coalesce("r_to_member_label", "member_label").alias("member_label"),
        pl.coalesce("r_to_weight", "weight").alias("weight"),
        pl.col("r_id").alias("fact_override_id"),
        pl.lit(None, pl.Int64).alias("source_fact_id"),
    )


def cloned(pairs: pl.DataFrame) -> pl.DataFrame:
    """One extra row per fact and matching ``clone`` rule."""
    ids = [clone_id(f, r) for f, r in zip(pairs["id"], pairs["r_id"])]
    return pairs.select(
        pl.Series("id", ids, dtype=pl.Int64),
        "company_id",
        "statement",
        pl.col("r_to_concept").alias("concept"),
        pl.col("r_to_axis").alias("axis"),
        pl.col("r_to_member").alias("member"),
        pl.col("r_to_member_label").alias("member_label"),
        pl.coalesce("r_to_weight", "weight").alias("weight"),
        pl.col("r_id").alias("fact_override_id"),
        pl.col("id").alias("source_fact_id"),
    )


def run(data: CompanyData) -> pl.DataFrame:
    facts = facts_with_filing_period(data)
    rules = data.table("financial_facts_overrides")
    frames = [
        rewritten(overrides.matching_fact_overrides(facts, rules, "rewrite")),
        cloned(overrides.matching_fact_overrides(facts, rules, "clone")),
    ]
    return pl.concat(
        [f.cast(FINANCIAL_FACTS_OVERRIDDEN.schema) for f in frames], how="vertical"
    ).select(FINANCIAL_FACTS_OVERRIDDEN.columns)
