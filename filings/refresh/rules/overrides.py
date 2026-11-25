"""Matching override rows to the facts or concepts they apply to.

Everywhere the precedence is the same: a company's own row beats a global
row (``company_id = 0``, ``is_global``), and a global row beats nothing.
"""

from __future__ import annotations

import polars as pl

from . import ordering

FACT_OVERRIDE_FILTERS = ("axis", "member", "label", "form_type")


def for_company(overrides: pl.DataFrame, company_id: int) -> pl.DataFrame:
    """Override rows that can apply to ``company_id``: its own and the global ones."""
    return overrides.filter((pl.col("company_id") == company_id) | pl.col("is_global"))


def matching_fact_overrides(
    facts: pl.DataFrame, overrides: pl.DataFrame, action: str
) -> pl.DataFrame:
    """Pairs of fact and ``financial_facts_overrides`` row that matches it.

    ``facts`` must carry ``filing_fiscal_period_end``. A blank filter column on
    the override matches anything; a set one must equal the fact's value, and
    the period bounds are inclusive. Override columns come back prefixed
    ``r_``.
    """
    rules = overrides.filter(pl.col("action") == action)
    rules = rules.rename({c: f"r_{c}" for c in rules.columns})
    pairs = facts.join(
        rules,
        left_on=["statement", "concept"],
        right_on=["r_statement", "r_concept"],
        how="inner",
    )
    matches = [
        (pl.col("r_company_id") == pl.col("company_id")) | pl.col("r_is_global"),
        *(
            pl.col(f"r_{c}").is_null() | (pl.col(f"r_{c}") == pl.col(c))
            for c in FACT_OVERRIDE_FILTERS
        ),
        pl.col("r_from_period").is_null()
        | (pl.col("period_end") >= pl.col("r_from_period")),
        pl.col("r_to_period").is_null()
        | (pl.col("period_end") <= pl.col("r_to_period")),
        pl.col("r_from_filing_period").is_null()
        | (pl.col("filing_fiscal_period_end") >= pl.col("r_from_filing_period")),
        pl.col("r_to_filing_period").is_null()
        | (pl.col("filing_fiscal_period_end") <= pl.col("r_to_filing_period")),
    ]
    return pairs.filter(pl.all_horizontal(matches))


def most_specific_fact_override(pairs: pl.DataFrame) -> pl.DataFrame:
    """The one override that wins per fact.

    Company rows first, then the row with the most filters set, then the
    narrowest period window, then the most recently updated.
    """
    specificity = pl.sum_horizontal(
        pl.col(f"r_{c}").is_not_null().cast(pl.Int32)
        for c in (
            *FACT_OVERRIDE_FILTERS,
            "from_period",
            "to_period",
            "from_filing_period",
            "to_filing_period",
        )
    )
    return ordering.distinct_on(
        pairs,
        ["id"],
        [
            ordering.desc(pl.col("r_company_id") == pl.col("company_id")),
            ordering.desc(specificity),
            ordering.asc(pl.col("r_to_period") - pl.col("r_from_period")),
            ordering.desc("r_updated_at"),
        ],
    )


def best_concept_override(
    rows: pl.DataFrame,
    overrides: pl.DataFrame,
    concept_column: str = "concept",
    prefix: str = "cno_",
) -> pl.DataFrame:
    """Left-join each row's winning ``concept_normalization_overrides`` row.

    Matches on ``statement`` and ``concept_column``; the row's own company
    beats the global row. Override columns come back with ``prefix``.
    """
    candidates = overrides.rename({c: f"{prefix}{c}" for c in overrides.columns})
    company = candidates.join(
        rows.select("company_id").unique(),
        left_on=f"{prefix}company_id",
        right_on="company_id",
        how="semi",
    )
    global_ = candidates.filter(pl.col(f"{prefix}is_global"))
    keys = [f"{prefix}statement", f"{prefix}concept"]

    with_company = rows.join(
        company,
        left_on=["company_id", "statement", concept_column],
        right_on=[f"{prefix}company_id", *keys],
        how="left",
        coalesce=False,
    )
    company_hit = pl.col(f"{prefix}concept").is_not_null()
    hits = with_company.filter(company_hit)
    misses = with_company.filter(~company_hit).select(rows.columns)
    with_global = misses.join(
        global_,
        left_on=["statement", concept_column],
        right_on=keys,
        how="left",
        coalesce=False,
    )
    return pl.concat([hits, with_global.select(hits.columns)], how="vertical")
