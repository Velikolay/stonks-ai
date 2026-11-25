"""Stage 5: one normalized, deduplicated row per fact.

Reads the facts with their overrides, every normalization table and the
filings; writes ``financial_facts_normalized``. In order:

1. :mod:`.resolve`: label, sign, unit, dimensions and tree pointers per fact.
2. :mod:`.synthetic`: invented subtotals and headers, and their rolled-up values.
3. :mod:`.chains`: comparative-value chain sizes, a dedup tie-breaker.
4. :mod:`.dedup`: one winner per series and period.
"""

from __future__ import annotations

import polars as pl

from ...inputs import CompanyData
from ...io.schemas import FINANCIAL_FACTS_NORMALIZED
from ...rules.facts import ConflictRule, drop_conflicting, with_fact_overrides
from ...rules.units import canonical_unit, filing_currency
from .chains import chain_sizes
from .dedup import mark_duplicates, rank
from .resolve import resolve_facts
from .synthetic import rolled_up, with_synthetic_rows


def base_facts(data: CompanyData) -> pl.DataFrame:
    """Facts with overrides and clones, canonical units, and conflicts removed.

    A slot whose latest filing still reports two values is a real conflict
    and is dropped; an earlier value that a later filing restated stays, so
    a quarter can still be derived from the annual figure it belongs to.
    """
    facts = with_fact_overrides(data, include_clones=True).with_columns(
        canonical_unit(pl.col("unit")).alias("unit")
    )
    return drop_conflicting(facts, ConflictRule.LATEST_FILING, data.table("filings"))


def run(data: CompanyData) -> pl.DataFrame:
    facts = base_facts(data)
    overrides = data.table("concept_normalization_overrides")
    hierarchy = data.table("hierarchy_normalization")
    resolved = resolve_facts(
        facts,
        overrides,
        hierarchy,
        data.table("concept_normalization"),
        data.table("dimension_normalization"),
        filing_currency(facts),
    )
    rows = data.keep("rows", with_synthetic_rows(resolved, facts, overrides, hierarchy))
    rolled = data.keep("rolled_up", rolled_up(rows))
    sizes = data.keep("chain_sizes", chain_sizes(rolled))
    ranked = data.keep("ranked", rank(rolled, data.table("filings"), sizes))
    return (
        mark_duplicates(ranked)
        .select(FINANCIAL_FACTS_NORMALIZED.columns)
        .cast(FINANCIAL_FACTS_NORMALIZED.schema)
    )
