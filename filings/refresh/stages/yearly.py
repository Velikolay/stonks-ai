"""Stage 8: the annual presentation layer.

Reads ``financial_facts_normalized``, ``filings`` and ``financial_rewrites``;
writes ``yearly_financials``. Every non-duplicate 10-K and 20-F row, placed
in the fiscal year of the filing that closed its period, then the yearly
rewrites. A series takes its header and position from its latest period, so
a line stays put in the tree when the filer reorders statements.
"""

from __future__ import annotations

import polars as pl

from ..inputs import CompanyData
from ..io.schemas import YEARLY_FINANCIALS
from ..rules import ordering
from ..rules.periods import ANNUAL_FORMS, FOREIGN_ANNUAL_FORMS, fiscal_year_of
from ..rules.rewrites import apply_rewrites

SERIES = ["company_id", "statement", "normalized_label", "axis", "member"]


def with_latest_tree_position(rows: pl.DataFrame) -> pl.DataFrame:
    """Each row's header and position as of its series' latest period.

    A section header and a total can share a label. They are separate series,
    otherwise one takes the other's pointer and a header can point at itself.
    """
    series = [*SERIES, "is_abstract"]
    ordered = ordering.sort(rows, [ordering.desc("period_end"), ordering.asc("id")])
    return ordered.with_columns(
        pl.col("abstract_id").first().over(series).alias("abstract_id"),
        pl.col("position").first().over(series).alias("position"),
    )


def annual_rows(data: CompanyData) -> pl.DataFrame:
    filings = data.table("filings").select(
        pl.col("id").alias("fiscal_filing_id"),
        pl.col("fiscal_year").alias("filing_fiscal_year"),
        pl.col("fiscal_period_end").alias("filing_period_end"),
    )
    facts = data.table("financial_facts_normalized").filter(
        pl.col("form_type").is_in([*ANNUAL_FORMS, *FOREIGN_ANNUAL_FORMS])
        & ~pl.col("is_duplicate")
    )
    joined = facts.join(filings, on="fiscal_filing_id", how="inner").with_columns(
        fiscal_year_of(
            pl.col("filing_fiscal_year"),
            pl.col("filing_period_end"),
            pl.col("period_end"),
        ).alias("fiscal_year"),
        pl.when(pl.col("form_type").is_in(FOREIGN_ANNUAL_FORMS))
        .then(pl.lit("20-F"))
        .otherwise(pl.lit("10-K"))
        .alias("source_type"),
    )
    return with_latest_tree_position(joined)


def run(data: CompanyData) -> pl.DataFrame:
    rows = (
        annual_rows(data)
        .select(YEARLY_FINANCIALS.columns)
        .cast(YEARLY_FINANCIALS.dtypes)
    )
    return apply_rewrites(
        rows, data.table("financial_rewrites"), "yearly", data.company_id
    )
