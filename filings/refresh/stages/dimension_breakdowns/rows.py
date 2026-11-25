"""The 10-K amounts a breakdown is searched over, and the tolerance it must meet.

Only rows that survive dedup take part, so a breakdown holds on the values
the presentation tables show. Per-share amounts are not additive.
"""

from __future__ import annotations

from decimal import Decimal

import polars as pl

from ...inputs import CompanyData
from ...rules.periods import ANNUAL_FORMS, FOREIGN_ANNUAL_FORMS, fiscal_year_of
from ...rules.rounding import is_multiple_of

PERIOD = ["company_id", "statement", "normalized_label", "period_end"]
AXIS_PERIOD = ["company_id", "statement", "normalized_label", "axis", "period_end"]
BREAKDOWN = [
    "company_id",
    "statement",
    "normalized_label",
    "axis",
    "parent_member",
    "members",
]
#: :data:`BREAKDOWN` with the member list as one string, for grouping.
BREAKDOWN_KEY = [*BREAKDOWN[:-1], "members_key"]


def with_members_key(frame: pl.DataFrame) -> pl.DataFrame:
    """Add ``members_key``: the member list joined, since lists cannot be grouped on."""
    return frame.with_columns(pl.col("members").list.join("\x1f").alias("members_key"))


def rounding(value: pl.Expr) -> pl.Expr:
    """The unit a value looks rounded to: 1,000,000, 1,000, or 1 (also for 0)."""
    return (
        pl.when(value == 0)
        .then(1)
        .when(is_multiple_of(value, 1_000_000))
        .then(1_000_000)
        .when(is_multiple_of(value, 1_000))
        .then(1_000)
        .otherwise(1)
        .cast(pl.Int64)
    )


def within_tolerance(
    parent_value: Decimal,
    parent_rounding: int,
    members_total: Decimal,
    members_rounding: int,
    count: int,
) -> bool:
    """Members add up to the parent, allowing for members printed coarser.

    Members rounded coarser than their parent (millions a later 10-K
    reprinted beside a total in thousands) may miss it by half a unit each,
    up to 0.1%. Otherwise the sum must be exact.
    """
    gap = abs(parent_value - members_total)
    if members_rounding > parent_rounding:
        return gap <= min(
            Decimal(count * members_rounding) / 2, abs(parent_value) / 1000
        )
    return gap == 0


def within_tolerance_expr(
    parent_value: pl.Expr,
    parent_rounding: pl.Expr,
    members_total: pl.Expr,
    members_rounding: pl.Expr,
    count: pl.Expr,
) -> pl.Expr:
    """:func:`within_tolerance` as a polars expression."""
    gap = (parent_value - members_total).abs()
    allowed = pl.min_horizontal(
        (count * members_rounding).cast(pl.Decimal(38, 10)) / 2,
        parent_value.abs() / 1000,
    )
    return (
        pl.when(members_rounding > parent_rounding)
        .then(gap <= allowed)
        .otherwise(gap == 0)
    )


def breakdown_rows(data: CompanyData) -> pl.DataFrame:
    """Non-duplicate, non-per-share 10-K and 20-F amounts with their fiscal year."""
    filings = data.table("filings").select(
        pl.col("id").alias("fiscal_filing_id"),
        pl.col("fiscal_year").alias("filing_fiscal_year"),
        pl.col("fiscal_period_end").alias("filing_period_end"),
    )
    rows = data.table("financial_facts_normalized").filter(
        pl.col("form_type").is_in([*ANNUAL_FORMS, *FOREIGN_ANNUAL_FORMS])
        & ~pl.col("is_duplicate")
        & ~pl.col("is_abstract")
        & pl.col("value").is_not_null()
        & ~pl.col("unit").fill_null("").str.to_lowercase().str.contains("pershare")
    )
    return rows.join(filings, on="fiscal_filing_id", how="inner").select(
        "id",
        "company_id",
        "statement",
        "normalized_label",
        "axis",
        "member",
        "period_end",
        fiscal_year_of(
            pl.col("filing_fiscal_year"),
            pl.col("filing_period_end"),
            pl.col("period_end"),
        ).alias("fiscal_year"),
        "value",
        rounding(pl.col("value")).alias("rounding"),
    )


def real_dimensions(data: CompanyData, rows: pl.DataFrame) -> pl.DataFrame:
    """Each row's stored XBRL dimensions, without the axis defaults."""
    return (
        data.table("financial_fact_dimensions")
        .filter(~pl.col("is_default"))
        .join(rows.select(pl.col("id").alias("fact_id")), on="fact_id", how="semi")
    )
