"""Choose one row per series and period; mark the rest ``is_duplicate``.

Rows compete within (company, statement, label, axis, member, period end,
abstract or not) and rank: amendments first, then the period's own filing,
then a filing that did not recast the split, then the later filing, then a
published quarter over a year-to-date total of the same date, then the
longest comparative chain, then the lowest id. A row that rounds to the
winner's value, within 0.1%, then takes its place, so a later reprint in
millions never replaces the original's digits.
"""

from __future__ import annotations

import polars as pl

from ...rules import sql
from ...rules.periods import AMENDMENTS, form_family
from ...rules.rounding import differs_beyond_reprint, rounds_to

SLOT = [
    "company_id",
    "statement",
    "normalized_label",
    "axis",
    "member",
    "period_end",
    "is_abstract",
]


def fiscal_filings(filings: pl.DataFrame) -> pl.DataFrame:
    """The first filing of each form family that closed each period.

    A column for an earlier year is stored on the filing that printed it; its
    fiscal year and quarter come from this filing instead.
    """
    first = sql.distinct_on(
        filings, ["company_id", "fiscal_period_end", "form_type"], ["id"]
    )
    return first.select(
        "company_id",
        pl.col("fiscal_period_end").alias("period_end"),
        pl.col("form_type").alias("family"),
        pl.col("id").alias("fiscal_filing_id"),
    )


def recast_filings(rows: pl.DataFrame, filings: pl.DataFrame) -> pl.DataFrame:
    """Later filings that printed a member of an earlier period at another value.

    Such a filing recast that split (NVIDIA's fiscal 2025 10-K carving
    Singapore out of fiscal 2023 and 2024); none of its members for that
    period are shown. A filing that only adds members, rounded or not,
    extends the split instead.
    """
    period = filings.select(pl.col("id").alias("filing_id"), "fiscal_period_end")
    dims = rows.filter((pl.col("axis").fill_null("") != "") & ~pl.col("is_abstract"))
    dims = dims.join(period, on="filing_id")
    later = dims.filter(pl.col("fiscal_period_end") != pl.col("period_end"))
    own = dims.filter(pl.col("fiscal_period_end") == pl.col("period_end"))
    keys = [
        "company_id",
        "statement",
        "normalized_label",
        "axis",
        "member",
        "period_end",
    ]
    pairs = later.join(
        own.select(*keys, "period", pl.col("value").alias("own_value")),
        on=[*keys, "period"],
        nulls_equal=True,
    )
    recast = pairs.filter(
        differs_beyond_reprint(pl.col("own_value"), pl.col("value")).fill_null(False)
    )
    return recast.select(
        "company_id", "statement", "normalized_label", "axis", "period_end", "filing_id"
    ).unique()


def rank(
    rows: pl.DataFrame, filings: pl.DataFrame, sizes: pl.DataFrame
) -> pl.DataFrame:
    """Each row's rank in its slot, the slot winner's value and the recast flag."""
    filed = filings.select(
        pl.col("id").alias("filing_id"),
        pl.col("fiscal_period_end").alias("filed_period_end"),
        pl.col("filing_date").alias("filed_date"),
        pl.col("form_type").alias("filed_form"),
    )
    recast = recast_filings(rows, filings).with_columns(pl.lit(True).alias("is_recast"))
    ranked = (
        rows.join(sizes, on="id", how="left")
        .join(filed, on="filing_id", how="left")
        .join(
            recast,
            on=[
                "company_id",
                "statement",
                "normalized_label",
                "axis",
                "period_end",
                "filing_id",
            ],
            how="left",
        )
        .with_columns(
            pl.col("is_recast").fill_null(False),
            form_family(pl.col("filed_form")).alias("family"),
        )
        .join(
            fiscal_filings(filings),
            on=["company_id", "period_end", "family"],
            how="left",
        )
        .with_columns(
            pl.coalesce("fiscal_filing_id", "filing_id").alias("fiscal_filing_id")
        )
    )
    ranked = sql.row_number(
        ranked,
        SLOT,
        [
            sql.asc(
                pl.when(pl.col("form_type").is_in(AMENDMENTS)).then(0).otherwise(1)
            ),
            # The period's own filing beats a later reprint, which may be
            # recast under other members.
            sql.desc(
                pl.col("filed_period_end") == pl.col("period_end"), nulls_last=True
            ),
            sql.asc("is_recast"),
            sql.desc("filed_date", nulls_last=True),
            # A published quarter beats the year-to-date total of the same date.
            sql.asc(pl.when(pl.col("period") == "Q").then(0).otherwise(1)),
            sql.desc(pl.col("chain_size").fill_null(1)),
            sql.asc("id"),
            # One fact under two expanded parents appears twice; the copy
            # pointing at a real parent beats the one that needs a synthetic.
            sql.asc(pl.col("parent_concept").is_not_null()),
        ],
        name="filing_rank",
    )
    return ranked.with_columns(pl.col("value").first().over(SLOT).alias("winner_value"))


def mark_duplicates(ranked: pl.DataFrame) -> pl.DataFrame:
    """``is_duplicate`` per row, keeping the winner's digits when a reprint rounds."""
    final = sql.row_number(
        ranked,
        SLOT,
        [
            sql.asc("is_recast"),
            sql.desc(
                rounds_to(pl.col("value"), pl.col("winner_value")), nulls_last=True
            ),
            sql.asc(sql.is_multiple_of(pl.col("value"), 1_000)),
            sql.asc(sql.is_multiple_of(pl.col("value"), 1_000_000)),
            sql.asc("filing_rank"),
        ],
        name="rn",
    )
    marked = final.with_columns(
        ((pl.col("rn") > 1) | pl.col("is_recast")).alias("is_duplicate")
    )
    # A row repeated under several parents keeps the copy that won.
    return sql.distinct_on(marked, ["id"], ["is_duplicate"])
