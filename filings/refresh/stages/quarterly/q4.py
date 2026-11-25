"""Q4: the annual figure minus the first three quarters.

Q4 is only calculated when the series has its three earlier quarters, or
their nine-month total. A later 10-K can recast the year under new members,
so every member's Q4 on one axis comes from one 10-K, chosen in order:

1. an amendment,
2. the year's own 10-K,
3. a 10-K on the quarters' split (its members all have quarters, and every
   member with quarters is in it),
4. the first 10-K filed after the quarters (a backfilled comparative does
   not count), then the latest before them.

A member that 10-K leaves out comes from the next one in that order. The
nine months subtracted come from a 10-Q on the same split as that 10-K
(see :func:`nine_month_split`); without one, Q1-Q3 are summed.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

import polars as pl

from ...rules import sql
from ...rules.periods import ANNUAL_FORMS, QUARTERLY_FORMS, quarter_apart
from ...rules.rounding import reprint_tolerance_of
from .rows import SERIES, WEIGHTED_AVERAGE_SHARES, is_per_share

AXIS_YEAR = ["company_id", "statement", "normalized_label", "axis", "fiscal_tag"]
MEMBER_YEAR = [*SERIES, "fiscal_tag"]
#: A per-share Q4 is only derived when Q1-Q3 sit under the 10-K's figure; a
#: mid-year stock split leaves them on another share basis.
PER_SHARE_SLACK = (Decimal("1.05"), Decimal("0.05"))


def quarters_by_year(restated: pl.DataFrame, filings: pl.DataFrame) -> pl.DataFrame:
    """Q1-Q3 per member and fiscal year, as one amount to subtract from the 10-K.

    Three restated quarters sum to the restated basis. Otherwise three
    discrete quarters sum, unless a published Q3 kept the nine-month total,
    since the earlier quarters can be on a previous classification; then,
    or when a quarter is missing, the nine-month total is used.
    """
    dates = filings.select(pl.col("id").alias("filing_id"), "filing_date")
    rows = restated.join(dates, on="filing_id", how="inner")
    shown = ~pl.col("drop_ytd")
    return rows.group_by(MEMBER_YEAR).agg(
        # A backfilled comparative was printed a year later; it is not when
        # the quarters were filed.
        pl.col("filing_date")
        .filter(~pl.col("is_comparative_backfill"))
        .max()
        .alias("filed"),
        pl.when(pl.col("restated_value").count() == 3)
        .then(pl.col("restated_value").sum())
        .when(
            (pl.col("fiscal_quarter").filter(shown).drop_nulls().n_unique() == 3)
            & ~pl.col("prefer_ytd_anchor").fill_null(False).any()
        )
        .then(sql.sum_or_null(pl.col("value").filter(shown)))
        .otherwise(pl.col("q3_ytd").max())
        .alias("value"),
    )


def annual_members(data_rows: pl.DataFrame, filings: pl.DataFrame) -> pl.DataFrame:
    """Every 10-K's members for each year, including recasts that lost in dedup."""
    src = filings.select(
        pl.col("id").alias("filing_id"),
        "filing_date",
        (pl.col("form_type") == "10-K/A").alias("is_amendment"),
    )
    fiscal = filings.select(
        pl.col("id").alias("fiscal_filing_id"),
        pl.col("fiscal_period_end").alias("period_end"),
        pl.col("fiscal_year").alias("f_fiscal_year"),
        pl.col("fiscal_quarter").alias("f_fiscal_quarter"),
        pl.col("id").alias("fiscal_tag"),
    )
    rows = data_rows.filter(
        pl.col("form_type").is_in(ANNUAL_FORMS)
        & (pl.col("period") == "YTD")
        & (pl.col("axis") != "")
        & ~pl.col("is_abstract")
    )
    return (
        rows.join(src, on="filing_id", how="inner")
        .join(fiscal, on=["fiscal_filing_id", "period_end"], how="inner")
        .with_columns(
            pl.col("f_fiscal_year").alias("fiscal_year"),
            pl.col("f_fiscal_quarter").alias("fiscal_quarter"),
        )
        .drop("f_fiscal_year", "f_fiscal_quarter")
    )


def nine_months(
    data_rows: pl.DataFrame, filings: pl.DataFrame, tags: pl.DataFrame
) -> pl.DataFrame:
    """Every 10-Q that printed the nine months before a 10-K.

    Includes reprints a year later under that year's members.
    """
    src = filings.select(pl.col("id").alias("filing_id"), "filing_date")
    fiscal = tags.select(
        pl.col("id").alias("fiscal_filing_id"),
        pl.col("fiscal_period_end").alias("period_end"),
        "fiscal_tag",
    )
    annual = filings.select(
        pl.col("id").alias("fiscal_tag"),
        pl.col("fiscal_period_end").alias("annual_period_end"),
    )
    rows = (
        data_rows.filter(
            pl.col("form_type").is_in(QUARTERLY_FORMS)
            & (pl.col("period") == "YTD")
            & (pl.col("axis") != "")
            & ~pl.col("is_abstract")
        )
        .join(src, on="filing_id", how="inner")
        .join(fiscal, on=["fiscal_filing_id", "period_end"], how="inner")
        .join(annual, on="fiscal_tag", how="inner")
        .filter(quarter_apart(pl.col("annual_period_end"), pl.col("period_end")))
    )
    keys = [*SERIES, "filing_id", "fiscal_tag"]
    return sql.distinct_on(rows, keys, ["is_duplicate", "id"]).select(
        *keys, "filing_date", "value"
    )


def _member_sets(frame: pl.DataFrame) -> dict[tuple, dict[int, tuple]]:
    """``(line, axis, year) -> filing -> (filing_date, members)``."""
    grouped = frame.group_by(*AXIS_YEAR, "filing_id", "filing_date").agg(
        pl.col("member").unique().alias("members")
    )
    out: dict[tuple, dict[int, tuple]] = {}
    for r in grouped.iter_rows(named=True):
        key = tuple(r[c] for c in AXIS_YEAR)
        out.setdefault(key, {})[r["filing_id"]] = (
            r["filing_date"],
            frozenset(r["members"]),
        )
    return out


def _values(frame: pl.DataFrame) -> dict[tuple, dict[str, list]]:
    """``(line, axis, year, filing) -> member -> [values]``."""
    out: dict[tuple, dict[str, list]] = {}
    for r in frame.select(*AXIS_YEAR, "filing_id", "member", "value").iter_rows():
        out.setdefault(r[:6], {}).setdefault(r[6], []).append(r[7])
    return out


def _prints_year_as(own: dict[str, list], later: dict[str, list]) -> bool:
    """No member shared by both filings changed beyond the later one's rounding."""
    return not any(
        abs(o - v) > reprint_tolerance_of(v)
        for member, own_values in own.items()
        for o in own_values
        for v in later.get(member, ())
        if o is not None and v is not None
    )


def nine_month_split(annual: pl.DataFrame, nines: pl.DataFrame) -> pl.DataFrame:
    """For each 10-K of a year, the 10-Q whose nine months it subtracts.

    A 10-Q a year later is on the 10-K's split when the first 10-K after
    that 10-Q prints the year as this 10-K did: no shared member at another
    value beyond rounding. Its reprint is then used when all its members are
    the 10-K's, which covers a 10-K that moved a line out of another
    (Tesla's fiscal 2021 10-K taking regulatory credits out of automotive
    sales). Otherwise a 10-Q with the same members, the year's own first, so
    a 10-K that recast its year pairs with the 10-Q that reprinted the nine
    months that way and not with quarters filed on the earlier members.
    """
    annual_sets = _member_sets(annual)
    nine_sets = _member_sets(nines)
    annual_values = _values(annual)

    out = []
    for key, filings in annual_sets.items():
        for a_id, (a_date, a_members) in filings.items():
            best: Optional[tuple[tuple[bool, bool, int, int], int]] = None
            for n_id, (n_date, n_members) in nine_sets.get(key, {}).items():
                if not n_members <= a_members:
                    continue
                later = [
                    (b_date, b_id)
                    for b_id, (b_date, _) in filings.items()
                    if b_date > n_date and n_date > a_date
                ]
                on_split = False
                if later:
                    _, next_id = min(later)
                    on_split = _prints_year_as(
                        annual_values.get((*key, a_id), {}),
                        annual_values.get((*key, next_id), {}),
                    )
                if not (on_split or n_members == a_members):
                    continue
                rank = (
                    not on_split,
                    not (n_date < a_date),
                    abs((n_date - a_date).days),
                    n_id,
                )
                if best is None or rank < best[0]:
                    best = (rank, n_id)
            if best is not None:
                out.append((*key, a_id, best[1]))
    return pl.DataFrame(
        out,
        schema={
            "company_id": pl.Int32,
            "statement": pl.String,
            "normalized_label": pl.String,
            "axis": pl.String,
            "fiscal_tag": pl.Int32,
            "filing_id": pl.Int32,
            "nine_month_filing_id": pl.Int32,
        },
        orient="row",
    )


def annual_split(annual: pl.DataFrame, quarters: pl.DataFrame) -> pl.DataFrame:
    """Rank the 10-Ks of each line, axis and year as the source of Q4."""
    with_quarters = quarters.filter(pl.col("value").is_not_null())
    members_with_quarters = with_quarters.group_by(AXIS_YEAR).agg(
        pl.len().alias("quarter_members")
    )
    joined = annual.join(
        quarters.select(*MEMBER_YEAR, pl.col("value").alias("q_value"), "filed"),
        on=MEMBER_YEAR,
        how="left",
    ).join(members_with_quarters, on=AXIS_YEAR, how="left")
    has_quarters = pl.col("q_value").is_not_null() | (pl.col("value") == 0)
    candidates = joined.group_by(
        *AXIS_YEAR, "filing_id", "filing_date", "is_amendment"
    ).agg(
        (
            sql.bool_and(has_quarters)
            & (
                pl.col("member").filter(pl.col("q_value").is_not_null()).n_unique()
                == pl.col("quarter_members").max()
            )
        ).alias("matches_quarters"),
        pl.col("filed").max().alias("filed"),
    )
    candidates = candidates.with_columns(
        pl.col("filed").max().over(AXIS_YEAR).alias("quarters_filed")
    )
    after = pl.col("filing_date") >= pl.col("quarters_filed")
    return sql.row_number(
        candidates,
        AXIS_YEAR,
        [
            sql.desc("is_amendment"),
            sql.desc(pl.col("filing_id") == pl.col("fiscal_tag")),
            sql.desc("matches_quarters", nulls_last=True),
            sql.desc(after),
            sql.asc(pl.when(after).then(pl.col("filing_date"))),
            sql.desc("filing_date"),
            sql.desc("filing_id"),
        ],
        name="split_rank",
    ).select(*AXIS_YEAR, "filing_id", "split_rank")


def _per_share_fits(nine: pl.Expr, annual: pl.Expr) -> pl.Expr:
    factor, slack = PER_SHARE_SLACK
    return ~is_per_share(pl.col("unit")) | (
        nine.abs() <= annual.abs() * sql.decimal(factor) + sql.decimal(slack)
    )


def member_q4(
    annual: pl.DataFrame,
    quarters: pl.DataFrame,
    splits: pl.DataFrame,
    nine_split: pl.DataFrame,
    nines: pl.DataFrame,
) -> pl.DataFrame:
    """Q4 of every dimensioned member, from its year's best-ranked 10-K.

    A member of the first-ranked 10-K gets Q4 from those nine months even
    when its Q1-Q3 are hidden as a recast (Amazon's 2016 10-K introduced new
    categories, and only the 2017 10-Qs printed 2016's quarters on them).
    """
    rows = (
        splits.join(annual, on=[*AXIS_YEAR, "filing_id"])
        .join(
            quarters.select(*MEMBER_YEAR, pl.col("value").alias("q_value")),
            on=MEMBER_YEAR,
            how="left",
        )
        .join(nine_split, on=[*AXIS_YEAR, "filing_id"], how="left")
        .join(
            nines.select(
                *MEMBER_YEAR,
                pl.col("filing_id").alias("nine_month_filing_id"),
                pl.col("value").alias("nm_value"),
            ),
            on=[*MEMBER_YEAR, "nine_month_filing_id"],
            how="left",
        )
    )
    subtract = pl.coalesce("nm_value", "q_value")
    eligible = (
        (
            pl.col("q_value").is_not_null()
            | ((pl.col("split_rank") == 1) & pl.col("nm_value").is_not_null())
        )
        & (pl.col("statement") != "Balance Sheet")
        & ~pl.col("concept").is_in(WEIGHTED_AVERAGE_SHARES)
        & _per_share_fits(subtract, pl.col("value"))
    )
    chosen = sql.distinct_on(
        rows.filter(eligible), MEMBER_YEAR, ["split_rank", "is_duplicate", "id"]
    )
    return chosen.with_columns(
        (pl.col("value") - subtract).alias("value"),
        pl.lit("calculated").alias("source_type"),
    )


def line_q4(annual_rows: pl.DataFrame, quarters: pl.DataFrame) -> pl.DataFrame:
    """Q4 of every undimensioned flow line: its 10-K minus Q1-Q3."""
    rows = annual_rows.join(
        quarters.select(*MEMBER_YEAR, pl.col("value").alias("q_value")), on=MEMBER_YEAR
    )
    eligible = (
        (pl.col("axis").fill_null("") == "")
        & (pl.col("statement") != "Balance Sheet")
        # Instant stocks are not annual minus quarters.
        & pl.col("period").is_not_null()
        & ~pl.col("concept").is_in(WEIGHTED_AVERAGE_SHARES)
        & _per_share_fits(pl.col("q_value"), pl.col("value"))
        & pl.col("q_value").is_not_null()
        & ~pl.col("is_abstract")
    )
    return rows.filter(eligible).with_columns(
        (pl.col("value") - pl.col("q_value")).alias("value"),
        pl.lit("calculated").alias("source_type"),
    )
