"""Choose one row per series and period; mark the rest ``is_duplicate``.

Rows compete within (company, statement, label, axis, member, period end,
abstract or not) and rank: a hidden split last (a recast, or every split a
restatement replaced; see :func:`restating_filings`), then amendments, then
the period's own filing, then the later filing, then a
published quarter over a year-to-date total of the same date, then the
longest comparative chain, then the earliest filing, then the lowest id. A
row that rounds to the winner's value, within 0.1%, then takes its place, so
a later reprint in millions never replaces the original's digits.
"""

from __future__ import annotations

from decimal import Decimal

import polars as pl

from ...rules import ordering
from ...rules.periods import AMENDMENTS, form_family
from ...rules.rounding import (
    REPRINT_SHARE,
    differs_beyond_reprint,
    is_multiple_of,
    reprint_tolerance,
    rounds_to,
)

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
    first = ordering.distinct_on(
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

    A later filing can restate an earlier year's split by moving an amount
    out of some members into another, so the members it shares with the
    original filing no longer carry their original values. Mixing the two
    splits would count the moved amount twice. So when a later filing
    changes any shared member beyond rounding, it recast the period, and
    none of its members for that period are shown; the original split
    stands. For example, NVIDIA's fiscal 2025 10-K carved Singapore out of
    its other regions for fiscal 2023 and 2024. A filing that only adds
    members, rounded or not, extends the split instead.

    A change of one rounding unit is a re-rounding, not a recast: Alphabet's
    2017 first-quarter 10-Q reprinted the 2016 quarter with Google at 20,092
    and Other Bets at 165 million, one million off the original's 20,091 and
    166, and added the Google properties, Network and other revenues the
    original 10-Q had not tagged. Microsoft's fiscal 2024 10-K moving 178
    million into Enterprise and partner services for fiscal 2023 is a recast.
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
    # Both prints rounded to the same unit and one unit apart are a re-rounding.
    rerounded = (pl.col("own_value") - pl.col("value")).abs() <= 2 * reprint_tolerance(
        pl.col("value")
    )
    recast = pairs.filter(
        differs_beyond_reprint(pl.col("own_value"), pl.col("value")).fill_null(False)
        & ~rerounded
    )
    return recast.select(
        "company_id", "statement", "normalized_label", "axis", "period_end", "filing_id"
    ).unique()


#: One split: a line's members on one axis for one period.
SPLIT = [
    "company_id",
    "statement",
    "normalized_label",
    "axis",
    "period_end",
    "period",
]


def adds_up(members: pl.Expr, line: pl.Expr) -> pl.Expr:
    """``members`` equal ``line`` within 0.1%, what coarser reprints can miss by."""
    return (members - line).abs() <= line.abs() * REPRINT_SHARE


#: Wider splits are not searched for a subset that adds up.
SUBSET_SEARCH_LIMIT = 16


def _some_subset_adds_up(row: dict) -> bool:
    """Two or more of the members add up to the line (see :func:`split_adds_up`)."""
    line, values = row["line"], [v for v in row["values"] or [] if v]
    if line is None or not 2 <= len(values) <= SUBSET_SEARCH_LIMIT:
        return False
    tolerance = abs(line) * REPRINT_SHARE
    sums = {Decimal(0): 0}
    for value in values:
        for total, count in list(sums.items()):
            reached = total + value
            sums[reached] = max(sums.get(reached, 0), count + 1)
    return any(n >= 2 and abs(t - line) <= tolerance for t, n in sums.items())


def split_adds_up(values: pl.Expr, total: pl.Expr, line: pl.Expr) -> pl.Expr:
    """The members, or two or more of them, add up to ``line``.

    A split can carry a subtotal beside its parts: Alphabet's FY2015 10-K
    restated 2014 as Google (Google properties, Google Network and other
    revenues) beside Other Bets, so only Google and Other Bets add up.
    """
    nested = pl.struct(values.alias("values"), line.alias("line")).map_elements(
        _some_subset_adds_up, return_dtype=pl.Boolean
    )
    return adds_up(total, line).fill_null(False) | nested


def _splits(rows: pl.DataFrame, filings: pl.DataFrame) -> pl.DataFrame:
    """Every filing's split of every line: its members' total, values and the line.

    Columns: :data:`SPLIT`, ``filing_id``, ``is_own``, ``is_annual``,
    ``fiscal_year``, ``filing_date``, ``total``, ``values``, ``members`` and
    ``line``, the filing's undimensioned value of the line for that period.
    """
    info = filings.select(
        pl.col("id").alias("filing_id"),
        "fiscal_period_end",
        "filing_date",
        "fiscal_year",
        form_family(pl.col("form_type")).is_in(["10-K", "20-F"]).alias("is_annual"),
    )
    data = rows.filter(~pl.col("is_abstract")).join(info, on="filing_id")
    data = data.with_columns(
        (pl.col("fiscal_period_end") == pl.col("period_end")).alias("is_own")
    )
    line_keys = [k for k in SPLIT if k != "axis"]
    lines = ordering.distinct_on(
        data.filter(pl.col("axis").fill_null("") == ""),
        [*line_keys, "filing_id"],
        ["id"],
    ).select(*line_keys, "filing_id", pl.col("value").alias("line"))
    # A filing can print a member twice (the face and a note); count it once.
    members = ordering.distinct_on(
        data.filter(pl.col("axis").fill_null("") != ""),
        [*SPLIT, "filing_id", "member"],
        ["id"],
    )
    return (
        members.group_by(
            [*SPLIT, "filing_id", "is_own", "is_annual", "fiscal_year", "filing_date"]
        )
        .agg(
            pl.col("value").sum().alias("total"),
            pl.col("value").alias("values"),
            pl.col("member").unique().sort().alias("members"),
        )
        .join(lines, on=[*line_keys, "filing_id"], how="left")
    )


def restating_filings(
    rows: pl.DataFrame, filings: pl.DataFrame, recast: pl.DataFrame
) -> pl.DataFrame:
    """The later filing whose restated split replaces the split as filed.

    A filer can change what its segment measure includes and restate the
    earlier years on the new basis. Amazon's 2015 segment operating income
    left out stock-based compensation and added up to 4,523 beside an
    operating income of 2,233; its 2016 10-K charged it to the segments and
    restated 2015 to 1,425 + (699) + 1,507 = 2,233.

    A fiscal year is restated on one axis when its annual split misses the
    line by more than 0.1% and a later annual filing's members, every member
    the original had among them, add up to the line as filed. A restating
    filing prints earlier lines only as comparatives, if at all, so its
    members meeting the original line is what shows the line itself was not
    restated. The year's quarters then follow, each onto a later filing that
    recast it, keeps every member it had and adds up to the quarter's line
    where the quarter has one. If any quarter with a split cannot follow,
    the whole year stays as filed, so a year is never shown half on one
    basis (Q4 is the 10-K minus the first three quarters). The earliest such
    filing wins; it is the one next year's 10-Qs, which restate the quarters,
    belong with. A year whose annual split adds up, or that no later split
    fixes, stays as filed, recast or not.
    """
    splits = _splits(rows, filings)
    # One filing of the period, no amendment beside it.
    own = splits.filter(pl.col("is_own")).filter(pl.len().over(SPLIT) == 1)
    pairs = (
        splits.filter(~pl.col("is_own"))
        .join(
            own.select(
                *SPLIT,
                pl.col("line").alias("own_line"),
                pl.col("total").alias("own_total"),
                pl.col("values").alias("own_values"),
                pl.col("members").alias("own_members"),
                pl.col("is_annual").alias("own_is_annual"),
                pl.col("fiscal_year").alias("own_year"),
            ),
            on=SPLIT,
            nulls_equal=True,
        )
        .filter(
            pl.col("own_members").list.set_difference(pl.col("members")).list.len() == 0
        )
    )
    year = ["company_id", "statement", "normalized_label", "axis", "own_year"]
    annual = pairs.filter(
        pl.col("own_is_annual")
        & pl.col("is_annual")
        & pl.col("own_line").is_not_null()
        & ~split_adds_up(pl.col("own_values"), pl.col("own_total"), pl.col("own_line"))
        & split_adds_up(pl.col("values"), pl.col("total"), pl.col("own_line"))
    )
    recast_keys = [
        "company_id",
        "statement",
        "normalized_label",
        "axis",
        "period_end",
        "filing_id",
    ]
    # A quarter with a line of its own must add up to it on the new basis;
    # a discrete second or third quarter has only the year-to-date line.
    quarters = (
        pairs.filter(~pl.col("own_is_annual") & ~pl.col("is_annual"))
        .filter(
            pl.col("own_line").is_null()
            | split_adds_up(pl.col("values"), pl.col("total"), pl.col("own_line"))
        )
        .join(annual.select(year).unique(), on=year)
        .join(recast.select(recast_keys).unique(), on=recast_keys)
    )
    # Every quarter of the year that has a split of its own must follow, or
    # the year stays as filed: Q4 is the 10-K minus Q1-Q3, so a restated
    # 10-K beside quarters on the old basis would put the difference in Q4.
    own_quarters = (
        splits.filter(pl.col("is_own") & ~pl.col("is_annual"))
        .select(*SPLIT, pl.col("fiscal_year").alias("own_year"))
        .unique()
    )
    followed = quarters.select(*SPLIT).unique().with_columns(pl.lit(True).alias("ok"))
    unfollowed = (
        own_quarters.join(annual.select(year).unique(), on=year)
        .join(followed, on=SPLIT, how="left", nulls_equal=True)
        .filter(pl.col("ok").is_null())
        .select(year)
        .unique()
    )
    best = ordering.distinct_on(
        pl.concat([annual, quarters]).join(unfollowed, on=year, how="anti"),
        SPLIT,
        [ordering.asc("filing_date"), ordering.asc("filing_id")],
    )
    return best.select(*SPLIT, pl.col("filing_id").alias("restating_filing_id"))


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
    restated = restating_filings(rows, filings, recast)
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
        .join(restated, on=SPLIT, how="left", nulls_equal=True)
        .with_columns(
            # A restated split replaces every other filing's members of the
            # period, so the two are never mixed; otherwise a recast is hidden.
            pl.when(pl.col("restating_filing_id").is_not_null())
            .then(pl.col("filing_id") != pl.col("restating_filing_id"))
            .otherwise(pl.col("is_recast").fill_null(False))
            .alias("is_recast"),
            form_family(pl.col("filed_form")).alias("family"),
        )
        .drop("restating_filing_id")
        .join(
            fiscal_filings(filings),
            on=["company_id", "period_end", "family"],
            how="left",
        )
        .with_columns(
            pl.coalesce("fiscal_filing_id", "filing_id").alias("fiscal_filing_id")
        )
    )
    ranked = ordering.row_number(
        ranked,
        SLOT,
        [
            ordering.asc("is_recast"),
            ordering.asc(
                pl.when(pl.col("form_type").is_in(AMENDMENTS)).then(0).otherwise(1)
            ),
            # The period's own filing beats a later reprint, which may be
            # recast under other members.
            ordering.desc(
                pl.col("filed_period_end") == pl.col("period_end"), nulls_last=True
            ),
            ordering.desc("filed_date", nulls_last=True),
            # A published quarter beats the year-to-date total of the same date.
            ordering.asc(pl.when(pl.col("period") == "Q").then(0).otherwise(1)),
            ordering.desc(pl.col("chain_size").fill_null(1)),
            # Two filings of one period (a predecessor and its successor filing
            # the same quarter): every row comes from the same one, so a
            # statement never mixes them. Synthetic ids are hashes.
            ordering.asc("filing_id"),
            ordering.asc("id"),
            # One fact under two expanded parents appears twice; the copy
            # pointing at a real parent beats the one that needs a synthetic.
            ordering.asc(pl.col("parent_concept").is_not_null()),
        ],
        name="filing_rank",
    )
    return ranked.with_columns(pl.col("value").first().over(SLOT).alias("winner_value"))


def mark_duplicates(ranked: pl.DataFrame) -> pl.DataFrame:
    """``is_duplicate`` per row, keeping the winner's digits when a reprint rounds."""
    final = ordering.row_number(
        ranked,
        SLOT,
        [
            ordering.asc("is_recast"),
            ordering.desc(
                rounds_to(pl.col("value"), pl.col("winner_value")), nulls_last=True
            ),
            ordering.asc(is_multiple_of(pl.col("value"), 1_000)),
            ordering.asc(is_multiple_of(pl.col("value"), 1_000_000)),
            ordering.asc("filing_rank"),
        ],
        name="rn",
    )
    marked = final.with_columns(
        ((pl.col("rn") > 1) | pl.col("is_recast")).alias("is_duplicate")
    )
    # A row repeated under several parents keeps the copy that won.
    return ordering.distinct_on(marked, ["id"], ["is_duplicate"])
