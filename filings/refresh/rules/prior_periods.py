"""Periods before a company's first filing, from its earliest comparatives.

EDGAR's XBRL starts in 2009, and a company's first tagged filings print the
year before beside their own period: the first 10-K the prior fiscal year,
each 10-Q the same quarter a year earlier. Face rows store that column only
as ``comparative_value``, so nothing ever reported the period itself.

A period earlier than the company's first filing, printed exactly a year
back (340 to 380 days) as the comparative of a filing's own column, gets a
*prior filing*: a filing that was never ingested, holding what one real
filing printed for that period. Its id is the source filing's id negated, so
a row's provenance stays readable, and it takes the source's form family,
filing date and URL, the fiscal quarter of the source and the fiscal year
before it. Each period has one source, so a year is never stitched from two
filings: amendments first, as dedup ranks them, then the later filing.

Its rows are copies of the source's face rows carrying the comparative as
their value, with the statement tree copied alongside, and the source's own
rows for that period (dimension columns, which are stored per column) moved
onto it. A copy keeps the source fact's overrides, since it was tagged by the
same filing. Downstream, the prior filing is that period's own filing:
dedup prefers it, the yearly and quarterly layers place its rows in its
fiscal year, and Q4 is its 10-K minus its 10-Qs' nine months.

The quarterly layer keeps a prior quarter only where its year adds up
(``on_own_basis`` in :mod:`..stages.quarterly.ytd`) and its series reaches
the first filing without a gap (:func:`attached_early_quarters`).
"""

from __future__ import annotations

import polars as pl

from ..inputs import CompanyData
from . import ordering
from .ids import abs_hash_expr
from .periods import AMENDMENTS, YEAR_GAP_DAYS, days_between, form_family, within

SOURCE = ["company_id", "source_filing_id", "period_end"]
#: The statements whose amounts a quarter is derived from; a 10-K/A that only
#: corrects a share count (Tesla's FY2011 options exercised) restates nothing.
FLOW_STATEMENTS = ("Income Statement", "Cash Flow Statement")
#: Copy id to its source fact's id, which dedup breaks a copy's ties by.
TIES = pl.Schema({"id": pl.Int64, "tie_id": pl.Int64})


def is_prior_filing(filing_id: pl.Expr) -> pl.Expr:
    return filing_id < 0


# --- Which periods, from which filing ------------------------------------------


def prior_sources(facts: pl.DataFrame, filings: pl.DataFrame) -> pl.DataFrame:
    """``company_id, source_filing_id, period_end``: one source per prior period.

    A source prints the period as the year-back comparative of its own
    column. The period must precede every filing of the company; one that a
    10-K closes is not also a 10-Q period. A prior year whose 10-K source
    restated it (:func:`restated_years`) keeps no quarter printed before
    that restatement.
    """
    filed = filings.select(
        pl.col("id").alias("filing_id"),
        pl.col("fiscal_period_end").alias("filing_period_end"),
        pl.col("form_type").alias("filing_form"),
        "filing_date",
    )
    first = filings.group_by("company_id").agg(
        pl.col("fiscal_period_end").min().alias("first_period_end")
    )
    printed = (
        facts.filter(pl.col("comparative_period_end").is_not_null())
        .join(filed, on="filing_id", how="inner")
        .join(first, on="company_id", how="inner")
        .filter(
            (pl.col("period_end") == pl.col("filing_period_end"))
            & (pl.col("comparative_period_end") < pl.col("first_period_end"))
            & within(
                days_between(pl.col("period_end"), pl.col("comparative_period_end")),
                YEAR_GAP_DAYS,
            )
        )
    )
    candidates = (
        printed.select(
            "company_id",
            pl.col("filing_id").alias("source_filing_id"),
            pl.col("comparative_period_end").alias("period_end"),
            form_family(pl.col("filing_form")).alias("family"),
            pl.col("filing_form").is_in(AMENDMENTS).alias("is_amendment"),
            "filing_date",
        )
        .group_by(pl.all())
        .len("facts")
    )
    # A filing is the source of one period: the one most of its rows print.
    candidates = ordering.distinct_on(
        candidates,
        ["source_filing_id"],
        [ordering.desc("facts"), ordering.desc("period_end")],
    )
    chosen = ordering.distinct_on(
        candidates,
        ["company_id", "family", "period_end"],
        [
            ordering.desc("is_amendment"),
            ordering.desc("filing_date"),
            ordering.desc("source_filing_id"),
        ],
    )
    annual = chosen.filter(pl.col("family") == "10-K")
    quarters = chosen.filter(pl.col("family") != "10-K").join(
        annual, on=["company_id", "period_end"], how="anti"
    )
    stale = quarters.join(
        restated_years(printed, candidates, annual), on="company_id", how="inner"
    ).filter(
        (pl.col("period_end") < pl.col("year_end"))
        & (days_between(pl.col("year_end"), pl.col("period_end")) < YEAR_GAP_DAYS[1])
        & (pl.col("filing_date") < pl.col("restated_on"))
    )
    quarters = quarters.join(
        stale.select("source_filing_id"), on="source_filing_id", how="anti"
    )
    return pl.concat([annual, quarters]).select(SOURCE).sort(SOURCE)


def restated_years(
    printed: pl.DataFrame, candidates: pl.DataFrame, annual: pl.DataFrame
) -> pl.DataFrame:
    """``company_id, year_end, restated_on``: prior years a later 10-K restated.

    Two 10-Ks print the year as a comparative (a 10-K and its 10-K/A) and
    give an undimensioned income or cash flow amount two values; the chosen,
    later one restated it on the date it was filed. Apple's FY2009 10-K/A
    applied the new revenue rules to fiscal 2008 (37,491 against 32,479), so
    the nine months its earlier third-quarter 10-Q reprinted are on the old
    basis and Q4 would mix the two. Only a line each filing prints once is
    compared: cash and equity appear as beginning and ending balances under
    one concept.
    """
    tenks = candidates.filter(pl.col("family") == "10-K").select(
        "company_id", "source_filing_id", "period_end"
    )
    line = ["company_id", "comparative_period_end", "statement", "concept", "period"]
    once = (
        printed.filter(
            (pl.col("axis").fill_null("") == "")
            & pl.col("statement").is_in(FLOW_STATEMENTS)
            & ~pl.col("unit").fill_null("").str.contains("(?i)share")
        )
        .join(
            tenks,
            left_on=["company_id", "filing_id", "comparative_period_end"],
            right_on=["company_id", "source_filing_id", "period_end"],
            how="inner",
        )
        .group_by(*line, "filing_id")
        .agg(pl.col("comparative_value").unique())
        .filter(pl.col("comparative_value").list.len() == 1)
        .explode("comparative_value")
    )
    disagree = (
        once.group_by(line)
        .agg(
            pl.col("filing_id").n_unique().alias("filings"),
            pl.col("comparative_value").n_unique().alias("values"),
        )
        .filter((pl.col("filings") > 1) & (pl.col("values") > 1))
        .select("company_id", pl.col("comparative_period_end").alias("period_end"))
        .unique()
    )
    return annual.join(disagree, on=["company_id", "period_end"], how="inner").select(
        "company_id",
        pl.col("period_end").alias("year_end"),
        pl.col("filing_date").alias("restated_on"),
    )


# --- Prior filings and their rows ----------------------------------------------


def with_prior_filings(filings: pl.DataFrame, sources: pl.DataFrame) -> pl.DataFrame:
    """``filings`` plus the prior filing of each source.

    The form is the family's (``10-K`` for a 10-K/A), since a prior filing is
    the period's only filing; its rows keep the source's form.
    """
    if sources.is_empty():
        return filings
    source = filings.rename({"id": "source_filing_id"}).drop(
        "company_id", "fiscal_period_end"
    )
    prior = (
        sources.join(source, on="source_filing_id", how="inner")
        .with_columns(
            (-pl.col("source_filing_id")).alias("id"),
            form_family(pl.col("form_type")).alias("form_type"),
            pl.col("period_end").alias("fiscal_period_end"),
            (pl.col("fiscal_year") - 1).alias("fiscal_year"),
            pl.lit(False).alias("is_shell"),
        )
        .select(pl.col(c).cast(t) for c, t in filings.schema.items())
    )
    return pl.concat([filings, prior])


def filings_with_prior(data: CompanyData) -> pl.DataFrame:
    """The company's filings and the prior filings its normalized rows name.

    For the stages after ``facts_normalized``, which read its rows and never
    the raw comparatives.
    """
    sources = (
        data.table("financial_facts_normalized")
        .filter(is_prior_filing(pl.col("filing_id")))
        .group_by("company_id", "filing_id")
        .agg(pl.col("period_end").min())
        .select(
            "company_id",
            (-pl.col("filing_id")).alias("source_filing_id"),
            "period_end",
        )
    )
    return with_prior_filings(data.table("filings"), sources)


def prior_id(fact_id: pl.Expr) -> pl.Expr:
    """The id of a fact's copy on its filing's prior filing."""
    return abs_hash_expr("prior-period", fact_id)


def with_prior_facts(
    facts: pl.DataFrame, sources: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """``facts`` plus each prior filing's rows, and each copy's source id.

    A source fact of its filing's own period is copied when it has a
    comparative for the prior period and the filing stored no row of its own
    for that slot. Its parent and header pointers name the copies of theirs,
    or nothing when those were not copied. The source's rows dated in the
    prior period move onto the prior filing.

    The second frame (:data:`TIES`) maps each copy's ``id`` to its source's:
    dedup ranks a copy as it ranked the source, so two concepts that share a
    label (Amazon's current and non-current deferred tax assets) keep the
    same winner in the prior year.
    """
    if sources.is_empty():
        return facts, TIES.to_frame()
    pairs = sources.select(
        pl.col("source_filing_id").alias("filing_id"),
        pl.col("period_end").alias("prior_period_end"),
    )
    tagged = facts.join(pairs, on="filing_id", how="left")
    moved_here = pl.col("period_end") == pl.col("prior_period_end")
    slot = ["filing_id", "statement", "concept", "axis", "member", "period"]

    stored = tagged.filter(moved_here).select(
        *slot, pl.col("period_end").alias("comparative_period_end")
    )
    copied = tagged.filter(
        (pl.col("comparative_period_end") == pl.col("prior_period_end"))
        & ~moved_here
        & (pl.col("comparative_value").is_not_null() | pl.col("is_abstract"))
    ).join(stored, on=[*slot, "comparative_period_end"], how="anti", nulls_equal=True)
    ties = copied.select(
        prior_id(pl.col("id")).alias("id"), pl.col("id").alias("tie_id")
    )
    pointers = ties.select(pl.col("tie_id").alias("pointer"), pl.col("id").alias("to"))

    def repointed(rows: pl.DataFrame, column: str) -> pl.DataFrame:
        return (
            rows.join(pointers, left_on=column, right_on="pointer", how="left")
            .with_columns(pl.col("to").alias(column))
            .drop("to")
        )

    filing_id = facts.schema["filing_id"]
    copies = copied.with_columns(
        prior_id(pl.col("id")).alias("id"),
        (-pl.col("filing_id")).cast(filing_id).alias("filing_id"),
        pl.col("comparative_value").alias("value"),
        pl.col("comparative_period_end").alias("period_end"),
        pl.lit(None, facts.schema["comparative_value"]).alias("comparative_value"),
        pl.lit(None, pl.Date).alias("comparative_period_end"),
    )
    copies = repointed(repointed(copies, "parent_id"), "abstract_id")
    moved = tagged.with_columns(
        pl.when(moved_here)
        .then(-pl.col("filing_id"))
        .otherwise(pl.col("filing_id"))
        .cast(filing_id)
        .alias("filing_id"),
    )
    rows = pl.concat([moved.select(facts.columns), copies.select(facts.columns)])
    return rows, ties


# --- Quarterly: no gap before the first filing ---------------------------------


def attached_early_quarters(
    rows: pl.DataFrame, filings: pl.DataFrame, series: list[str]
) -> pl.DataFrame:
    """Quarterly ``rows`` without early quarters a gap separates from their series.

    An early quarter is one before the company's first filing: a prior
    filing's, or a column an early filing printed for a year it does not
    report. It is kept only when its series reaches the first filing
    through quarters it has, each the fiscal quarter after the one before.
    So a series opens where its first gap-free run does, and a stray column
    that stands alone is dropped. Quarters are counted, not days: Costco's
    fourth quarter is 16 weeks.

    Each line is judged on its own, so a kept quarter's subtotal can be
    missing: Amazon's first-quarter 2010 10-Q, the only print of the first
    quarter of 2009, has no gross profit line, so gross profit's 2008
    quarters stand apart while revenue's below it run on. The row then
    points at no parent rather than at a row the table does not have.
    """
    if rows.is_empty():
        return rows
    first = (
        filings.filter(~is_prior_filing(pl.col("id")))
        .group_by("company_id")
        .agg(pl.col("fiscal_period_end").min().alias("_first"))
    )
    dated = rows.join(first, on="company_id", how="left").with_columns(
        (pl.col("period_end") < pl.col("_first")).fill_null(False).alias("_early"),
        (pl.col("fiscal_year") * 4 + pl.col("fiscal_quarter")).alias("_n"),
    )
    periods = dated.select(*series, "period_end", "_n", "_early").unique(
        subset=[*series, "period_end"]
    )
    # Walk back from the series' first quarter on or after the first filing.
    anchor = (
        periods.filter(~pl.col("_early"))
        .group_by(series)
        .agg(pl.col("period_end").min().alias("_anchor"))
    )
    kept = (
        periods.join(anchor, on=series, how="inner", nulls_equal=True)
        .filter(pl.col("period_end") <= pl.col("_anchor"))
        .sort([*series, "period_end"], descending=[False] * len(series) + [True])
        .with_columns(pl.col("_n").shift(1).over(series).alias("_later"))
        .filter(
            pl.col("_early")
            & (
                (
                    pl.col("_later").is_not_null()
                    & (pl.col("_later") - pl.col("_n") != 1)
                )
                .cast(pl.Int32)
                .cum_sum()
                .over(series)
                == 0
            )
        )
        .select(*series, "period_end", pl.lit(True).alias("_kept"))
    )
    out = dated.join(
        kept, on=[*series, "period_end"], how="left", nulls_equal=True
    ).filter(~pl.col("_early") | pl.col("_kept").fill_null(False))
    dangling = (
        pl.col("_early")
        & pl.col("parent_id").is_not_null()
        & ~pl.col("parent_id").is_in(out["id"].implode())
    )
    return out.with_columns(
        pl.when(dangling).then(None).otherwise(pl.col("parent_id")).alias("parent_id")
    ).select(rows.columns)
