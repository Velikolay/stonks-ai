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

#: Cuts a fact can be shown on when it is filed beside an axis total. The
#: parser keeps these axes from segment notes; ConsolidationItemsAxis is only
#: ever the total's axis here, never the slice's.
SLICE_AXES = frozenset(
    f"{ns}:{name}"
    for ns in ("srt", "us-gaap")
    for name in (
        "StatementBusinessSegmentsAxis",
        "StatementGeographicalAxis",
        "ProductOrServiceAxis",
    )
) | {
    "ifrs-full:SegmentsAxis",
    "ifrs-full:GeographicalAreasAxis",
    "ifrs-full:ProductsAndServicesAxis",
}


def on_real_slice(facts: pl.DataFrame, dimensions: pl.DataFrame) -> pl.DataFrame:
    """Show a fact filed under an axis total on its one real slice.

    Segment notes tag a segment's amount twice: ConsolidationItemsAxis =
    OperatingSegmentsMember, the whole of the segments, and the segment
    itself on StatementBusinessSegmentsAxis. edgartools reports the first
    axis, with a member such as "Operating Segments - Google Cloud", so the
    amount misses the segment's revenue. When the stored axis's coordinate is
    an axis total and exactly one coordinate is not, the fact takes that
    coordinate's axis, member and label, the key a single-dimension fact on
    that segment has. A fact whose filing tags that slot on its own with
    another value keeps its axis, so the two do not conflict.
    """
    if dimensions.is_empty() or facts.is_empty():
        return facts
    real = dimensions.filter(~pl.col("is_default"))
    one_slice = (
        real.group_by("fact_id")
        .agg(pl.len().alias("slices"))
        .filter(pl.col("slices") == 1)
        .select("fact_id")
    )
    slice_coordinate = real.join(one_slice, on="fact_id", how="semi").filter(
        pl.col("axis").is_in(list(SLICE_AXES))
    )
    totals = dimensions.filter(pl.col("is_default")).select(
        "fact_id", pl.col("axis").alias("total_axis")
    )
    moves = (
        facts.select("id", "axis")
        .join(totals, left_on=["id", "axis"], right_on=["fact_id", "total_axis"])
        .join(slice_coordinate, left_on="id", right_on="fact_id")
        .filter(pl.col("axis_right") != pl.col("axis"))
        .select(
            "id",
            pl.col("axis_right").alias("s_axis"),
            pl.col("member").alias("s_member"),
            pl.col("member_label").alias("s_member_label"),
        )
        .unique("id")
    )
    if moves.is_empty():
        return facts

    slot = ["filing_id", "concept", "statement", "period_end", "period"]
    moved = facts.join(moves, on="id", how="inner")
    clashes = moved.join(
        facts.select(
            *slot,
            pl.col("axis").alias("s_axis"),
            pl.col("member").alias("s_member"),
            pl.col("value").alias("twin_value"),
        ),
        on=[*slot, "s_axis", "s_member"],
        how="inner",
        nulls_equal=True,
    ).filter(pl.col("twin_value") != pl.col("value"))
    moves = moves.join(clashes.select("id"), on="id", how="anti")

    return (
        facts.join(moves, on="id", how="left")
        .with_columns(
            pl.coalesce("s_axis", "axis").alias("axis"),
            pl.coalesce("s_member", "member").alias("member"),
            pl.coalesce("s_member_label", "member_label").alias("member_label"),
        )
        .drop("s_axis", "s_member", "s_member_label")
    )


def without_finer_cuts(facts: pl.DataFrame, dimensions: pl.DataFrame) -> pl.DataFrame:
    """Drop a fact that a filing tagged beside its own slot with another dimension.

    edgartools stores a fact's first axis only. A filing that cuts an amount
    further prints it twice on that axis and member, once alone and once with
    a second coordinate, and the two would conflict. Microsoft's fiscal 2016
    first-quarter 10-Q tags Corporate and Other operating income as -1,274
    and, with ``IncomeStatementLocationAxis`` = impairment, integration and
    restructuring, as 0; the conflict dropped the segment amount. When facts
    of one filing share a stored slot, those with more real (non-default)
    coordinates than the fewest are a finer cut and are dropped.
    """
    if dimensions.is_empty() or facts.is_empty():
        return facts
    real = (
        dimensions.filter(~pl.col("is_default"))
        .group_by("fact_id")
        .agg(pl.len().alias("coordinates"))
    )
    slot = [
        "filing_id",
        "statement",
        "concept",
        "axis",
        "member",
        "period_end",
        "period",
    ]
    counted = facts.join(
        real, left_on="id", right_on="fact_id", how="left"
    ).with_columns(pl.col("coordinates").fill_null(0))
    finest = pl.col("coordinates") > pl.col("coordinates").min().over(slot)
    dimensioned = pl.col("axis").fill_null("") != ""
    return counted.filter(~(dimensioned & finest)).drop("coordinates")


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
    A fact filed under an axis total is first moved onto its real slice
    (``on_real_slice``), and a finer cut beside its own slot is dropped
    (``without_finer_cuts``); an override still matches the fact as filed.
    """
    dimensions = data.table("financial_fact_dimensions")
    facts = without_finer_cuts(
        on_real_slice(data.table("financial_facts"), dimensions), dimensions
    )
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
