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
(see :func:`nine_month_source`); without one, Q1-Q3 are summed.

Frames bring the rows together; each member-year and each axis-year is then
decided in plain Python.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Iterator, Optional, Sequence

import polars as pl

from ...records import ROW, frame_of, grouped, numbered, records
from ...rules import ordering
from ...rules.periods import ANNUAL_FORMS, QUARTERLY_FORMS, quarter_apart
from ...rules.rounding import (
    decimal,
    differs_beyond_reprint,
    reprint_tolerance_of,
)
from .rows import SERIES, WEIGHTED_AVERAGE_SHARES, is_per_share

AXIS_YEAR = ["company_id", "statement", "normalized_label", "axis", "fiscal_tag"]
MEMBER_YEAR = [*SERIES, "fiscal_tag"]
DECIMAL = pl.Decimal(38, 10)
#: A per-share Q4 is only derived when Q1-Q3 sit under the 10-K's figure; a
#: mid-year stock split leaves them on another share basis.
PER_SHARE_SLACK = (Decimal("1.05"), Decimal("0.05"))


def per_share_fits(nine_months: Decimal, annual: Optional[Decimal]) -> bool:
    factor, slack = PER_SHARE_SLACK
    return annual is not None and abs(nine_months) <= abs(annual) * factor + slack


# --- Q1-Q3 of each member and year -------------------------------------------


@dataclass(slots=True)
class Quarter:
    """A discrete Q1-Q3 row of one member-year."""

    member_year: tuple
    filing_date: date
    is_comparative_backfill: bool
    fiscal_quarter: Optional[int]
    value: Optional[Decimal]
    restated_value: Optional[Decimal]
    drop_ytd: bool
    q3_ytd: Optional[Decimal]
    prefer_ytd_anchor: bool


@dataclass(slots=True)
class FirstThree:
    """Q1-Q3 of one member-year as one amount to subtract from the 10-K."""

    value: Optional[Decimal]
    #: When the quarters were filed. A backfilled comparative was printed a
    #: year later, so it does not count.
    filed: Optional[date]


def first_three_quarters(quarters: Sequence[Quarter]) -> FirstThree:
    """Three restated quarters sum to the restated basis.

    Otherwise three discrete quarters sum, unless a published Q3 kept the
    nine-month total, since the earlier quarters can be on a previous
    classification; then, or when a quarter is missing, the nine-month total
    is used.
    """
    filed = max(
        (q.filing_date for q in quarters if not q.is_comparative_backfill),
        default=None,
    )
    restated = [q.restated_value for q in quarters if q.restated_value is not None]
    if len(restated) == 3:
        return FirstThree(sum(restated, Decimal(0)), filed)

    shown = [q for q in quarters if not q.drop_ytd]
    numbers = {q.fiscal_quarter for q in shown if q.fiscal_quarter is not None}
    if len(numbers) == 3 and not any(q.prefer_ytd_anchor for q in quarters):
        values = [q.value for q in shown if q.value is not None]
        return FirstThree(sum(values, Decimal(0)) if values else None, filed)

    nine = max((q.q3_ytd for q in quarters if q.q3_ytd is not None), default=None)
    return FirstThree(nine, filed)


def quarters_by_year(
    restated: pl.DataFrame, filings: pl.DataFrame
) -> dict[tuple, FirstThree]:
    """:class:`FirstThree` per member-year (``MEMBER_YEAR`` tuple)."""
    dates = filings.select(pl.col("id").alias("filing_id"), "filing_date")
    rows = restated.filter(
        ~pl.col("is_abstract") & pl.col("fiscal_tag").is_not_null()
    ).join(dates, on="filing_id", how="inner")
    by_member = grouped(
        records(rows, Quarter, key=MEMBER_YEAR), lambda q: q.member_year
    )
    return {key: first_three_quarters(qs) for key, qs in by_member.items()}


def by_year_frame(by_year: dict[tuple, FirstThree]) -> pl.DataFrame:
    """:func:`quarters_by_year` as a frame on ``MEMBER_YEAR``."""
    return frame_of(
        ((*key, q.value, q.filed) for key, q in by_year.items()),
        {
            "company_id": pl.Int32,
            "statement": pl.String,
            "normalized_label": pl.String,
            "axis": pl.String,
            "member": pl.String,
            "fiscal_tag": pl.Int32,
            "value": DECIMAL,
            "filed": pl.Date,
        },
    )


# --- The 10-Ks and nine-month 10-Qs of each axis and year ---------------------


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
            (
                (pl.col("statement") != "Balance Sheet")
                & ~pl.col("concept").is_in(WEIGHTED_AVERAGE_SHARES)
            ).alias("is_flow"),
            is_per_share(pl.col("unit")).alias("is_per_share"),
        )
        .drop("f_fiscal_year", "f_fiscal_quarter")
    )


def nine_months(
    data_rows: pl.DataFrame,
    filings: pl.DataFrame,
    tags: pl.DataFrame,
    moved: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    """Every 10-Q that printed the nine months before a 10-K.

    Includes reprints a year later under that year's members. ``moved`` are
    the line-years whose quarters next year's comparatives replaced (see
    :mod:`.restatement`); a reprint on that basis matches them.
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
    # A later 10-Q prints the year's nine months on the line as a comparative.
    # When that differs from the year's own 10-Q, the reprint restated the
    # total, so its members are on another basis than the year's quarters.
    line = data_rows.filter(
        pl.col("form_type").is_in(QUARTERLY_FORMS)
        & (pl.col("period") == "YTD")
        & (pl.col("axis") == "")
        & ~pl.col("is_abstract")
    )
    line_keys = ["company_id", "statement", "normalized_label", "period_end"]
    own_line = ordering.distinct_on(
        line.filter(pl.col("filing_id") == pl.col("fiscal_filing_id")),
        [*line_keys, "concept"],
        ["id"],
    ).select(*line_keys, "concept", pl.col("value").alias("own_line"))
    reprinted = (
        line.filter(pl.col("comparative_period_end").is_not_null())
        .select(
            "filing_id",
            "company_id",
            "statement",
            "normalized_label",
            "concept",
            pl.col("comparative_period_end").alias("period_end"),
            pl.col("comparative_value").alias("reprinted_line"),
        )
        .join(own_line, on=[*line_keys, "concept"], how="inner")
        .with_columns(
            differs_beyond_reprint(pl.col("own_line"), pl.col("reprinted_line"))
            .fill_null(False)
            .alias("restates_line")
        )
        # One label can sit on two concepts; the total that agrees decides.
        .group_by(["filing_id", *line_keys])
        .agg(pl.col("restates_line").all())
    )
    rows = rows.join(reprinted, on=["filing_id", *line_keys], how="left").with_columns(
        pl.col("restates_line").fill_null(False)
    )
    if moved is not None and not moved.is_empty():
        line_year = ["company_id", "statement", "normalized_label", "fiscal_tag"]
        on_new_basis = (
            moved.select(line_year).unique().with_columns(pl.lit(True).alias("_moved"))
        )
        rows = (
            rows.join(on_new_basis, on=line_year, how="left")
            .with_columns(pl.col("restates_line") & pl.col("_moved").is_null())
            .drop("_moved")
        )
    keys = [*SERIES, "filing_id", "fiscal_tag"]
    return ordering.distinct_on(rows, keys, ["is_duplicate", "id"]).select(
        *keys, "filing_date", "value", "restates_line"
    )


@dataclass(slots=True)
class AnnualMember:
    """One member's figure in one 10-K of an axis-year."""

    axis_year: tuple
    row_nr: int
    id: int
    filing_id: int
    filing_date: date
    is_amendment: bool
    is_duplicate: bool
    member: str
    value: Optional[Decimal]
    #: Not a balance or a weighted-average share count, so Q4 is a residual.
    is_flow: bool
    is_per_share: bool


@dataclass(slots=True)
class NineMonthMember:
    """One member's nine-month total in one 10-Q of an axis-year."""

    axis_year: tuple
    filing_id: int
    filing_date: date
    member: str
    value: Optional[Decimal]
    #: The 10-Q printed the year's nine-month line at another value.
    restates_line: bool = False


@dataclass
class Filing:
    """One 10-K or 10-Q of an axis-year, with its members' values."""

    filing_id: int
    filing_date: date
    is_amendment: bool = False
    rows: list[AnnualMember] = field(default_factory=list)
    values: dict[str, list[Optional[Decimal]]] = field(default_factory=dict)
    #: A nine-month 10-Q that printed the line itself at another value.
    restates_line: bool = False

    @property
    def members(self) -> frozenset[str]:
        return frozenset(self.values)


def _filings(rows: Sequence[AnnualMember | NineMonthMember]) -> list[Filing]:
    out: dict[int, Filing] = {}
    for r in rows:
        f = out.get(r.filing_id)
        if f is None:
            amendment = isinstance(r, AnnualMember) and r.is_amendment
            f = out[r.filing_id] = Filing(r.filing_id, r.filing_date, amendment)
        if isinstance(r, AnnualMember):
            f.rows.append(r)
        elif r.restates_line:
            f.restates_line = True
        f.values.setdefault(r.member, []).append(r.value)
    return list(out.values())


def introduced(annual: Filing, quarters: dict[str, FirstThree]) -> frozenset[str]:
    """Members a 10-K reports, beyond zero, that the year's quarters do not have."""
    return frozenset(
        r.member
        for r in annual.rows
        if r.value
        and (quarters.get(r.member) is None or quarters[r.member].value is None)
    )


def on_quarter_split(annual: Filing, quarters: dict[str, FirstThree]) -> bool:
    """Every member has quarters (or is zero), and every member with quarters is in it.

    A year where no member has quarters gives no split to be on, so every
    10-K of it passes and the next criterion decides.
    """
    with_quarters = {m for m, q in quarters.items() if q.value is not None}
    if not with_quarters:
        return True
    every_member = all(
        r.member in with_quarters or r.value == 0
        for r in annual.rows
        if r.value is not None
    )
    return every_member and with_quarters <= annual.members


def rank_annual_filings(
    fiscal_tag: int,
    annuals: Sequence[Filing],
    quarters: dict[str, FirstThree],
    paired: frozenset[int] = frozenset(),
    covered: frozenset[str] = frozenset(),
) -> list[Filing]:
    """The 10-Ks of one axis-year, best source of Q4 first (see the module).

    ``paired`` are the 10-Ks with a nine-month 10-Q on their members, and
    ``covered`` the members those nine months print.
    """
    filed = [
        q.filed
        for a in annuals
        for r in a.rows
        if (q := quarters.get(r.member)) is not None and q.filed is not None
    ]
    quarters_filed = max(filed) if filed else None
    shown = {a.filing_id for a in annuals if any(not r.is_duplicate for r in a.rows)}

    def rank(a: Filing) -> tuple:
        after = quarters_filed is not None and a.filing_date >= quarters_filed
        # A later 10-K that restated the year replaced every member of the
        # year's own 10-K (see facts_normalized.dedup.restating_filings).
        restated = a.filing_id == fiscal_tag and shown and a.filing_id not in shown
        # The year's own 10-K moved amounts onto members its quarters do not
        # have, and no 10-Q printed the nine months on them, so subtracting
        # the quarters puts the move into Q4. A later 10-K paired with a
        # 10-Q on its members goes first: Microsoft's fiscal 2023 10-K split
        # Dynamics out of Office and Other, and only the fiscal 2024 10-K and
        # its third-quarter 10-Q print fiscal 2023 on one split.
        unpaired_recast = (
            a.filing_id == fiscal_tag
            and a.filing_id not in paired
            and bool(introduced(a, quarters) & covered)
        )
        return (
            not a.is_amendment,
            bool(restated),
            unpaired_recast,
            a.filing_id != fiscal_tag,
            not on_quarter_split(a, quarters),
            # The first 10-K filed after the quarters, else the latest before.
            not after,
            a.filing_date if after else date.max,
            -a.filing_date.toordinal(),
            -a.filing_id,
        )

    return sorted(annuals, key=rank)


def _prints_year_as(own: Filing, later: Filing) -> bool:
    """No member shared by both filings changed beyond the later one's rounding."""
    return not any(
        abs(o - v) > reprint_tolerance_of(v)
        for member, own_values in own.values.items()
        for o in own_values
        for v in later.values.get(member, ())
        if o is not None and v is not None
    )


def nine_month_source(
    annual: Filing, annuals: Sequence[Filing], nines: Sequence[Filing]
) -> Optional[Filing]:
    """The 10-Q whose nine months this 10-K subtracts.

    Q4 is the 10-K minus nine months, and both must split the year the same
    way, or Q4 absorbs whatever moved between members. Two cases:

    * A 10-K moves an amount out of one member into a new one, and next
      year's 10-Q reprints this year's nine months on the new split, while
      this year's own 10-Qs still hold the old one. That reprint is used
      when all its members are this 10-K's and the first 10-K after it
      prints the year as this 10-K did (no shared member at another value
      beyond rounding). For example, Tesla's fiscal 2021 10-K took
      regulatory credits out of automotive sales.
    * A 10-K recasts its year onto other members. It pairs with a 10-Q on
      exactly its members, the year's own first, and not with quarters
      filed on the earlier members.
    """
    best: Optional[tuple[tuple, Filing]] = None
    for nine in nines:
        if not nine.members <= annual.members:
            continue
        on_split = False
        if nine.filing_date > annual.filing_date:
            later = [a for a in annuals if a.filing_date > nine.filing_date]
            if later:
                following = min(later, key=lambda a: (a.filing_date, a.filing_id))
                # The reprint moved amounts between members, not the total:
                # Alphabet's 2013 10-Qs reprinted 2012 without Motorola's
                # home business, which the year's own quarters still hold.
                on_split = _prints_year_as(annual, following) and not nine.restates_line
        if not (on_split or nine.members == annual.members):
            continue
        rank = (
            not on_split,
            not nine.filing_date < annual.filing_date,
            abs((nine.filing_date - annual.filing_date).days),
            nine.filing_id,
        )
        if best is None or rank < best[0]:
            best = (rank, nine)
    return None if best is None else best[1]


@dataclass(slots=True)
class Q4Source:
    """Where one 10-K of an axis-year stands as the source of Q4."""

    annual: Filing
    rank: int
    nine_months: Optional[Filing]
    #: First-ranked over the year's own 10-K, which recast the year with no
    #: nine months to pair; the other 10-Ks are on another split.
    alone: bool = False


def member_q4(
    sources: Sequence[Q4Source], quarters: dict[str, FirstThree]
) -> Iterator[tuple[int, Decimal]]:
    """``(annual row, amount to subtract)`` for every member that gets a Q4.

    Each member comes from the best-ranked 10-K that has it and something to
    subtract.

    A 10-K can introduce members that no 10-Q of its own year used. Only
    next year's 10-Qs print this year's quarters on them, and those reprints
    are hidden as a recast, so the member has no Q1-Q3 of its own. A member
    of the first-ranked 10-K therefore gets Q4 from that 10-K's nine months
    alone; lower-ranked 10-Ks still need the member's Q1-Q3, so a later
    recast cannot fill a member that way. For example, Amazon's 2016 10-K
    introduced new sales categories, and only the 2017 10-Qs printed 2016's
    quarters on them.
    """
    chosen: dict[str, tuple[tuple, int, Decimal]] = {}
    if sources and sources[0].alone:
        # Filling a member it left out from the year's own 10-K would count
        # a line twice beside its restated successor (Microsoft's Enterprise
        # Services beside Enterprise and Partner Services).
        sources = sources[:1]
    for source in sources:
        for row in source.annual.rows:
            q = quarters.get(row.member)
            first_three = q.value if q is not None else None
            nine = None
            if source.nine_months is not None:
                nine = source.nine_months.values.get(row.member, [None])[0]
            subtract = nine if nine is not None else first_three
            if subtract is None or not row.is_flow:
                continue
            if first_three is None and source.rank != 1:
                continue
            if row.is_per_share and not per_share_fits(subtract, row.value):
                continue
            order = (source.rank, row.is_duplicate, row.id)
            best = chosen.get(row.member)
            if best is None or order < best[0]:
                chosen[row.member] = (order, row.row_nr, subtract)
    for _, row_nr, subtract in chosen.values():
        yield row_nr, subtract


def q4_sources(
    annual: pl.DataFrame,
    nines: pl.DataFrame,
    quarters: dict[tuple, dict[str, FirstThree]],
) -> dict[tuple, list[Q4Source]]:
    """Per axis-year, its 10-Ks ranked with the nine months each subtracts."""
    annual_rows = grouped(
        records(annual, AnnualMember, key=AXIS_YEAR), lambda r: r.axis_year
    )
    nine_rows = grouped(
        records(nines, NineMonthMember, key=AXIS_YEAR), lambda r: r.axis_year
    )
    out = {}
    for axis_year, rows in annual_rows.items():
        annuals = _filings(rows)
        nine_filings = _filings(nine_rows.get(axis_year, []))
        nine_of = {
            a.filing_id: nine_month_source(a, annuals, nine_filings) for a in annuals
        }
        paired = frozenset(i for i, nine in nine_of.items() if nine is not None)
        covered = frozenset(
            m for nine in nine_of.values() if nine is not None for m in nine.members
        )
        year_quarters = quarters.get(axis_year, {})
        ranked = rank_annual_filings(
            axis_year[-1], annuals, year_quarters, paired, covered
        )
        own = next((a for a in annuals if a.filing_id == axis_year[-1]), None)
        alone = (
            own is not None
            and ranked[0] is not own
            and not ranked[0].is_amendment
            and own.filing_id not in paired
            and ranked[0].filing_id in paired
            and bool(introduced(own, year_quarters) & ranked[0].members)
        )
        out[axis_year] = [
            Q4Source(a, rank, nine_of[a.filing_id], alone and rank == 1)
            for rank, a in enumerate(ranked, start=1)
        ]
    return out


def by_axis_year(
    by_year: dict[tuple, FirstThree],
) -> dict[tuple, dict[str, FirstThree]]:
    """Member-years regrouped: axis-year to member to its Q1-Q3."""
    out: dict[tuple, dict[str, FirstThree]] = {}
    for (*line, axis, member, fiscal_tag), q in by_year.items():
        out.setdefault((*line, axis, fiscal_tag), {})[member] = q
    return out


def sources_frame(sources: dict[tuple, list[Q4Source]]) -> pl.DataFrame:
    """The ranking and nine-month pairing per axis-year, for ``--dump-dir``."""
    return frame_of(
        (
            (
                *axis_year,
                s.annual.filing_id,
                s.rank,
                s.nine_months.filing_id if s.nine_months else None,
            )
            for axis_year, ranked in sources.items()
            for s in ranked
        ),
        {
            "company_id": pl.Int32,
            "statement": pl.String,
            "normalized_label": pl.String,
            "axis": pl.String,
            "fiscal_tag": pl.Int32,
            "filing_id": pl.Int32,
            "split_rank": pl.Int64,
            "nine_month_filing_id": pl.Int32,
        },
    )


def dimension_q4(
    annual: pl.DataFrame, nines: pl.DataFrame, by_year: dict[tuple, FirstThree]
) -> tuple[pl.DataFrame, dict[tuple, list[Q4Source]]]:
    """Q4 of every dimensioned member, and the sources it was chosen from."""
    annual = numbered(annual)
    quarters = by_axis_year(by_year)
    sources = q4_sources(annual, nines, quarters)
    subtract = frame_of(
        (
            pick
            for axis_year, ranked in sources.items()
            for pick in member_q4(ranked, quarters.get(axis_year, {}))
        ),
        {ROW: pl.UInt32, "subtract": DECIMAL},
    )
    q4 = annual.join(subtract, on=ROW).with_columns(
        (pl.col("value") - pl.col("subtract")).alias("value"),
        pl.lit("calculated").alias("source_type"),
    )
    return q4, sources


def line_q4(annual_rows: pl.DataFrame, by_year: pl.DataFrame) -> pl.DataFrame:
    """Q4 of every undimensioned flow line: its 10-K minus Q1-Q3."""
    rows = annual_rows.join(
        by_year.select(*MEMBER_YEAR, pl.col("value").alias("q_value")),
        on=MEMBER_YEAR,
    )
    factor, slack = PER_SHARE_SLACK
    fits = ~is_per_share(pl.col("unit")) | (
        pl.col("q_value").abs()
        <= pl.col("value").abs() * decimal(factor) + decimal(slack)
    )
    eligible = (
        (pl.col("axis").fill_null("") == "")
        & (pl.col("statement") != "Balance Sheet")
        # Instant stocks are not annual minus quarters.
        & pl.col("period").is_not_null()
        & ~pl.col("concept").is_in(WEIGHTED_AVERAGE_SHARES)
        & fits
        & pl.col("q_value").is_not_null()
        & ~pl.col("is_abstract")
    )
    return rows.filter(eligible).with_columns(
        (pl.col("value") - pl.col("q_value")).alias("value"),
        pl.lit("calculated").alias("source_type"),
    )
