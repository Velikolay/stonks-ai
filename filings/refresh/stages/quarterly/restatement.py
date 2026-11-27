"""Moving a year's quarters onto next year's comparatives.

When a line's own first three quarters overshoot its 10-K by more than 5%,
Q4 would reverse the year. If next year's 10-Q comparatives of those
quarters still fit under the 10-K, they replace the three quarters and Q4
becomes the 10-K minus them. Other undimensioned income-statement lines of
that year follow when next year's 10-K reprinted this year's figure
unchanged, so subtotals stay on one basis. Per-share amounts and share
counts stay on the filed quarters. A moved quarter's members follow axis by
axis onto next year's reprint of them when it adds up to the restated
quarter (:func:`with_restated_members`).

Each line is read on its own (:func:`line_years`); only the decision to
follow looks across the lines of a statement and year (:func:`moved_years`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Iterable, Iterator, Optional, Sequence

import polars as pl

from ...records import frame_of, grouped, records
from ...rules import ordering
from ...rules.periods import (
    ANNUAL_FORMS,
    YEAR_GAP_DAYS,
    consecutive_quarters,
    days_between,
    within,
)
from ...rules.rounding import reprint_tolerance_of
from .rows import is_per_share

LINE = ["company_id", "statement", "normalized_label"]
#: Own quarters this far past the 10-K would reverse the year in Q4.
OVERSHOOT = Decimal("1.05")
DECIMAL = pl.Decimal(38, 10)


def _income_lines() -> pl.Expr:
    """Undimensioned income-statement amounts: the lines a restatement moves."""
    return (
        (pl.col("statement") == "Income Statement")
        & (pl.col("axis") == "")
        & ~pl.col("is_abstract")
    )


def _sign(value: Decimal) -> int:
    return (value > 0) - (value < 0)


# --- What one line reports ---------------------------------------------------


@dataclass(slots=True)
class Comparative:
    """A 10-Q's comparative a year back: this year's quarter, as next year printed it."""

    line: tuple
    fiscal_year: int
    fiscal_quarter: Optional[int]
    #: The period the comparative is for.
    comparative_period_end: date
    period: Optional[str]
    comparative_value: Decimal


@dataclass(slots=True)
class OwnQuarter:
    """A discrete Q1-Q3 the line filed in its own year."""

    line: tuple
    period_end: date
    fiscal_tag: int
    fiscal_quarter: int
    value: Decimal


@dataclass(slots=True)
class Annual:
    """A 10-K figure of the line."""

    line: tuple
    id: int
    period_end: date
    fiscal_tag: Optional[int]
    value: Optional[Decimal]
    comparative_period_end: Optional[date]
    comparative_value: Optional[Decimal]


def comparative_quarters(comparatives: Sequence[Comparative]) -> dict[date, list]:
    """Each quarter next year's 10-Qs printed for one line, by period end.

    A published quarter's comparative is that quarter. A year-to-date one
    subtracts the cumulative comparative of the quarter before it in the same
    filing year; without a YTD, the cumulative is the running sum of the
    published quarters.
    """
    printed: dict[date, list[Decimal]] = {}
    for rows in grouped(comparatives, lambda c: c.fiscal_year).values():
        periods = grouped(rows, lambda c: (c.comparative_period_end, c.fiscal_quarter))
        running: Optional[Decimal] = None
        previous: Optional[tuple[date, Optional[Decimal]]] = None
        for (period_end, fiscal_quarter), same in sorted(
            periods.items(), key=lambda p: p[0][0]
        ):
            quarter = max(
                (c.comparative_value for c in same if c.period == "Q"), default=None
            )
            ytd = max(
                (c.comparative_value for c in same if c.period == "YTD"), default=None
            )
            if quarter is not None:
                running = quarter if running is None else running + quarter
            cumulative = ytd if ytd is not None else running

            value = quarter
            if value is None and fiscal_quarter == 1:
                value = cumulative
            elif value is None and previous is not None:
                prev_end, prev_cumulative = previous
                if (
                    consecutive_quarters(period_end, prev_end)
                    and cumulative is not None
                    and prev_cumulative is not None
                ):
                    value = cumulative - prev_cumulative
            previous = (period_end, cumulative)
            if value is not None:
                printed.setdefault(period_end, []).append(value)
    return printed


def annual_figures(annuals: Sequence[Annual]) -> dict[int, list[tuple]]:
    """One line's 10-K figures by fiscal tag: ``(value, next year's reprint)``."""
    ordered = sorted(annuals, key=lambda a: (a.period_end, a.id))
    figures: dict[int, list[tuple]] = {}
    for annual, following in zip(ordered, [*ordered[1:], None]):
        if annual.fiscal_tag is None or annual.value is None:
            continue
        reprint = None
        if following and following.comparative_period_end == annual.period_end:
            reprint = following.comparative_value
        figures.setdefault(annual.fiscal_tag, []).append((annual.value, reprint))
    return figures


@dataclass(slots=True)
class YearBasis:
    """One line's year: its own Q1-Q3 beside next year's comparatives of them."""

    line: tuple
    fiscal_tag: int
    annual: Decimal
    #: This year's figure as next year's 10-K reprinted it.
    next_annual: Optional[Decimal]
    own: Decimal
    comparative: Decimal
    #: Period end to the comparative quarter that would replace it.
    replacements: dict[date, Decimal]

    @property
    def statement_year(self) -> tuple:
        company_id, statement, _ = self.line
        return company_id, statement, self.fiscal_tag

    @property
    def tolerance(self) -> Decimal:
        return max(abs(self.annual) / 1000, Decimal(1))

    @property
    def comparatives_fit(self) -> bool:
        """The 10-K minus the comparatives keeps the 10-K's sign."""
        rest = self.annual - self.comparative
        return _sign(rest) == _sign(self.annual) or abs(rest) <= self.tolerance

    @property
    def overshoots(self) -> bool:
        """The own quarters exceed the 10-K by more than 5%, so Q4 would reverse it."""
        return (
            _sign(self.own) == _sign(self.annual)
            and abs(self.own) > abs(self.annual) * OVERSHOOT
        )

    @property
    def can_follow(self) -> bool:
        """Next year reprinted the 10-K unchanged, and the quarters did change."""
        return (
            self.next_annual is not None
            and abs(self.next_annual - self.annual) <= self.tolerance
            and abs(self.own - self.comparative) > self.tolerance
        )


def line_years(
    own: Sequence[OwnQuarter],
    comparatives: Sequence[Comparative],
    annuals: Sequence[Annual],
) -> Iterator[YearBasis]:
    """The years of one line with three own quarters, three comparatives and one 10-K."""
    printed = comparative_quarters(comparatives)
    # A quarter pairs when the line filed it once and next year printed it once.
    pairs = [
        (quarters[0], printed[period_end][0])
        for period_end, quarters in grouped(own, lambda q: q.period_end).items()
        if len(quarters) == 1 and len(printed.get(period_end, ())) == 1
    ]
    tenks = annual_figures(annuals)

    for fiscal_tag, year in grouped(pairs, lambda p: p[0].fiscal_tag).items():
        numbers = {q.fiscal_quarter for q, _ in year}
        if len(year) != 3 or len(numbers) != 3 or len(tenks.get(fiscal_tag, ())) != 1:
            continue
        annual, next_annual = tenks[fiscal_tag][0]
        yield YearBasis(
            line=year[0][0].line,
            fiscal_tag=fiscal_tag,
            annual=annual,
            next_annual=next_annual,
            own=sum((q.value for q, _ in year), Decimal(0)),
            comparative=sum((c for _, c in year), Decimal(0)),
            replacements={q.period_end: c for q, c in year},
        )


def moved_years(years: Iterable[YearBasis]) -> list[YearBasis]:
    """The line-years that move onto next year's comparatives.

    A line moves when its own quarters overshoot. It follows another line of
    the same statement and year that moved when it can follow. Either way,
    the comparatives must fit.
    """
    fitting = [y for y in years if y.comparatives_fit]
    overshooting = {y.statement_year for y in fitting if y.overshoots}
    return [
        y
        for y in fitting
        if y.overshoots or (y.statement_year in overshooting and y.can_follow)
    ]


# --- Frames in and out -------------------------------------------------------


def restated_years(
    quarters: pl.DataFrame, discrete: pl.DataFrame, tagged: pl.DataFrame
) -> list[YearBasis]:
    """Every income line's moved years, from the 10-Qs and 10-Ks of the company."""
    comparatives = quarters.filter(
        _income_lines()
        & pl.col("comparative_value").is_not_null()
        & within(
            days_between(pl.col("period_end"), pl.col("comparative_period_end")),
            YEAR_GAP_DAYS,
        )
    )
    own = discrete.filter(
        _income_lines()
        & pl.col("fiscal_quarter").is_in([1, 2, 3])
        & pl.col("fiscal_tag").is_not_null()
        & pl.col("value").is_not_null()
        & ~pl.col("drop_ytd")
        & ~pl.col("is_comparative_backfill")
        & ~pl.col("is_non_additive")
        & ~is_per_share(pl.col("unit"))
    )
    annual = tagged.filter(
        pl.col("source_type").is_in(ANNUAL_FORMS)
        & _income_lines()
        & (pl.col("period") == "YTD")
    )
    own_by_line = grouped(records(own, OwnQuarter, key=LINE), lambda r: r.line)
    printed_by_line = grouped(
        records(comparatives, Comparative, key=LINE), lambda r: r.line
    )
    annual_by_line = grouped(records(annual, Annual, key=LINE), lambda r: r.line)
    years = [
        year
        for line, own_quarters in own_by_line.items()
        for year in line_years(
            own_quarters,
            printed_by_line.get(line, []),
            annual_by_line.get(line, []),
        )
    ]
    return moved_years(years)


def moved_frame(moved: Sequence[YearBasis]) -> pl.DataFrame:
    """The moved line-years, for ``--dump-dir``."""
    return frame_of(
        ((*y.line, y.fiscal_tag, y.annual, y.own, y.comparative) for y in moved),
        {
            "company_id": pl.Int32,
            "statement": pl.String,
            "normalized_label": pl.String,
            "fiscal_tag": pl.Int32,
            "annual": DECIMAL,
            "own": DECIMAL,
            "comparative": DECIMAL,
        },
    )


def with_restated_values(
    discrete: pl.DataFrame, moved: Sequence[YearBasis]
) -> pl.DataFrame:
    """``restated_value``: the comparative that replaces an own quarter, if any."""
    restated = frame_of(
        (
            (*y.line, period_end, value)
            for y in moved
            for period_end, value in y.replacements.items()
        ),
        {
            "company_id": pl.Int32,
            "statement": pl.String,
            "normalized_label": pl.String,
            "period_end": pl.Date,
            "restated": DECIMAL,
        },
    )
    eligible = (
        (pl.col("axis") == "")
        & ~pl.col("is_abstract")
        & ~pl.col("drop_ytd")
        & ~pl.col("is_comparative_backfill")
    )
    joined = discrete.join(restated, on=[*LINE, "period_end"], how="left")
    return joined.with_columns(
        pl.when(eligible).then(pl.col("restated")).alias("restated_value")
    ).drop("restated")


# --- Members follow a moved line-year ------------------------------------------

#: The 10-Q that reprints a quarter a year later ends this many days after it.
_REPRINT_DAYS = (340, 380)


def reprint_adds_up(
    members: dict[str, Decimal], restated: Decimal, splits: Sequence[Sequence[str]]
) -> bool:
    """The reprinted members, or one known split of them, add up to the restated line.

    Rounded members may miss it by half a unit each, up to 0.1%. An axis that
    holds a subtotal beside its parts only adds up through a split.
    """

    def close(values: list[Decimal]) -> bool:
        if len(values) < 2:
            return False
        gap = abs(sum(values, Decimal(0)) - restated)
        unit = max(reprint_tolerance_of(v) for v in values)
        return gap <= min(unit * len(values), abs(restated) / 1000)

    if close(list(members.values())):
        return True
    return any(
        all(m in members for m in split) and close([members[m] for m in split])
        for split in splits
    )


def following_members(
    moved: Sequence[YearBasis],
    normalized: pl.DataFrame,
    filings: pl.DataFrame,
    breakdowns: pl.DataFrame,
) -> pl.DataFrame:
    """Next year's reprint of each moved quarter, axis by axis, where it adds up.

    A line-year that moved onto next year's comparatives leaves its members
    on the filed basis, so they no longer add up to the line: S&P Global's
    2012 quarters moved off McGraw-Hill Education when the FY2012 10-K put it
    in discontinued operations, while the segments kept it. The reprint that
    restated the line printed the members too (dedup hid them as a recast);
    they replace the quarter's members on that axis when they add up to the
    restated quarter. The earliest such 10-Q wins.
    """
    replaced = frame_of(
        (
            (*y.line, y.fiscal_tag, period_end, value)
            for y in moved
            for period_end, value in y.replacements.items()
        ),
        {
            "company_id": pl.Int32,
            "statement": pl.String,
            "normalized_label": pl.String,
            "fiscal_tag": pl.Int32,
            "period_end": pl.Date,
            "restated": DECIMAL,
        },
    )
    if replaced.is_empty():
        return normalized.clear()
    later = filings.select(
        pl.col("id").alias("filing_id"),
        "filing_date",
        pl.col("fiscal_period_end").alias("reprint_period_end"),
    )
    candidates = (
        normalized.filter(
            (pl.col("form_type") == "10-Q")
            & (pl.col("period") == "Q")
            & (pl.col("axis") != "")
            & ~pl.col("is_abstract")
            & pl.col("value").is_not_null()
        )
        .join(replaced, on=[*LINE, "period_end"], how="inner")
        .join(later, on="filing_id", how="inner")
        .filter(
            within(
                days_between(pl.col("reprint_period_end"), pl.col("period_end")),
                _REPRINT_DAYS,
            )
        )
    )
    if candidates.is_empty():
        return normalized.clear()
    # One row per member of a filing; a slot tagged twice keeps the first.
    candidates = ordering.distinct_on(
        candidates,
        [*LINE, "axis", "member", "period_end", "filing_id"],
        ["is_duplicate", "id"],
    )
    splits = grouped(
        breakdowns.filter(pl.col("parent_member") == "").iter_rows(named=True),
        lambda b: (b["company_id"], b["statement"], b["normalized_label"], b["axis"]),
    )
    slot = [*LINE, "axis", "period_end"]
    chosen: list[tuple] = []
    for key, rows in candidates.group_by([*slot, "restated"], maintain_order=True):
        restated = key[-1]
        by_filing = grouped(rows.iter_rows(named=True), lambda r: r["filing_id"])
        axis_splits = [b["members"] for b in splits.get(key[:4], [])]
        for filing_id, members in sorted(
            by_filing.items(), key=lambda f: (f[1][0]["filing_date"], f[0])
        ):
            values = {m["member"]: m["value"] for m in members}
            if reprint_adds_up(values, restated, axis_splits):
                chosen.append((*key[:-1], filing_id))
                break
    picked = frame_of(
        chosen,
        {
            "company_id": pl.Int32,
            "statement": pl.String,
            "normalized_label": pl.String,
            "axis": pl.String,
            "period_end": pl.Date,
            "filing_id": pl.Int32,
        },
    )
    return candidates.join(picked, on=[*slot, "filing_id"], how="semi")


def with_restated_members(
    restated: pl.DataFrame,
    moved: Sequence[YearBasis],
    normalized: pl.DataFrame,
    filings: pl.DataFrame,
    breakdowns: pl.DataFrame,
) -> pl.DataFrame:
    """``restated`` with each followed quarter's members swapped for the reprint.

    The reprint rows take the moved quarter's fiscal tag and quarter from its
    line, and their value as ``restated_value``, so Q4 subtracts the three
    restated quarters on that axis.
    """
    reprints = following_members(moved, normalized, filings, breakdowns)
    if reprints.is_empty():
        return restated
    slot = [*LINE, "axis", "period_end"]
    line = restated.filter(
        (pl.col("axis") == "") & pl.col("restated_value").is_not_null()
    )
    line_cols = [
        "fiscal_filing_id",
        "filing_period_end",
        "filing_fiscal_year",
        "fiscal_quarter",
        "filing_fiscal_tag",
        "fiscal_year",
        "fiscal_tag",
        "annual_period_end",
    ]
    context = ordering.distinct_on(
        line.select(*LINE, "period_end", *line_cols),
        [*LINE, "period_end"],
        ["fiscal_tag"],
    )
    added = (
        reprints.drop([c for c in line_cols if c in reprints.columns], strict=False)
        .join(context, on=[*LINE, "period_end"], how="inner")
        .with_columns(
            pl.lit("calculated").alias("source_type"),
            pl.lit(False).alias("is_comparative_backfill"),
            pl.lit(None, DECIMAL).alias("anchor_value"),
            pl.lit(False).alias("is_non_additive"),
            pl.lit(False).alias("is_per_share"),
            pl.lit(False).alias("drop_ytd"),
            pl.lit(None, DECIMAL).alias("q3_ytd"),
            pl.lit(False).alias("prefer_ytd_anchor"),
            pl.lit(False).alias("is_duplicate"),
            pl.col("value").alias("restated_value"),
        )
    )
    kept = restated.join(added.select(slot).unique(), on=slot, how="anti")
    return pl.concat(
        [kept, added.select(restated.columns).cast(restated.schema)],
        how="vertical",
    )
