"""Year-to-date totals reduced to discrete quarters, one series at a time.

A series is one line and member. Its 10-Q amounts are walked in period
order. A YTD figure becomes a quarter by subtracting the amount one quarter
before it (80 to 100 days), or that quarter's own YTD total when the
earlier filing published the quarter beside it. A YTD with no quarter
before it is still a six- or nine-month total and is not shown.

A series that opens on such a YTD can still start with a quarter: when next
year's filing prints the missing quarter as a comparative, that comparative
is added as a backfill row, the opening YTD reduces against it, and the
backfill itself is not shown.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Iterator, Optional, Sequence

import polars as pl

from ...records import ROW, frame_of, grouped, numbered, records
from ...rules.ids import abs_hash_expr
from ...rules.periods import consecutive_quarters
from .rows import SERIES, is_non_additive, is_per_share

DECIMAL = pl.Decimal(38, 10)


# --- Backfilling the quarter before a series opens ---------------------------


@dataclass(slots=True)
class Opening:
    """A 10-Q row, as the search for an opening comparative reads it."""

    series: tuple
    row_nr: int
    id: int
    period_end: date
    period: Optional[str]
    is_abstract: bool
    has_comparative: bool
    comparative_period_end: Optional[date]


def opening_comparative(rows: Sequence[Opening]) -> Optional[tuple[int, int]]:
    """``(opening row, source row)`` when a comparative fills the quarter before.

    The series must open on a YTD. The source is the earliest row whose
    comparative ended one quarter before that opening.
    """
    opening = min(rows, key=lambda r: (r.period_end, r.is_abstract, r.id))
    if opening.period != "YTD":
        return None
    sources = [
        r
        for r in rows
        if r.has_comparative
        and r.comparative_period_end is not None
        and consecutive_quarters(opening.period_end, r.comparative_period_end)
    ]
    if not sources:
        return None
    source = min(sources, key=lambda r: (r.period_end, r.id))
    return opening.row_nr, source.row_nr


def comparative_backfill(quarters: pl.DataFrame) -> pl.DataFrame:
    """Backfill rows: the opening row, moved to its source's comparative."""
    rows = numbered(quarters).with_columns(
        pl.col("comparative_value").is_not_null().alias("has_comparative")
    )
    by_series = grouped(records(rows, Opening, key=SERIES), lambda r: r.series)
    found = [p for rs in by_series.values() if (p := opening_comparative(rs))]
    pairs = frame_of(found, {ROW: pl.UInt32, "source": pl.UInt32})
    source = rows.select(
        pl.col(ROW).alias("source"),
        pl.col("filing_id").alias("source_filing_id"),
        pl.col("fiscal_quarter").alias("source_fiscal_quarter"),
        pl.col("period").alias("source_period"),
        pl.col("comparative_value").alias("source_value"),
        pl.col("comparative_period_end").alias("source_period_end"),
    )
    opening = rows.join(pairs, on=ROW).join(source, on="source")
    return opening.with_columns(
        pl.col("source_filing_id").alias("filing_id"),
        abs_hash_expr(
            "quarterly-prior",
            pl.col("company_id"),
            pl.col("statement"),
            pl.col("normalized_label"),
            pl.col("axis"),
            pl.col("member"),
            pl.col("source_period_end"),
        ).alias("id"),
        pl.col("source_fiscal_quarter").alias("fiscal_quarter"),
        pl.col("source_value").alias("value"),
        pl.col("source_period_end").alias("period_end"),
        pl.col("source_period").alias("period"),
        pl.lit(None, pl.Date).alias("comparative_period_end"),
        pl.lit(None, DECIMAL).alias("comparative_value"),
        pl.lit(False).alias("is_abstract"),
        pl.lit(True).alias("is_synthetic"),
        pl.lit("calculated").alias("source_type"),
        pl.lit(True).alias("is_comparative_backfill"),
    ).select(
        quarters.columns
    )  # drops the source and has_comparative columns


# --- Reducing each series ----------------------------------------------------


@dataclass(slots=True)
class Amount:
    """A 10-Q amount of one series, as the reduction reads it."""

    series: tuple
    row_nr: int
    period_end: date
    period: Optional[str]
    value: Optional[Decimal]
    #: The YTD total a published quarter was printed beside, hidden by dedup.
    anchor_value: Optional[Decimal]
    fiscal_quarter: Optional[int]
    #: Period end of the 10-K that closes this amount's fiscal year.
    annual_period_end: Optional[date]
    is_non_additive: bool
    is_per_share: bool

    @property
    def amount_to_subtract(self) -> Optional[Decimal]:
        """What the next quarter's YTD subtracts: this quarter's own YTD if printed."""
        if self.period == "Q" and self.anchor_value is not None:
            return self.anchor_value
        return self.value

    @property
    def counts_for_q4(self) -> bool:
        """An additive amount of the quarter just before the 10-K."""
        return (
            not self.is_non_additive
            and not self.is_per_share
            and self.annual_period_end is not None
            and consecutive_quarters(self.annual_period_end, self.period_end)
        )


@dataclass(slots=True)
class Reduced:
    """One amount as a discrete quarter."""

    row_nr: int
    value: Optional[Decimal]
    #: An unreduced YTD: a six- or nine-month total, not shown as a quarter.
    drop_ytd: bool
    #: The nine months before the 10-K, kept for Q4.
    q3_ytd: Optional[Decimal]
    #: A published Q3 beside its nine-month total: Q4 subtracts that total.
    prefer_ytd_anchor: bool


def reduce_series(amounts: Sequence[Amount]) -> Iterator[Reduced]:
    """Every amount of one series as a discrete quarter, in period order."""
    previous: Optional[Amount] = None
    for amount in sorted(amounts, key=lambda a: a.period_end):
        prior = None
        if previous is not None and consecutive_quarters(
            amount.period_end, previous.period_end
        ):
            prior = previous.amount_to_subtract
        yield reduce(amount, opens_series=previous is None, prior=prior)
        previous = amount


def reduce(amount: Amount, opens_series: bool, prior: Optional[Decimal]) -> Reduced:
    """One amount, given the amount a quarter before it (``prior``), if any."""
    reducible = amount.period == "YTD" and not amount.is_non_additive
    value = amount.value
    if reducible and prior is not None and value is not None:
        value -= prior
    # A YTD left unreduced is the opening of a series, or a Q2/Q3 whose prior
    # quarter is missing. An unreduced Q1 is that quarter.
    drop = reducible and (
        opens_series or (prior is None and amount.fiscal_quarter in (2, 3))
    )

    published_q3 = (
        amount.period == "Q"
        and amount.anchor_value is not None
        and amount.counts_for_q4
    )
    q3_ytd = None
    if published_q3:
        q3_ytd = amount.anchor_value
    elif amount.period == "YTD" and amount.counts_for_q4:
        q3_ytd = amount.value
    return Reduced(amount.row_nr, value, drop, q3_ytd, published_q3)


REDUCED_SCHEMA = {
    ROW: pl.UInt32,
    "value": DECIMAL,
    "drop_ytd": pl.Boolean,
    "q3_ytd": DECIMAL,
    "prefer_ytd_anchor": pl.Boolean,
}


def discrete_quarters(
    quarters: pl.DataFrame,
    backfill: pl.DataFrame,
    anchors: pl.DataFrame,
    filings: pl.DataFrame,
) -> pl.DataFrame:
    """Every 10-Q row with its YTD reduced; headers pass through unchanged.

    Added columns: ``is_non_additive``, ``is_per_share``, ``drop_ytd``,
    ``q3_ytd`` and ``prefer_ytd_anchor`` (see :class:`Reduced`).
    """
    keys = [*SERIES, "period_end", "filing_id", "concept"]
    annual = filings.select(
        pl.col("id").alias("fiscal_tag"),
        pl.col("fiscal_period_end").alias("annual_period_end"),
    )
    amounts = (
        pl.concat([quarters.filter(~pl.col("is_abstract")), backfill])
        .join(anchors, on=keys, how="left")
        .join(annual, on="fiscal_tag", how="left")
        .with_columns(
            is_non_additive().alias("is_non_additive"),
            is_per_share(pl.col("unit")).alias("is_per_share"),
        )
    )
    amounts = numbered(amounts)
    by_series = grouped(records(amounts, Amount, key=SERIES), lambda a: a.series)
    reduced = frame_of(
        (
            (r.row_nr, r.value, r.drop_ytd, r.q3_ytd, r.prefer_ytd_anchor)
            for series in by_series.values()
            for r in reduce_series(series)
        ),
        REDUCED_SCHEMA,
    )
    amounts = amounts.drop("value").join(reduced, on=ROW).drop(ROW)

    headers = quarters.filter(pl.col("is_abstract")).with_columns(
        pl.lit(None, DECIMAL).alias("anchor_value"),
        pl.lit(None, pl.Date).alias("annual_period_end"),
        pl.lit(False).alias("is_non_additive"),
        pl.lit(False).alias("is_per_share"),
        pl.lit(False).alias("drop_ytd"),
        pl.lit(None, DECIMAL).alias("q3_ytd"),
        pl.lit(False).alias("prefer_ytd_anchor"),
    )
    return pl.concat([amounts, headers.select(amounts.columns)])
