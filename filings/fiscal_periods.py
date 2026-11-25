"""Each filing's fiscal year and quarter, checked against the company's year ends.

A filing's cover declares its fiscal year and quarter, and the cover is
sometimes wrong. Netflix's FY2012 10-K still declares 2011, NVIDIA's first
two 10-Qs of fiscal 2021 declare 2020, and a Broadcom 10-Q for its second
quarter declares Q1. A filing that claims another filing's period hides one
of the two from every fiscal-year lookup.

The company's annual reports give its fiscal year ends, so a filing's
quarter follows from how far its period end sits before the next year end.
The year follows from that year end and the company's naming convention:
most companies name a fiscal year for the calendar year it ends in, while
some retailers name it for the year it starts in. The convention is the
offset most of the company's covers use, so one wrong cover cannot change
it. A filing is corrected only when its period is unambiguous and its cover
disagrees.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, Optional

ANNUAL_FORMS = frozenset({"10-K", "10-K/A", "20-F", "20-F/A"})

#: A quarter is a quarter of a year, in days.
QUARTER_DAYS = 365.25 / 4
#: How far a quarter end may sit from a whole number of quarters before the
#: year end. A 52/53-week year moves its quarters by days, and a 12-12-12-16
#: week year (Costco) moves its third quarter by three weeks.
QUARTER_TOLERANCE_DAYS = 30
#: A fiscal year ending this early in January is named for the year before
#: (a 52/53-week year ending on the Saturday nearest December 31).
EARLY_JANUARY_DAYS = 7
#: Year ends are extrapolated this many years past the first and last report.
EXTRAPOLATED_YEARS = 2

#: Fiscal year and quarter.
Period = tuple[int, int]


@dataclass(frozen=True)
class FilingPeriod:
    """What a filing declares about its period."""

    id: int
    form_type: str
    period_end: date
    fiscal_year: int
    fiscal_quarter: int


def _shift_years(day: date, years: int) -> date:
    try:
        return day.replace(year=day.year + years)
    except ValueError:  # 29 February
        return day.replace(year=day.year + years, day=28)


def _year_end_candidates(year_ends: list[date]) -> list[date]:
    """The reported year ends, and each one a year or two either side.

    The neighbours stand in for a year whose annual report is missing, and
    for the years before the first report and after the latest one.
    """
    candidates = set(year_ends)
    for end in year_ends:
        for years in range(1, EXTRAPOLATED_YEARS + 1):
            candidates.add(_shift_years(end, years))
            candidates.add(_shift_years(end, -years))
    return sorted(candidates)


def _named_year(year_end: date) -> int:
    """The calendar year a fiscal year ending on ``year_end`` is counted in."""
    return (year_end - timedelta(days=EARLY_JANUARY_DAYS)).year


def _position(
    filing: FilingPeriod, candidates: list[date]
) -> Optional[tuple[date, int]]:
    """The fiscal year end the filing's period falls in, and its quarter."""
    if filing.form_type in ANNUAL_FORMS:
        return filing.period_end, 4
    later = [end for end in candidates if end > filing.period_end]
    if not later:
        return None
    year_end = later[0]
    days = (year_end - filing.period_end).days
    quarters_left = round(days / QUARTER_DAYS)
    quarter = 4 - quarters_left
    if not 1 <= quarter <= 3:
        return None
    if abs(days - quarters_left * QUARTER_DAYS) > QUARTER_TOLERANCE_DAYS:
        return None
    return year_end, quarter


def corrected_periods(filings: Iterable[FilingPeriod]) -> dict[int, Period]:
    """Filing id to the fiscal year and quarter it should carry.

    Only filings whose stored period differs are returned. A company with no
    annual report, or whose covers follow no convention, gets no corrections.
    """
    filings = list(filings)
    year_ends = sorted({f.period_end for f in filings if f.form_type in ANNUAL_FORMS})
    if not year_ends:
        return {}
    candidates = _year_end_candidates(year_ends)

    positions = {
        f.id: position
        for f in filings
        if (position := _position(f, candidates)) is not None
    }
    offsets = Counter(
        f.fiscal_year - _named_year(positions[f.id][0])
        for f in filings
        if f.id in positions
    )
    if not offsets:
        return {}
    # The most common offset; a tie goes to naming the year it ends in.
    offset = max(offsets, key=lambda o: (offsets[o], o == 0, o))
    if offset not in (0, -1):
        return {}

    changes: dict[int, Period] = {}
    for f in filings:
        if f.id not in positions:
            continue
        year_end, quarter = positions[f.id]
        period = (_named_year(year_end) + offset, quarter)
        if period != (f.fiscal_year, f.fiscal_quarter):
            changes[f.id] = period
    return changes
