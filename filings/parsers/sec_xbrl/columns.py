"""The period columns of an edgartools statement frame.

A column is named by its end date, optionally followed by a period token:
``2025-06-28 (Q2)``, ``2025-06-28 (YTD)``, ``2024-12-31 (FY)`` or just
``2024-12-31`` for an instant. The helpers here pick which columns a filing
reports and which earlier column is each one's comparative.
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from ...models import PeriodType
from .values import as_date

_QUARTER_TOKENS = ("q1", "q2", "q3", "q4", "quarter")
#: Two period ends this many days apart are the same period a year apart.
_YEAR_GAP_DAYS = (340, 380)
_ISO_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


def column_end(column: str) -> str:
    """The end-date part of a column name, as text."""
    return column.split(" ")[0]


def is_quarter_column(column: str) -> bool:
    """The column reports a single quarter."""
    lowered = column.lower()
    return any(token in lowered for token in _QUARTER_TOKENS)


def column_shape(column: str) -> str:
    """The period token after the date: ``q`` for any quarter, else lowercased.

    Every quarter is ``q``: edgartools numbers quarters from the calendar for
    some filers, so a fiscal Q1 can sit beside a prior-year "Q2". A plain date
    has an empty shape.
    """
    parts = column.split(" ", 1)
    if len(parts) < 2:
        return ""
    if is_quarter_column(column):
        return "q"
    return parts[1].lower()


def is_mostly_empty(df: pd.DataFrame, column: str, threshold: float = 20.0) -> bool:
    """Fewer than ``threshold`` percent of the column's cells hold a value.

    edgartools issue 408 returns some period columns nearly empty.
    """
    if df is None or df.empty or column not in df.columns:
        return True
    filled = df[column].apply(lambda x: x is not None and x != "" and pd.notna(x)).sum()
    return bool(filled / len(df) * 100 < threshold)


def period_columns(frame: pd.DataFrame) -> list[str]:
    """The frame's columns that are periods and hold values."""
    return [
        column
        for column in frame.columns
        if isinstance(column, str)
        and ("-" in column or "/" in column)
        and not is_mostly_empty(frame, column)
    ]


def same_shape(columns: list[str], exemplar: str) -> list[str]:
    """Columns with the exemplar's shape, oldest first.

    A Q3 column matches the prior-year Q3, not the YTD beside it.
    """
    shape = column_shape(exemplar)
    return sorted((c for c in columns if column_shape(c) == shape), key=column_end)


def prior_column(columns: list[str], column: str) -> Optional[str]:
    """``column``'s comparative.

    For a duration, the same period a year earlier: the column of the same
    shape ending 340 to 380 days before, else the closest earlier one. A 10-Q
    statement of equity also prints the year's earlier quarters, which are
    not the comparative. For an instant, the earlier date the filing prints
    first: on a 10-Q balance sheet, the prior year end rather than the same
    quarter a year ago.

    Args:
        columns: Period columns in the frame's order.
        column: The column whose comparative is needed.
    """
    shape, end = column_shape(column), column_end(column)
    earlier = [c for c in columns if column_shape(c) == shape and column_end(c) < end]
    if not earlier:
        return None
    if shape == "":
        return earlier[0]
    end_date = as_date(end)
    year_back = [
        c
        for c in earlier
        if end_date is not None
        and (start := as_date(column_end(c))) is not None
        and _YEAR_GAP_DAYS[0] <= (end_date - start).days <= _YEAR_GAP_DAYS[1]
    ]
    return max(year_back or earlier, key=column_end)


def current_column(columns: list[str]) -> str:
    """The column for the filing's own period.

    A quarter and a year-to-date column often share an end date, and the
    year-to-date name sorts last; the quarter is the period the filing reports.
    """
    latest_end = max(column_end(c) for c in columns)
    current = [c for c in columns if column_end(c) == latest_end]
    quarters = [c for c in current if is_quarter_column(c)]
    return sorted(quarters or current)[-1]


def same_date_ytd_column(columns: list[str], quarter_column: str) -> Optional[str]:
    """The year-to-date column ending on the quarter column's date, if any.

    That is a ``(YTD)`` column or a bare date (Costco's 24 weeks). A ``(FY)``
    column beside a quarter is a trailing twelve months, which Amazon's 10-Q
    cash flow statements print, and not the year to date.
    """
    if not is_quarter_column(quarter_column):
        return None
    end = column_end(quarter_column)
    ytd = [
        c for c in columns if column_end(c) == end and column_shape(c) in ("(ytd)", "")
    ]
    return sorted(ytd)[-1] if ytd else None


def period_type(
    column: str, statement: str, point_in_time: object = None
) -> Optional[PeriodType]:
    """The period a column's amount covers: a quarter, year to date, or None.

    Instant facts have no period, on any statement. edgartools sets
    ``point_in_time`` True for instants (fixed in 5.58.0 for instants under
    duration columns). A duration is a quarter when the column says so,
    otherwise year to date.
    """
    if _as_bool(point_in_time) is True or statement == "Balance Sheet":
        return None
    if not _ISO_DATE.search(column):
        return None
    return PeriodType.Q if is_quarter_column(column) else PeriodType.YTD


def _as_bool(value: object) -> Optional[bool]:
    """A pandas, numpy or Python boolean; None when missing or NaN."""
    if value is True or value is False:
        return bool(value)
    if value is None:
        return None
    try:
        return None if value != value else bool(value)
    except (TypeError, ValueError):
        return None
