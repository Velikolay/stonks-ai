"""Forms, fiscal periods and the gaps between them."""

from __future__ import annotations

import polars as pl

ANNUAL_FORMS = ("10-K", "10-K/A")
QUARTERLY_FORMS = ("10-Q", "10-Q/A")
FOREIGN_ANNUAL_FORMS = ("20-F", "20-F/A")
AMENDMENTS = ("10-K/A", "10-Q/A", "20-F/A")

#: Two quarter-ends this many days apart are consecutive quarters.
QUARTER_GAP_DAYS = (80, 100)
#: Two period-ends this many days apart are the same quarter a year apart.
YEAR_GAP_DAYS = (340, 380)


def form_family(form: pl.Expr) -> pl.Expr:
    """``10-K`` for 10-K and 10-K/A, and likewise for 10-Q and 20-F."""
    return (
        pl.when(form.is_in(ANNUAL_FORMS))
        .then(pl.lit("10-K"))
        .when(form.is_in(QUARTERLY_FORMS))
        .then(pl.lit("10-Q"))
        .when(form.is_in(FOREIGN_ANNUAL_FORMS))
        .then(pl.lit("20-F"))
        .otherwise(form)
    )


def days_between(later: pl.Expr, earlier: pl.Expr) -> pl.Expr:
    """``later - earlier`` in days, as SQL date subtraction gives."""
    return (later - earlier).dt.total_days()


def within(days: pl.Expr, bounds: tuple[int, int]) -> pl.Expr:
    """``days BETWEEN low AND high``."""
    return days.is_between(bounds[0], bounds[1], closed="both")


def quarter_apart(later: pl.Expr, earlier: pl.Expr) -> pl.Expr:
    """The two dates are consecutive quarter-ends (80 to 100 days)."""
    return within(days_between(later, earlier), QUARTER_GAP_DAYS)


def fiscal_year_of(
    filing_fiscal_year: pl.Expr, filing_period_end: pl.Expr, period_end: pl.Expr
) -> pl.Expr:
    """Fiscal year of a column for ``period_end`` printed in another year's filing.

    Zero shift on the filing that closed ``period_end``; otherwise the
    calendar-year gap, for a period whose own filing was never ingested.
    """
    return filing_fiscal_year - (filing_period_end.dt.year() - period_end.dt.year())
