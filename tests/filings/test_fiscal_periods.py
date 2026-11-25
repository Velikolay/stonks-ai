"""Fiscal periods corrected against a company's year ends."""

from datetime import date
from itertools import count

from filings.fiscal_periods import FilingPeriod, corrected_periods

_ids = count(1)


def filing(form: str, end: str, year: int, quarter: int) -> FilingPeriod:
    return FilingPeriod(next(_ids), form, date.fromisoformat(end), year, quarter)


def corrections(*filings: FilingPeriod) -> dict[str, tuple[int, int]]:
    changes = corrected_periods(filings)
    return {f.period_end.isoformat(): changes[f.id] for f in filings if f.id in changes}


def test_an_annual_cover_that_still_declares_last_year_is_corrected() -> None:
    """Netflix's FY2012 10-K declares fiscal 2011 and a 2011 period end."""
    assert corrections(
        filing("10-K", "2010-12-31", 2010, 4),
        filing("10-K", "2011-12-31", 2011, 4),
        filing("10-K", "2012-12-31", 2011, 4),
        filing("10-K", "2013-12-31", 2013, 4),
    ) == {"2012-12-31": (2012, 4)}


def test_a_quarter_cover_on_the_previous_fiscal_year_is_corrected() -> None:
    """NVIDIA's fiscal 2021 first quarter declares fiscal 2020."""
    assert corrections(
        filing("10-Q", "2019-04-28", 2020, 1),
        filing("10-K", "2020-01-26", 2020, 4),
        filing("10-Q", "2020-04-26", 2020, 1),
        filing("10-K", "2021-01-31", 2021, 4),
    ) == {"2020-04-26": (2021, 1)}


def test_a_wrong_quarter_is_corrected() -> None:
    """Broadcom's second quarter of fiscal 2012 declares Q1."""
    assert corrections(
        filing("10-K", "2011-10-30", 2011, 4),
        filing("10-Q", "2012-01-29", 2012, 1),
        filing("10-Q", "2012-04-29", 2012, 1),
        filing("10-K", "2012-10-28", 2012, 4),
    ) == {"2012-04-29": (2012, 2)}


def test_a_calendar_fallback_is_moved_onto_the_fiscal_year() -> None:
    """Early covers declare no period; the calendar quarter was stored."""
    assert corrections(
        filing("10-Q", "2009-06-27", 2009, 2),
        filing("10-K", "2009-09-26", 2009, 3),
        filing("10-Q", "2009-12-26", 2009, 4),
        filing("10-K", "2010-09-25", 2010, 4),
    ) == {
        "2009-06-27": (2009, 3),
        "2009-09-26": (2009, 4),
        "2009-12-26": (2010, 1),
    }


def test_a_year_named_for_when_it_starts_keeps_that_convention() -> None:
    """A retailer's fiscal 2019 ends in February 2020; one cover is wrong."""
    assert corrections(
        filing("10-K", "2020-02-01", 2019, 4),
        filing("10-Q", "2020-05-02", 2020, 1),
        filing("10-Q", "2020-08-01", 2021, 2),
        filing("10-K", "2021-01-30", 2020, 4),
        filing("10-K", "2022-01-29", 2021, 4),
    ) == {"2020-08-01": (2020, 2)}


def test_quarters_of_uneven_length_keep_their_cover() -> None:
    """A 12-12-12-16 week year puts the third quarter 16 weeks before year end."""
    assert (
        corrections(
            filing("10-K", "2009-08-30", 2009, 4),
            filing("10-Q", "2009-11-22", 2010, 1),
            filing("10-Q", "2010-02-14", 2010, 2),
            filing("10-Q", "2010-05-09", 2010, 3),
            filing("10-K", "2010-08-29", 2010, 4),
        )
        == {}
    )


def test_a_year_ending_in_early_january_is_named_for_the_year_before() -> None:
    assert (
        corrections(
            filing("10-K", "2021-01-02", 2020, 4),
            filing("10-Q", "2021-10-02", 2021, 3),
            filing("10-K", "2022-01-01", 2021, 4),
        )
        == {}
    )


def test_quarters_after_the_latest_annual_report_are_checked() -> None:
    assert corrections(
        filing("10-K", "2024-12-31", 2024, 4),
        filing("10-Q", "2025-03-31", 2024, 1),
        filing("10-Q", "2025-06-30", 2025, 2),
    ) == {"2025-03-31": (2025, 1)}


def test_an_ambiguous_period_or_no_annual_report_changes_nothing() -> None:
    """A quarter six weeks before year end is no whole number of quarters."""
    assert (
        corrections(
            filing("10-K", "2023-12-31", 2023, 4),
            filing("10-Q", "2024-11-17", 2024, 1),
            filing("10-K", "2024-12-31", 2024, 4),
        )
        == {}
    )
    assert corrections(filing("10-Q", "2024-03-31", 2023, 1)) == {}
