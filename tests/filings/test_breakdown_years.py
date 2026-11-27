"""Breakdown years as the API sends them."""

from filings.db.dimension_breakdowns import filed_years


def test_years_before_the_first_annual_report_do_not_date_a_split() -> None:
    """Apple's regions have FY2008 from the FY2009 10-K's comparative only."""
    assert filed_years([2012, 2008, 2009], 2009) == [2009, 2012]


def test_a_split_with_only_earlier_years_keeps_them() -> None:
    assert filed_years([2008], 2009) == [2008]


def test_without_an_annual_report_every_year_stays() -> None:
    assert filed_years([2021, 2020], None) == [2020, 2021]
