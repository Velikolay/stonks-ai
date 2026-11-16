"""Tests for inferring the CIK a company filed under before a reorganization."""

from datetime import date
from types import SimpleNamespace

from filings.sec_xbrl_filings_loader import earlier_registrant_ciks


def _filing(form, filed, period, accession, ciks):
    return SimpleNamespace(
        form=form,
        filing_date=filed,
        report_date=period,
        accession_number=accession,
        all_ciks=ciks,
    )


def _company(cik, filings):
    def get_filings(*_args, **_kwargs):
        return filings

    return SimpleNamespace(cik=cik, get_filings=get_filings)


def test_earliest_10k_keeps_a_registrant_whose_reports_stop() -> None:
    """Alphabet's first 10-K names Google Inc., whose own reports end that quarter."""
    alphabet_filings = [
        _filing("10-Q", date(2015, 10, 29), date(2015, 9, 30), "A-Q", [1652044]),
        _filing(
            "10-K",
            date(2016, 2, 11),
            date(2015, 12, 31),
            "A-K",
            [1288776, 1652044],
        ),
    ]
    google_filings = [
        _filing("10-Q", date(2015, 10, 29), date(2015, 9, 30), "G-Q", [1288776]),
        _filing(
            "10-K", date(2016, 2, 11), date(2015, 12, 31), "A-K", [1288776, 1652044]
        ),
    ]
    companies = {
        1652044: _company(1652044, alphabet_filings),
        1288776: _company(1288776, google_filings),
    }

    assert earlier_registrant_ciks(companies[1652044], resolve=companies.get) == (
        "1288776",
    )


def test_a_coregistrant_that_keeps_filing_is_left_out() -> None:
    """A subsidiary that still files its own 10-Q is not the previous registrant."""
    parent_filings = [
        _filing("10-K", date(2016, 2, 11), date(2015, 12, 31), "P-K", [1, 2])
    ]
    subsidiary_filings = [
        _filing("10-Q", date(2024, 5, 1), date(2024, 3, 31), "S-Q", [2])
    ]
    companies = {1: _company(1, parent_filings), 2: _company(2, subsidiary_filings)}

    assert earlier_registrant_ciks(companies[1], resolve=companies.get) == ()


def test_a_filer_with_no_reports_of_its_own_is_left_out() -> None:
    """A CIK that only appears on this company's submission has no history to load."""
    filings = [_filing("10-K", date(2016, 2, 11), date(2015, 12, 31), "P-K", [1, 2])]
    companies = {1: _company(1, filings), 2: _company(2, filings)}

    assert earlier_registrant_ciks(companies[1], resolve=companies.get) == ()
