"""Tests for the pinned company-id lock."""

import pytest

from filings.company_ids import company_id_by_ticker, locked_company_id


def test_locked_tickers_match_override_companies() -> None:
    """The lock file pins the companies the override CSVs refer to."""
    mapping = company_id_by_ticker()
    assert mapping["AAPL"] == 1
    assert mapping["GOOGL"] == 2
    assert mapping["GOOG"] == 2
    assert mapping["AMZN"] == 3
    assert mapping["MSFT"] == 4
    assert mapping["TSLA"] == 5
    assert mapping["UBER"] == 6
    assert mapping["NFLX"] == 7
    assert mapping["NVDA"] == 8
    assert mapping["META"] == 9


def test_locked_company_id_is_case_insensitive() -> None:
    """Ticker matching ignores case."""
    assert locked_company_id(["nvda"]) == 8
    assert locked_company_id(["ACME"]) is None


def test_locked_company_id_rejects_mixed_companies() -> None:
    """One EDGAR company cannot resolve to two pinned ids."""
    with pytest.raises(ValueError):
        locked_company_id(["AAPL", "MSFT"])
