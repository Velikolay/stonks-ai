"""Tests for a dated rate series."""

from datetime import date, datetime, timezone
from decimal import Decimal

import requests

from filings.exchange_rates import convert_series, download_rates, rate_on_or_before


class TestRateOnOrBefore:
    """A closed market uses the previous business day's rate."""

    def test_weekend_period_end_uses_the_prior_print(self):
        series = [
            (date(2024, 12, 30), Decimal("32.50")),
            (date(2024, 12, 31), Decimal("32.79")),
        ]
        assert rate_on_or_before(series, date(2024, 12, 31)) == Decimal("32.79")
        assert rate_on_or_before(series, date(2025, 1, 1)) == Decimal("32.79")
        assert rate_on_or_before(series, date(2024, 12, 29)) is None


class TestConvertSeries:
    """Every amount uses the rate on or before its own date."""

    def test_divides_each_amount(self):
        series = [(date(2024, 12, 31), Decimal("32.79"))]
        converted = convert_series([(date(2024, 12, 31), 2_894_307_700_000)], series)
        assert converted == [float(Decimal("2894307700000") / Decimal("32.79"))]

    def test_missing_rate_converts_nothing(self):
        series = [(date(2024, 12, 31), Decimal("0.96"))]
        assert (
            convert_series(
                [(date(2010, 12, 31), 100), (date(2024, 12, 31), 200)],
                series,
            )
            is None
        )


class _Response:
    """Minimal response for a scripted HTTP session."""

    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


class _Session:
    """Records GET urls and answers from a handler."""

    def __init__(self, handler):
        self.calls: list[str] = []
        self._handler = handler

    def get(self, url: str, timeout: int = 30, headers: dict = None) -> _Response:
        self.calls.append(url)
        return self._handler(url)


def _stamp(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())


class TestDownloadRates:
    """Only the requested period ends are fetched."""

    def test_weekend_reuses_the_ecb_business_day(self):
        def handler(url: str) -> _Response:
            if url.endswith("2025-01-01?from=USD&to=EUR"):
                return _Response(200, {"date": "2024-12-31", "rates": {"EUR": 0.96}})
            return _Response(200, {"date": "2024-12-31", "rates": {"EUR": 0.96}})

        session = _Session(handler)
        rows = download_rates(
            "EUR",
            [date(2025, 1, 1), date(2024, 12, 31), date(2099, 1, 1)],
            session,
        )
        assert rows == [("EUR", date(2024, 12, 31), Decimal("0.96"))]
        assert len(session.calls) == 2
        assert all("frankfurter.app" in url for url in session.calls)

    def test_missing_currency_uses_the_close_on_or_before(self):
        def handler(url: str) -> _Response:
            if "frankfurter.app" in url:
                return _Response(404, {})
            return _Response(
                200,
                {
                    "chart": {
                        "result": [
                            {
                                "timestamp": [
                                    _stamp(date(2024, 12, 30)),
                                    _stamp(date(2024, 12, 31)),
                                    _stamp(date(2025, 1, 1)),
                                ],
                                "indicators": {
                                    "quote": [{"close": [32.5, 32.76, 32.8]}]
                                },
                            }
                        ]
                    }
                },
            )

        session = _Session(handler)
        rows = download_rates("TWD", [date(2024, 12, 31)], session)
        assert rows == [("TWD", date(2024, 12, 31), Decimal("32.76"))]
        assert sum("frankfurter.app" in url for url in session.calls) == 1
        assert sum("finance.yahoo.com" in url for url in session.calls) == 1
