"""Tests for USD convenience translation."""

from datetime import date
from decimal import Decimal

from filings.fx import (
    convert_to_usd,
    currency_code,
    foreign_currency,
    reporting_currency,
)


class TestReportingCurrency:
    """The rate to load is the issuer's own currency."""

    def test_raw_unit_refs_name_their_currency(self):
        assert currency_code("eur") == "EUR"
        assert currency_code("eurPerShare") == "EUR"
        assert currency_code("iso4217_twd") == "TWD"
        assert currency_code("unit_twd") == "TWD"
        assert currency_code("usdeps") == "USD"
        assert currency_code("unit12") is None
        assert currency_code("shares") is None

    def test_majority_foreign_currency_is_loaded(self):
        assert reporting_currency({"unit_twd": 40, "iso4217_twd": 10, "usd": 2}) == (
            "TWD"
        )
        assert reporting_currency({"usd": 100, "eur": 1}) is None
        assert reporting_currency({"shares": 5}) is None


class TestForeignCurrency:
    """Which units a USD translation applies to."""

    def test_eur_and_per_share_are_foreign(self):
        assert foreign_currency("eur") == ("EUR", False)
        assert foreign_currency("twdPerShare") == ("TWD", True)

    def test_usd_shares_and_opaque_refs_stay(self):
        assert foreign_currency("usd") is None
        assert foreign_currency("usdPerShare") is None
        assert foreign_currency("shares") is None
        assert foreign_currency("unit12") is None
        assert foreign_currency(None) is None


class TestConvertToUsd:
    """One period-end rate converts every amount in the series."""

    def test_divides_by_the_period_end_rate(self):
        rates = {"TWD": [(date(2024, 12, 31), Decimal("32.79"))]}
        translated = convert_to_usd(
            "twd",
            [(date(2024, 12, 31), 2_894_307_700_000)],
            rates,
        )
        assert translated is not None
        unit, values = translated
        assert unit == "usd"
        assert values[0] == float(Decimal("2894307700000") / Decimal("32.79"))

    def test_per_share_keeps_the_per_share_unit(self):
        rates = {"EUR": [(date(2025, 12, 31), Decimal("0.92"))]}
        translated = convert_to_usd(
            "eurPerShare",
            [(date(2025, 12, 31), 24.73)],
            rates,
        )
        assert translated is not None
        assert translated[0] == "usdPerShare"

    def test_missing_rate_leaves_the_series(self):
        rates = {"EUR": [(date(2024, 12, 31), Decimal("0.96"))]}
        assert (
            convert_to_usd(
                "eur",
                [(date(2010, 12, 31), 100), (date(2024, 12, 31), 200)],
                rates,
            )
            is None
        )
