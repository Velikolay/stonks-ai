"""Decide which stored fact units convert, then apply a period-end USD rate.

One rate applies to every line of a period, which is how a 20-F convenience
translation works. The rate series itself lives in ``filings.exchange_rates``.
"""

from datetime import date
from decimal import Decimal
from re import fullmatch, search
from typing import Mapping, Optional, Sequence

from filings.exchange_rates import convert_series

_MONETARY_UNIT = r"^([a-z]{3})(pershare)?$"
_CODE_PATTERNS = (
    r"^[a-z]{3}$",
    r"^[a-z]{3}pershare$",
    r"(?:iso4217_?|unit_)([a-z]{3})$",
    r"^u_iso4217([a-z]{3})$",
    r"^u_([a-z]{3})$",
)


def currency_code(unit: Optional[str]) -> Optional[str]:
    """Return the ISO currency named by a stored unit ref.

    Args:
        unit: Raw or canonical unit, such as ``eur``, ``iso4217_twd`` or ``unit12``.

    Returns:
        Upper-case currency, or None when the ref names no currency.
    """
    if not unit:
        return None
    lowered = unit.strip().lower()
    if lowered == "usdeps":
        return "USD"
    for pattern in _CODE_PATTERNS:
        matched = fullmatch(pattern, lowered) or search(pattern, lowered)
        if matched is None:
            continue
        code = matched.group(1) if matched.lastindex else matched.group(0)[:3]
        return code.upper()
    return None


def reporting_currency(unit_counts: Mapping[str, int]) -> Optional[str]:
    """Return the currency used by the most facts, when it is not USD.

    Args:
        unit_counts: Unit ref to the number of facts carrying it.

    Returns:
        The issuer's foreign reporting currency, or None for a USD filer.
    """
    totals: dict[str, int] = {}
    for unit, count in unit_counts.items():
        code = currency_code(unit)
        if code is None:
            continue
        totals[code] = totals.get(code, 0) + count
    if not totals:
        return None
    code = max(totals, key=lambda item: totals[item])
    if code == "USD":
        return None
    return code


def foreign_currency(unit: Optional[str]) -> Optional[tuple[str, bool]]:
    """Return the ISO code and whether the unit is per share.

    USD amounts and non-monetary units such as ``shares`` are left alone.

    Args:
        unit: Canonical fact unit, for example ``eur`` or ``twdPerShare``.

    Returns:
        Upper-case currency and a per-share flag, or None when no translation
        applies.
    """
    if not unit:
        return None
    matched = fullmatch(_MONETARY_UNIT, unit.strip().lower())
    if matched is None or matched.group(1) == "usd":
        return None
    return matched.group(1).upper(), matched.group(2) is not None


def convert_to_usd(
    unit: Optional[str],
    period_values: Sequence[tuple[Optional[date], float]],
    rates: Mapping[str, Sequence[tuple[date, Decimal]]],
) -> Optional[tuple[str, list[float]]]:
    """Convert one series to USD, or leave it unchanged when a rate is missing.

    A series is converted only when every period has a rate. Converting some
    periods and not others would label a foreign amount as USD.

    Args:
        unit: Canonical unit of the series.
        period_values: Period end and filed amount, in period order.
        rates: Currency code to dated foreign-units-per-USD rates.

    Returns:
        The USD unit and converted amounts, or None to keep the filed series.
    """
    parsed = foreign_currency(unit)
    if parsed is None:
        return None
    code, per_share = parsed
    series = rates.get(code)
    if not series:
        return None
    dated: list[tuple[date, float]] = []
    for period_end, value in period_values:
        if period_end is None:
            return None
        dated.append((period_end, value))
    converted = convert_series(dated, series)
    if converted is None:
        return None
    return ("usdPerShare" if per_share else "usd", converted)
