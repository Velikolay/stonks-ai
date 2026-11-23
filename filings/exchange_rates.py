"""Foreign-currency rates for statement period ends, in units per one US dollar.

ECB reference rates come from Frankfurter, one requested date at a time. A
weekend or holiday comes back as the previous business day. A currency that
series does not publish, such as the Taiwan dollar, uses the daily close in a
short window ending on that date.
"""

import logging
from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, List, Optional, Sequence, Tuple

import requests

logger = logging.getLogger(__name__)

# A period end on a weekend or holiday takes a rate from this many days before.
LOOKBACK_DAYS = 14

RateRow = Tuple[str, date, Decimal]


class _UnsupportedCurrency(Exception):
    """Frankfurter does not publish this currency."""


def _session() -> requests.Session:
    """Return an HTTP session that identifies this load."""
    session = requests.Session()
    session.headers["User-Agent"] = "Mozilla/5.0"
    session.headers["Cache-Control"] = "no-cache"
    return session


def _frankfurter_date(
    session: requests.Session, currency: str, day: date
) -> Optional[RateRow]:
    """Download the ECB rate for one date, quoted per one US dollar.

    Raises:
        _UnsupportedCurrency: The series does not publish this currency.
    """
    url = f"https://api.frankfurter.app/{day.isoformat()}?from=USD&to={currency}"
    response = session.get(url, timeout=30)
    if response.status_code == 404:
        raise _UnsupportedCurrency(currency)
    response.raise_for_status()
    body = response.json()
    value = (body.get("rates") or {}).get(currency)
    raw_date = body.get("date")
    if value in (None, 0) or not raw_date:
        return None
    return (currency, date.fromisoformat(raw_date), Decimal(str(value)))


def _yahoo_window(
    session: requests.Session, currency: str, start: date, end: date, host: str
) -> List[RateRow]:
    """Download daily closes from ``start`` through ``end``."""
    period_start = int(
        datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp()
    )
    # period2 is exclusive, so the end date's bar needs the following midnight.
    period_end = (
        int(datetime(end.year, end.month, end.day, tzinfo=timezone.utc).timestamp())
        + 86400
    )
    nonce = int(datetime.now(timezone.utc).timestamp() * 1000)
    url = (
        f"https://{host}.finance.yahoo.com/v8/finance/chart/"
        f"{currency}=X?period1={period_start}&period2={period_end}"
        f"&interval=1d&cb={nonce}"
    )
    response = session.get(url, timeout=60)
    response.raise_for_status()
    result = (response.json().get("chart") or {}).get("result") or [None]
    chart = result[0] or {}
    timestamps = chart.get("timestamp") or []
    quotes = (chart.get("indicators") or {}).get("quote") or [{}]
    closes = (quotes[0] or {}).get("close") or []
    rows: List[RateRow] = []
    for timestamp, close in zip(timestamps, closes):
        if close in (None, 0):
            continue
        rate_date = datetime.fromtimestamp(timestamp, timezone.utc).date()
        if rate_date < start or rate_date > end:
            continue
        rows.append((currency, rate_date, Decimal(str(close))))
    return rows


def _yahoo_date(
    session: requests.Session, currency: str, day: date
) -> Optional[RateRow]:
    """Download the latest daily close on or before ``day``."""
    start = day - timedelta(days=LOOKBACK_DAYS)
    for host in ("query1", "query2"):
        try:
            rows = _yahoo_window(session, currency, start, day, host)
        except requests.RequestException:
            logger.warning(
                "Close rate failed for %s on %s from %s", currency, day, host
            )
            continue
        if rows:
            return max(rows, key=lambda row: row[1])
    return None


def download_rates(
    currency: str,
    period_ends: Iterable[date],
    session: Optional[requests.Session] = None,
) -> List[RateRow]:
    """Download the rate for each period end.

    Args:
        currency: ISO currency code.
        period_ends: Statement period ends that need a rate. Dates after today
            are skipped.
        session: HTTP session to reuse.

    Returns:
        One observation per rate date. A weekend shares the previous business
        day's row. Empty when every date fails.
    """
    dates = sorted({day for day in period_ends if day <= date.today()})
    if not dates:
        return []
    http = session or _session()
    rows: List[RateRow] = []
    use_close = False
    for day in dates:
        row: Optional[RateRow] = None
        if not use_close:
            try:
                row = _frankfurter_date(http, currency, day)
            except _UnsupportedCurrency:
                use_close = True
            except requests.RequestException:
                logger.exception("ECB rate failed for %s on %s", currency, day)
        if row is None:
            row = _yahoo_date(http, currency, day)
        if row is None:
            logger.warning("No %s rate for %s", currency, day.isoformat())
            continue
        rows.append(row)
    by_date: dict[date, RateRow] = {}
    for row in rows:
        by_date[row[1]] = row
    return [by_date[rate_date] for rate_date in sorted(by_date)]


def rate_on_or_before(
    series: Sequence[tuple[date, Decimal]], period_end: date
) -> Optional[Decimal]:
    """Return the latest rate on or before ``period_end``.

    Args:
        series: Rates for one currency, sorted by date.
        period_end: Date of the amount.

    Returns:
        Foreign-currency units per one US dollar, or None when every rate is
        later than the date.
    """
    dates = [item[0] for item in series]
    index = bisect_right(dates, period_end) - 1
    if index < 0:
        return None
    return series[index][1]


def convert_series(
    period_values: Sequence[tuple[date, float]],
    series: Sequence[tuple[date, Decimal]],
) -> Optional[List[float]]:
    """Divide each amount by the rate on or before its date.

    Args:
        period_values: Date and amount, in the caller's order.
        series: Rates for one currency, sorted by date.

    Returns:
        Converted amounts, or None when any date has no positive rate. A
        partial result would mix currencies inside one series.
    """
    converted: List[float] = []
    for period_end, value in period_values:
        rate = rate_on_or_before(series, period_end)
        if rate is None or rate == 0:
            return None
        converted.append(float(Decimal(str(value)) / rate))
    return converted
