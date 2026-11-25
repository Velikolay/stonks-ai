"""What a filing declares about itself, read once before its statements.

The cover page gives the fiscal year and quarter, the instance gives the
reporting currency, and the definition linkbase gives each axis default.
:class:`FilingContext` carries the last two to every fact built from the
filing, so the parser itself holds no per-filing state.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Optional

import pandas as pd

from .concepts import UNIT_PREFIXES
from .values import as_date, sec_member

logger = logging.getLogger(__name__)

# Cover-page period focus. A 10-K declares FY; a 10-Q declares Q1, Q2, or Q3.
_FOCUS_QUARTER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "FY": 4}

#: A translated amount and the reporting-currency amount and unit it stands for.
Translation = tuple[Decimal, Decimal, str]

#: ``(concept, period_end)`` to every translation of that concept on that date.
ReportingAmounts = dict[tuple[str, date], list[Translation]]


def fiscal_focus(entity_info: Optional[dict]) -> tuple[Optional[int], Optional[int]]:
    """The fiscal year and quarter declared on the filing cover.

    edgartools reads ``DocumentFiscalYearFocus`` and
    ``DocumentFiscalPeriodFocus`` into ``entity_info``. A missing or
    unrecognized value is None, so the caller can fall back to the period end.
    """
    if not entity_info:
        return None, None
    year_text = str(entity_info.get("fiscal_year") or "").strip()
    year = int(year_text) if year_text.isdigit() and len(year_text) == 4 else None
    period = str(entity_info.get("fiscal_period") or "").strip().upper()
    return year, _FOCUS_QUARTER.get(period)


def _reporting_unit(currency: str, unit_ref: object) -> str:
    code = currency.lower()
    ref = str(unit_ref or "").lower()
    return f"{code}PerShare" if "per_share" in ref or "pershare" in ref else code


def reporting_currency_amounts(facts: object) -> ReportingAmounts:
    """Each translated amount beside the reporting-currency amount it stands for.

    A 20-F often tags the current year twice, once in the reporting currency
    and once as a convenience translation. The statement frame keeps one of
    them and stamps that unit on the whole row, so a prior-year column in the
    reporting currency can sit beside a current-year translation. The
    reporting currency is the one most undimensioned facts use. With only one
    currency there is nothing to choose, and the result is empty.

    A translation shares its context with the amount it translates, so the two
    are matched on start and end. The end alone is not enough: a 10-K's year
    and its fourth quarter end on one date, and a single fact in another
    currency (Moody's one rupee amount) would otherwise swap the year for the
    quarter.
    """
    if facts is None or not isinstance(facts, pd.DataFrame) or facts.empty:
        return {}
    required = {"concept", "numeric_value", "currency", "period_end", "is_dimensioned"}
    if not required.issubset(facts.columns):
        return {}

    undimensioned = facts.loc[~facts["is_dimensioned"].fillna(False)].copy()
    if undimensioned.empty:
        return {}
    currencies = undimensioned["currency"].astype(str).str.strip().str.upper()
    undimensioned["currency_code"] = currencies
    undimensioned = undimensioned.loc[currencies.str.fullmatch(r"[A-Z]{3}")]
    if undimensioned["currency_code"].nunique() < 2:
        return {}
    reporting = str(undimensioned["currency_code"].value_counts().index[0])

    by_period: dict[tuple, tuple[Decimal, str]] = {}
    translated: list[tuple[tuple, Decimal]] = []
    for row in undimensioned.itertuples(index=False):
        concept = row.concept
        if not isinstance(concept, str) or concept == "":
            continue
        if pd.isna(row.numeric_value):
            continue
        period_end = as_date(row.period_end) or as_date(
            getattr(row, "period_instant", None)
        )
        if period_end is None:
            continue
        period = (concept, as_date(getattr(row, "period_start", None)), period_end)
        value = Decimal(str(row.numeric_value))
        if row.currency_code == reporting:
            unit_ref = getattr(row, "unit_ref", "")
            by_period.setdefault(period, (value, _reporting_unit(reporting, unit_ref)))
        else:
            translated.append((period, value))

    amounts: ReportingAmounts = {}
    for period, value in translated:
        original = by_period.get(period)
        if original is not None:
            concept, _, period_end = period
            amounts.setdefault((concept, period_end), []).append((value, *original))
    return amounts


def axis_defaults(xbrl: object) -> frozenset[str]:
    """Each axis default member: the member that means the whole axis."""
    axes = getattr(xbrl, "axes", None) or {}
    return frozenset(
        sec_member(member)
        for axis in axes.values()
        if (member := getattr(axis, "default_member_id", None))
    )


@dataclass(frozen=True)
class FilingContext:
    """Filing-wide facts that change how a statement row is read."""

    reporting_amounts: ReportingAmounts = field(default_factory=dict)
    axis_defaults: frozenset[str] = frozenset()

    @classmethod
    def of(cls, xbrl: object, accession: str = "") -> "FilingContext":
        amounts: ReportingAmounts = {}
        try:
            amounts = reporting_currency_amounts(xbrl.facts.to_dataframe())  # type: ignore[attr-defined]
        except Exception:
            logger.exception(
                "Could not read reporting-currency facts for %s", accession
            )
        return cls(amounts, axis_defaults(xbrl))

    def is_axis_total(self, member: str) -> bool:
        """The member means the whole axis rather than a slice.

        The definition linkbase names the domain as the axis default. Filers
        also tag the consolidated total as OperatingSegmentsMember beside one
        real slice, and that pair is still one region or one product.
        """
        local = member.rsplit(":", 1)[-1]
        return (
            member in self.axis_defaults
            or local == "OperatingSegmentsMember"
            or local.endswith("Domain")
        )

    def reporting_amount(
        self,
        concept: str,
        period_end: Optional[date],
        value: Optional[Decimal],
        unit: Optional[str],
    ) -> tuple[Optional[Decimal], Optional[str]]:
        """The reporting-currency amount and unit when the frame kept a translation.

        The frame stamps one unit on the whole row, so beside a translated
        column the others carry the translation's currency too. An amount
        that is no translation is in the reporting currency, and so is its
        unit.
        """
        if period_end is None or value is None:
            return value, unit
        for translation, amount, reporting_unit in self.reporting_amounts.get(
            (concept, period_end), []
        ):
            if _same_amount(value, translation) and not _same_amount(value, amount):
                return amount, reporting_unit
        return value, self._reporting_unit_of(unit)

    def _reporting_unit_of(self, unit: Optional[str]) -> Optional[str]:
        reporting = next(iter(self.reporting_amounts.values()), None)
        if not reporting:
            return unit
        code = reporting[0][2][:3]
        text = str(unit or "").strip().lower()
        per_share = text.endswith("pershare") or text.endswith("per_share")
        for suffix in ("_per_share", "pershare"):
            text = text.removesuffix(suffix)
        for prefix in UNIT_PREFIXES:
            text = text.removeprefix(prefix)
        if len(text) != 3 or not text.isalpha() or text == code:
            return unit
        return f"{code}PerShare" if per_share else code


def _same_amount(a: Decimal, b: Decimal) -> bool:
    return a == b or (b != 0 and abs(a - b) <= abs(b) * Decimal("1e-9"))
