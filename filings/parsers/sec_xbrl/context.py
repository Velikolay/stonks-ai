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

from .values import as_date, sec_member

logger = logging.getLogger(__name__)

# Cover-page period focus. A 10-K declares FY; a 10-Q declares Q1, Q2, or Q3.
_FOCUS_QUARTER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "FY": 4}

#: ``(concept, period_end)`` to ``(amount, unit)`` in the reporting currency.
ReportingAmounts = dict[tuple[str, date], tuple[Decimal, str]]


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
    """Each concept and period's amount in the reporting currency.

    A 20-F often tags the current year twice, once in the reporting currency
    and once as a convenience translation. The statement frame keeps one of
    them and stamps that unit on the whole row, so a prior-year column in the
    reporting currency can sit beside a current-year translation. The
    reporting currency is the one most undimensioned facts use. With only one
    currency there is nothing to choose, and the result is empty.
    """
    if facts is None or not isinstance(facts, pd.DataFrame) or facts.empty:
        return {}
    required = {"concept", "numeric_value", "currency", "period_end", "is_dimensioned"}
    if not required.issubset(facts.columns):
        return {}

    undimensioned = facts.loc[~facts["is_dimensioned"].fillna(False)].copy()
    if undimensioned.empty:
        return {}
    currencies = undimensioned["currency"].dropna().astype(str).str.strip().str.upper()
    currencies = currencies[currencies.str.fullmatch(r"[A-Z]{3}")]
    if currencies.nunique() < 2:
        return {}
    reporting = str(currencies.value_counts().index[0])

    chosen = undimensioned.loc[
        undimensioned["currency"].astype(str).str.strip().str.upper() == reporting
    ]
    amounts: ReportingAmounts = {}
    for row in chosen.itertuples(index=False):
        period_end = as_date(row.period_end) or as_date(
            getattr(row, "period_instant", None)
        )
        concept = row.concept
        if period_end is None or not isinstance(concept, str) or concept == "":
            continue
        if pd.isna(row.numeric_value):
            continue
        unit_ref = row.unit_ref if hasattr(row, "unit_ref") else ""
        amounts[(concept, period_end)] = (
            Decimal(str(row.numeric_value)),
            _reporting_unit(reporting, unit_ref),
        )
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
        """The reporting-currency amount and unit when the frame kept the other one."""
        if period_end is None or value is None:
            return value, unit
        chosen = self.reporting_amounts.get((concept, period_end))
        if chosen is None:
            return value, unit
        amount, reporting_unit = chosen
        if amount == value or (
            amount != 0 and abs(value - amount) <= abs(amount) * Decimal("1e-9")
        ):
            return value, unit
        return amount, reporting_unit
