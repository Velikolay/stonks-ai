"""Statement cells and fact metadata as the values stored on a fact."""

from __future__ import annotations

import logging
import math
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def is_nullish(value: object) -> bool:
    """True for None, NaN, pandas NA, and anything that is not a number."""
    if value is None:
        return True
    try:
        return math.isnan(value)  # type: ignore[arg-type]
    except Exception:
        return True


def to_decimal(value: object) -> Decimal:
    """A statement amount as Decimal, rounded to five places.

    edgartools returns binary floats, so 54.65 can arrive as
    54.650000000000006. The fact model allows five decimal places.

    Raises:
        ValueError, TypeError: ``value`` is not a number.
    """
    parsed = Decimal(str(value))
    exponent = parsed.as_tuple().exponent
    if isinstance(exponent, int) and exponent < -5:
        return parsed.quantize(Decimal("0.00001"))
    return parsed


def optional_decimal(value: object, what: str, concept: str) -> Optional[Decimal]:
    """:func:`to_decimal` of a cell that may be empty; a bad number logs and is None."""
    if is_nullish(value):
        return None
    try:
        return to_decimal(value)
    except (ValueError, TypeError):
        logger.warning("Invalid %s for concept %s: %s", what, concept, value)
        return None


def as_date(value: object) -> Optional[date]:
    """A date from a fact period, column name or filing field; None when missing."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text or text.lower() == "nat":
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def filed_unit(unit: object, default: Optional[str] = None) -> Optional[str]:
    """The unit ref as the filing wrote it, or ``default`` when blank.

    Currency canonicalization happens when the normalized facts are built.
    """
    if unit is None or (isinstance(unit, float) and pd.isna(unit)):
        return default
    text = str(unit).strip()
    return text if text else default


def sec_concept(concept: object) -> Optional[str]:
    """``prefix:LocalName`` from edgartools' ``prefix_LocalName``; None when missing."""
    if not isinstance(concept, str) or not concept:
        return None
    return concept.replace("_", ":", 1)


def sec_member(member: object) -> str:
    """A dimension axis or member id with a colon namespace."""
    return str(member).replace("_", ":")


def fact_key(*parts: object) -> str:
    """Deterministic key of a fact from its identity, so reloads keep the same key."""
    return str(uuid.uuid5(uuid.NAMESPACE_OID, "|".join(str(p) for p in parts)))


def print_sign(row: object) -> Optional[Decimal]:
    """The presentation-linkbase sign (+1 or -1); the instance amount stays in value.

    edgartools names it ``preferred_sign``.
    """
    getter = getattr(row, "get", None)
    sign = getter("preferred_sign") if getter else None
    if is_nullish(sign):
        return None
    try:
        return to_decimal(sign)
    except (ValueError, TypeError):
        logger.warning("Invalid preferred_sign: %s", sign)
        return None


def flag(value: object) -> bool:
    """True for a statement flag cell, including numpy booleans and text."""
    if isinstance(value, bool):
        return value
    text = "" if value is None else str(value).strip().lower()
    return text in {"true", "1"}


def cell_text(value: object) -> str:
    """A statement cell as text, empty when the cell is empty."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return str(value)
