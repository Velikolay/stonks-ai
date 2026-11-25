"""Telling a reprint rounded to thousands or millions from a real change.

Later filings reprint earlier periods rounded (millions beside an original
in dollars). Such a value equals the original within half its rounding unit,
and within 0.1%.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Union

import polars as pl


def decimal(value: Union[int, str, Decimal]) -> pl.Expr:
    """A Decimal(38, 10) literal that mixes with value columns without float casts."""
    return pl.lit(Decimal(value), dtype=pl.Decimal(38, 10))


def round_half_away(expr: pl.Expr) -> pl.Expr:
    """Round to a whole number with halves away from zero, as filers round.

    polars rounds halves to even.
    """
    return (expr.abs() + Decimal("0.5")).floor() * expr.sign()


def is_multiple_of(expr: pl.Expr, unit: int) -> pl.Expr:
    """``expr`` is a whole multiple of ``unit``; polars has no Decimal remainder."""
    return (expr / unit).floor() * unit == expr


#: A rounded reprint is off by at most this share of the value.
REPRINT_SHARE = Decimal("0.001")


def reprint_tolerance_of(value: Decimal) -> Decimal:
    """:func:`reprint_tolerance` for one value."""
    if value != 0 and value % 1_000_000 == 0:
        return Decimal(500_000)
    if value != 0 and value % 1_000 == 0:
        return Decimal(500)
    return Decimal(0)


def reprint_tolerance(value: pl.Expr) -> pl.Expr:
    """Half the unit ``value`` looks rounded to: 500000, 500, or exactly 0."""
    nonzero = value != 0
    return (
        pl.when(nonzero & is_multiple_of(value, 1_000_000))
        .then(pl.lit(Decimal(500_000)))
        .when(nonzero & is_multiple_of(value, 1_000))
        .then(pl.lit(Decimal(500)))
        .otherwise(pl.lit(Decimal(0)))
        .cast(pl.Decimal(38, 10))
    )


def differs_beyond_reprint(original: pl.Expr, reprint: pl.Expr) -> pl.Expr:
    """``reprint`` changed ``original`` by more than its own rounding explains."""
    return (original - reprint).abs() > reprint_tolerance(reprint)


def rounding_unit(value: pl.Expr) -> pl.Expr:
    """1,000,000 when ``value`` is whole millions, else 1,000."""
    return (
        pl.when(is_multiple_of(value, 1_000_000))
        .then(pl.lit(Decimal(1_000_000)))
        .otherwise(pl.lit(Decimal(1_000)))
        .cast(pl.Decimal(38, 10))
    )


def rounds_to(value: pl.Expr, target: pl.Expr) -> pl.Expr:
    """``value`` equals ``target``, or is a finer print that rounds to it.

    ``target`` must look rounded (whole thousands) and the two must be within
    0.1%, so an unrelated value that happens to round the same way does not.
    """
    unit = rounding_unit(target)
    return (value == target) | (
        (target != 0)
        & is_multiple_of(target, 1_000)
        & (round_half_away(value / unit) * unit == target)
        & ((value - target).abs() <= target.abs() * REPRINT_SHARE)
    )
