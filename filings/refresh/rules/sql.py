"""Postgres semantics the stages depend on, expressed in polars.

Three things differ between SQL and polars by default and change results:

* Null ordering. Postgres sorts ``ASC NULLS LAST`` and ``DESC NULLS FIRST``.
* String ordering. Postgres uses the database collation (``en_US.UTF-8``,
  libc), breaking ties by byte order. ``locale.strxfrm`` on the same machine
  reproduces it exactly.
* Rounding. ``round(numeric)`` rounds half away from zero; polars rounds half
  to even.

Every sort that decides which row wins goes through :func:`sort` or
:func:`distinct_on`, so these rules hold everywhere.
"""

from __future__ import annotations

import locale
from dataclasses import dataclass
from decimal import Decimal
from functools import lru_cache
from typing import Optional, Sequence, Union

import polars as pl

IntoExpr = Union[pl.Expr, str]

DATABASE_COLLATION = "en_US.UTF-8"

try:
    locale.setlocale(locale.LC_COLLATE, DATABASE_COLLATION)
except locale.Error:  # pragma: no cover - depends on the host's locales
    locale.setlocale(locale.LC_COLLATE, "C")


@lru_cache(maxsize=None)
def collation_key(text: str) -> str:
    """Sort key matching the database's ``ORDER BY text``."""
    return locale.strxfrm(text) + "\x00" + text


def collate(expr: IntoExpr) -> pl.Expr:
    """``expr`` mapped to its collation key; sort on this, not on the string."""
    e = pl.col(expr) if isinstance(expr, str) else expr
    return e.map_elements(collation_key, return_dtype=pl.String, skip_nulls=True)


@dataclass(frozen=True)
class Order:
    """One ``ORDER BY`` term. ``nulls_last=None`` means the Postgres default."""

    expr: IntoExpr
    descending: bool = False
    nulls_last: Optional[bool] = None

    def resolved_nulls_last(self) -> bool:
        if self.nulls_last is not None:
            return self.nulls_last
        return not self.descending


def asc(expr: IntoExpr, nulls_last: Optional[bool] = None) -> Order:
    return Order(expr, False, nulls_last)


def desc(expr: IntoExpr, nulls_last: Optional[bool] = None) -> Order:
    return Order(expr, True, nulls_last)


OrderLike = Union[Order, IntoExpr]


def _orders(orders: Sequence[OrderLike]) -> list[Order]:
    return [o if isinstance(o, Order) else Order(o) for o in orders]


def sort(frame: pl.DataFrame, orders: Sequence[OrderLike]) -> pl.DataFrame:
    """``ORDER BY`` with Postgres null placement. Stable, like a final ``id`` tie-break."""
    terms = _orders(orders)
    if not terms:
        return frame
    return frame.sort(
        by=[pl.col(t.expr) if isinstance(t.expr, str) else t.expr for t in terms],
        descending=[t.descending for t in terms],
        nulls_last=[t.resolved_nulls_last() for t in terms],
        maintain_order=True,
    )


def distinct_on(
    frame: pl.DataFrame, keys: Sequence[str], orders: Sequence[OrderLike] = ()
) -> pl.DataFrame:
    """``SELECT DISTINCT ON (keys) ... ORDER BY keys, orders``: the first row per key."""
    ordered = sort(frame, list(orders))
    return ordered.unique(subset=list(keys), keep="first", maintain_order=True)


def row_number(
    frame: pl.DataFrame,
    partition: Sequence[str],
    orders: Sequence[OrderLike],
    name: str = "row_number",
) -> pl.DataFrame:
    """``ROW_NUMBER() OVER (PARTITION BY partition ORDER BY orders)``."""
    ordered = sort(frame, list(orders))
    number = pl.int_range(1, pl.len() + 1, dtype=pl.Int64)
    if partition:
        number = number.over(list(partition))
    return ordered.with_columns(number.alias(name))


def first_value(expr: IntoExpr, partition: Sequence[str]) -> pl.Expr:
    """``FIRST_VALUE(expr) OVER (PARTITION BY partition)`` on a frame already sorted."""
    e = pl.col(expr) if isinstance(expr, str) else expr
    return e.first().over(list(partition)) if partition else e.first()


def round_half_away(expr: pl.Expr) -> pl.Expr:
    """Postgres ``round(numeric)``: half away from zero."""
    return (expr.abs() + Decimal("0.5")).floor() * expr.sign()


def is_multiple_of(expr: pl.Expr, unit: int) -> pl.Expr:
    """``expr % unit = 0`` for decimals, which polars cannot take a remainder of."""
    return (expr / unit).floor() * unit == expr


def decimal(value: Union[int, str, Decimal]) -> pl.Expr:
    """A Decimal(38, 10) literal that mixes with value columns without float casts."""
    return pl.lit(Decimal(value), dtype=pl.Decimal(38, 10))
