"""How rows are ordered when an order decides which row wins.

Every sort that picks a winner goes through :func:`sort`, :func:`distinct_on`
or :func:`row_number`, so two rules hold everywhere:

* Nulls sort last ascending and first descending, unless a term says
  otherwise.
* Text sorts by :func:`text_key`: ignoring case first, so "Data and AI" and
  "Data And AI" sit together, then lowercase before uppercase. The key is
  plain Python, so the order is the same on every machine.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Sequence, Union

import polars as pl

IntoExpr = Union[pl.Expr, str]


@lru_cache(maxsize=None)
def text_key(text: str) -> str:
    """Sort key for text: case-insensitive, then lowercase before uppercase."""
    return text.casefold() + "\x00" + text.swapcase()


def by_text(expr: IntoExpr) -> pl.Expr:
    """``expr`` mapped to its :func:`text_key`; sort on this, not on the string."""
    e = pl.col(expr) if isinstance(expr, str) else expr
    return e.map_elements(text_key, return_dtype=pl.String, skip_nulls=True)


@dataclass(frozen=True)
class Order:
    """One sort term. ``nulls_last=None``: last ascending, first descending."""

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
    """Sort by ``orders`` with the null placement above. Stable."""
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
    """The first row per ``keys`` in ``orders`` order."""
    ordered = sort(frame, list(orders))
    return ordered.unique(subset=list(keys), keep="first", maintain_order=True)


def row_number(
    frame: pl.DataFrame,
    partition: Sequence[str],
    orders: Sequence[OrderLike],
    name: str = "row_number",
) -> pl.DataFrame:
    """``name``: each row's 1-based place in ``orders`` within its partition."""
    ordered = sort(frame, list(orders))
    number = pl.int_range(1, pl.len() + 1, dtype=pl.Int64)
    if partition:
        number = number.over(list(partition))
    return ordered.with_columns(number.alias(name))


def first_value(expr: IntoExpr, partition: Sequence[str]) -> pl.Expr:
    """The first ``expr`` per partition, on a frame already sorted."""
    e = pl.col(expr) if isinstance(expr, str) else expr
    return e.first().over(list(partition)) if partition else e.first()
