"""Moving rows between frames and plain Python records.

A stage can prepare its rows as frames, decide one group at a time in plain
Python, and join the decisions back on a row number. A record class is a
slotted dataclass whose fields are named after the columns it reads.
"""

from __future__ import annotations

from dataclasses import fields
from typing import Any, Callable, Hashable, Iterable, Mapping, Sequence, TypeVar

import polars as pl

T = TypeVar("T")
K = TypeVar("K", bound=Hashable)

#: The row number that carries a decision back to its frame row.
ROW = "row_nr"


def numbered(frame: pl.DataFrame) -> pl.DataFrame:
    """``frame`` with a :data:`ROW` column."""
    return frame.with_row_index(ROW)


def records(
    frame: pl.DataFrame, cls: Callable[..., T], key: Sequence[str] = ()
) -> list[T]:
    """One ``cls`` per row, built from the columns named like its fields.

    With ``key``, the first field receives those columns as one tuple, so
    records can be grouped by series or line without a field per column.
    """
    names = [f.name for f in fields(cls)]  # type: ignore[arg-type]
    if not key:
        return [cls(*values) for values in _rows(frame, names)]
    n = len(key)
    rows = _rows(frame, [*key, *names[1:]])
    return [cls(values[:n], *values[n:]) for values in rows]


def _rows(frame: pl.DataFrame, columns: Sequence[str]) -> Iterable[tuple]:
    """Row tuples; column by column is faster than ``iter_rows``."""
    return zip(*(frame.get_column(c).to_list() for c in columns))


def grouped(items: Iterable[T], key: Callable[[T], K]) -> dict[K, list[T]]:
    """``items`` by ``key``, each group in input order."""
    out: dict[K, list[T]] = {}
    for item in items:
        out.setdefault(key(item), []).append(item)
    return out


def frame_of(rows: Iterable[Sequence[Any]], schema: Mapping[str, Any]) -> pl.DataFrame:
    """A frame from plain tuples, typed even when there are none."""
    return pl.DataFrame(list(rows), schema=dict(schema), orient="row")
