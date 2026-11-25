"""Deterministic row ids.

Synthetic, clone and derived rows get ids hashed from their identity, so a
refresh is reproducible and a parent can be referenced before it exists. The
hash is the first eight bytes of BLAKE2b: positive for synthetic rows,
negative for clones, so neither collides with a stored fact's serial id.
"""

from __future__ import annotations

import hashlib
from typing import Iterable, Optional

import polars as pl

_INT63 = (1 << 63) - 1


def positive_hash(text: str) -> int:
    """A stable id in ``[0, 2**63)`` for ``text``."""
    digest = hashlib.blake2b(text.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "little") & _INT63


def clone_id(fact_id: int, override_id: int) -> int:
    """Negative id of the copy a ``clone`` fact override adds beside a fact."""
    return -1 - positive_hash(f"fact-clone|{fact_id}|{override_id}")


def md5_hex(text: str) -> str:
    """Hex MD5 of ``text``: the group ids of the normalization tables."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def join_key(parts: Iterable[Optional[object]]) -> Optional[str]:
    """The parts joined with ``|``; any missing part gives None."""
    values = list(parts)
    if any(p is None for p in values):
        return None
    return "|".join(str(p) for p in values)


def abs_hash_expr(*parts: pl.Expr | str) -> pl.Expr:
    """Polars expression for :func:`positive_hash` of the parts joined with ``|``.

    String arguments are literals; expressions are cast to text. A null part
    makes the id null.
    """
    exprs = [pl.lit(p) if isinstance(p, str) else p.cast(pl.String) for p in parts]
    key = pl.concat_str(exprs, separator="|")
    return key.map_elements(positive_hash, return_dtype=pl.Int64, skip_nulls=True)
