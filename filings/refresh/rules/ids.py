"""Deterministic row ids.

Synthetic, clone and derived rows get ids hashed from their identity, so a
refresh is reproducible and a parent can be referenced before it exists. The
hash is Postgres' ``hashtextextended(text, 0)``, which the SQL refresh used;
keeping it leaves every stored id unchanged. Any stable 64-bit hash would do.
"""

from __future__ import annotations

import hashlib
from typing import Iterable, Optional

import polars as pl

_MASK32 = 0xFFFFFFFF
_INT64_SPAN = 1 << 64


def _rot(x: int, k: int) -> int:
    return ((x << k) | (x >> (32 - k))) & _MASK32


def _mix(a: int, b: int, c: int) -> tuple[int, int, int]:
    a = (a - c) & _MASK32
    a ^= _rot(c, 4)
    c = (c + b) & _MASK32
    b = (b - a) & _MASK32
    b ^= _rot(a, 6)
    a = (a + c) & _MASK32
    c = (c - b) & _MASK32
    c ^= _rot(b, 8)
    b = (b + a) & _MASK32
    a = (a - c) & _MASK32
    a ^= _rot(c, 16)
    c = (c + b) & _MASK32
    b = (b - a) & _MASK32
    b ^= _rot(a, 19)
    a = (a + c) & _MASK32
    c = (c - b) & _MASK32
    c ^= _rot(b, 4)
    b = (b + a) & _MASK32
    return a, b, c


def _final(a: int, b: int, c: int) -> tuple[int, int, int]:
    c ^= b
    c = (c - _rot(b, 14)) & _MASK32
    a ^= c
    a = (a - _rot(c, 11)) & _MASK32
    b ^= a
    b = (b - _rot(a, 25)) & _MASK32
    c ^= b
    c = (c - _rot(b, 16)) & _MASK32
    a ^= c
    a = (a - _rot(c, 4)) & _MASK32
    b ^= a
    b = (b - _rot(a, 14)) & _MASK32
    c ^= b
    c = (c - _rot(b, 24)) & _MASK32
    return a, b, c


def hashtextextended(text: str, seed: int = 0) -> int:
    """Postgres ``hashtextextended(text, seed)`` as a signed 64-bit integer.

    Bob Jenkins' lookup3 over the UTF-8 bytes, as in ``hash_bytes_extended``
    in ``src/common/hashfn.c`` on a little-endian server.
    """
    k = text.encode("utf-8")
    length = len(k)
    a = b = c = (0x9E3779B9 + length + 3923095) & _MASK32
    if seed != 0:
        a = (a + ((seed >> 32) & _MASK32)) & _MASK32
        b = (b + (seed & _MASK32)) & _MASK32
        a, b, c = _mix(a, b, c)

    pos = 0
    remaining = length
    while remaining >= 12:
        a = (a + int.from_bytes(k[pos : pos + 4], "little")) & _MASK32
        b = (b + int.from_bytes(k[pos + 4 : pos + 8], "little")) & _MASK32
        c = (c + int.from_bytes(k[pos + 8 : pos + 12], "little")) & _MASK32
        a, b, c = _mix(a, b, c)
        pos += 12
        remaining -= 12

    tail = k[pos:]
    # The lowest byte of c is reserved for the length, so c takes bytes 8-10
    # shifted up by one byte.
    if remaining >= 9:
        c = (c + (int.from_bytes(tail[8:11], "little") << 8)) & _MASK32
    if remaining >= 5:
        b = (b + int.from_bytes(tail[4:8], "little")) & _MASK32
    if remaining >= 1:
        a = (a + int.from_bytes(tail[0:4], "little")) & _MASK32

    a, b, c = _final(a, b, c)
    unsigned = (b << 32) | c
    return unsigned - _INT64_SPAN if unsigned >= 1 << 63 else unsigned


def abs_hash(text: str) -> int:
    """``abs(hashtextextended(text, 0))``, the positive synthetic-row id."""
    return abs(hashtextextended(text))


def clone_id(fact_id: int, override_id: int) -> int:
    """Negative id of the copy a ``clone`` fact override adds beside a fact."""
    h = hashtextextended(f"fact-clone|{fact_id}|{override_id}")
    return h if h < 0 else -1 - h


def md5_hex(text: str) -> str:
    """Postgres ``md5(text)``: the group ids of the normalization tables."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def join_key(parts: Iterable[Optional[object]]) -> Optional[str]:
    """``a || '|' || b ...`` with Postgres semantics: any NULL part gives NULL."""
    values = list(parts)
    if any(p is None for p in values):
        return None
    return "|".join(str(p) for p in values)


def abs_hash_expr(*parts: pl.Expr | str) -> pl.Expr:
    """Polars expression for ``abs(hashtextextended(p1 || '|' || p2 ...))``.

    String arguments are literals; expressions are cast to text. A NULL part
    makes the id NULL, as concatenation does in SQL.
    """
    exprs = [pl.lit(p) if isinstance(p, str) else p.cast(pl.String) for p in parts]
    key = pl.concat_str(exprs, separator="|")
    return key.map_elements(abs_hash, return_dtype=pl.Int64, skip_nulls=True)
