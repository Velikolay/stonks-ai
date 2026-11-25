"""Unit canonicalisation.

A fact's unit ref is often an ISO currency wrapped in a filing-specific name
(``iso4217_USD``, ``U_USD``, ``usdPerShare``). The currency code is the unit,
lower-cased; per-share amounts keep a ``PerShare`` suffix; anything else is
left as stored.
"""

from __future__ import annotations

import polars as pl

_CURRENCY = r"^[a-z]{3}$"


def canonical_unit(unit: pl.Expr) -> pl.Expr:
    """The canonical spelling of a stored unit ref."""
    trimmed = unit.str.strip_chars(" ")
    low = trimmed.str.to_lowercase()
    last3 = low.str.extract(r"([a-z]{3})$")
    return (
        pl.when(unit.is_null() | (trimmed == ""))
        .then(pl.lit(None, pl.String))
        .when(low.str.contains(_CURRENCY))
        .then(low)
        .when(low.str.contains(r"^[a-z]{3}pershare$"))
        .then(low.str.slice(0, 3) + "PerShare")
        .when(low == "usdeps")
        .then(pl.lit("usdPerShare"))
        .when(low.str.contains(r"(iso4217_?|unit_)[a-z]{3}$"))
        .then(last3)
        .when(low.str.contains(r"^u_iso4217[a-z]{3}$"))
        .then(last3)
        .when(low.str.contains(r"^u_[a-z]{3}$"))
        .then(last3)
        .otherwise(trimmed)
    )


def is_currency(unit: pl.Expr) -> pl.Expr:
    """A bare currency code such as ``usd`` (case-insensitive)."""
    return unit.str.contains(r"(?i)^[a-z]{3}$")


def is_per_share_concept(concept: pl.Expr) -> pl.Expr:
    """``EarningsPerShare*`` or ``*PerShare`` after the namespace prefix."""
    local = concept.str.split(":").list.get(1, null_on_oob=True).fill_null("")
    return local.str.contains(r"(?i)(^EarningsPerShare|PerShare$)")


def filing_currency(facts: pl.DataFrame) -> pl.DataFrame:
    """The one currency a filing reports in, when it reports in exactly one."""
    currency = facts.filter(is_currency(pl.col("unit"))).with_columns(
        pl.col("unit").str.to_lowercase()
    )
    per_filing = currency.group_by("filing_id").agg(
        pl.col("unit").min().alias("currency"), pl.col("unit").n_unique().alias("n")
    )
    return per_filing.filter(pl.col("n") == 1).select("filing_id", "currency")
