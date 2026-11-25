"""The SQL-compatibility rules every stage relies on."""

from __future__ import annotations

from decimal import Decimal

import polars as pl
import pytest

from filings.refresh.rules import sql
from filings.refresh.rules.graph import components
from filings.refresh.rules.ids import clone_id, hashtextextended, md5_hex
from filings.refresh.rules.rounding import reprint_tolerance_of, rounds_to
from filings.refresh.rules.units import canonical_unit

D = Decimal


@pytest.mark.parametrize(
    "text, expected",
    [
        # SELECT hashtextextended(text, 0) on the project database.
        ("", -6939563903564495251),
        ("abc", -6747756470228489321),
        ("Balance Sheet|us-gaap:AssetsNoncurrent|42|1", 2500098253329028445),
        ("€uro ünïcode ✓", -2060781594144540993),
    ],
)
def test_hash_matches_postgres(text: str, expected: int) -> None:
    assert hashtextextended(text) == expected


def test_clone_ids_are_negative_and_distinct() -> None:
    assert clone_id(1, 2) < 0
    assert clone_id(1, 2) != clone_id(1, 3)


def test_md5_matches_postgres() -> None:
    # SELECT md5('a|b')
    assert md5_hex("a|b") == "d0726241020676b14aa6298ce6a18b21"


def test_sort_puts_nulls_where_postgres_does() -> None:
    df = pl.DataFrame({"x": [2, None, 1]})
    assert sql.sort(df, [sql.asc("x")])["x"].to_list() == [1, 2, None]
    assert sql.sort(df, [sql.desc("x")])["x"].to_list() == [None, 2, 1]
    assert sql.sort(df, [sql.desc("x", nulls_last=True)])["x"].to_list() == [2, 1, None]


def test_collation_ignores_case_and_punctuation_first() -> None:
    words = ["b", "B", "a", "A", "_z", "Z", "ab", "a b"]
    ordered = sorted(words, key=sql.collation_key)
    assert ordered.index("a") < ordered.index("B") < ordered.index("Z")


def test_distinct_on_keeps_first_row_per_key() -> None:
    df = pl.DataFrame({"k": [1, 1, 2], "v": [10, 20, 30]})
    kept = sql.distinct_on(df, ["k"], [sql.desc("v")])
    assert kept.sort("k")["v"].to_list() == [20, 30]


def test_round_half_away_from_zero() -> None:
    s = pl.Series("v", [D("2.5"), D("-2.5"), D("1.4")], dtype=pl.Decimal(38, 10))
    rounded = pl.select(sql.round_half_away(pl.lit(s))).to_series().to_list()
    assert rounded == [D(3), D(-3), D(1)]


def test_is_multiple_of_on_decimals() -> None:
    s = pl.Series(
        "v", [D(3_000_000), D(3_500), D("1000.5"), D(-2_000)], dtype=pl.Decimal(38, 10)
    )
    out = pl.select(sql.is_multiple_of(pl.lit(s), 1_000)).to_series().to_list()
    assert out == [True, False, False, True]


def test_sum_and_bool_and_are_null_over_nothing() -> None:
    df = pl.DataFrame(
        {"g": [1, 1], "v": [None, None]}, schema={"g": pl.Int64, "v": pl.Int64}
    )
    out = df.group_by("g").agg(
        sql.sum_or_null(pl.col("v")).alias("s"),
        sql.bool_and(pl.col("v") > 0).alias("b"),
    )
    assert out.row(0) == (1, None, None)


def test_rounded_reprint_counts_as_the_same_value() -> None:
    df = pl.DataFrame(
        {"value": [D(1_234_567), D(1_299_999)], "winner": [D(1_235_000), D(1_235_000)]},
        schema={"value": pl.Decimal(38, 10), "winner": pl.Decimal(38, 10)},
    )
    out = df.select(rounds_to(pl.col("value"), pl.col("winner"))).to_series().to_list()
    assert out == [True, False]


def test_reprint_tolerance_follows_rounding() -> None:
    assert reprint_tolerance_of(D(5_000_000)) == 500_000
    assert reprint_tolerance_of(D(5_000)) == 500
    assert reprint_tolerance_of(D(5_001)) == 0
    assert reprint_tolerance_of(D(0)) == 0


@pytest.mark.parametrize(
    "stored, canonical",
    [
        ("USD", "usd"),
        (" iso4217_EUR ", "eur"),
        ("U_JPY", "jpy"),
        ("usdPerShare", "usdPerShare"),
        ("USDEPS", "usdPerShare"),
        ("shares", "shares"),
        ("", None),
        (None, None),
    ],
)
def test_canonical_unit(stored: str | None, canonical: str | None) -> None:
    out = pl.select(canonical_unit(pl.lit(stored, dtype=pl.String))).item()
    assert out == canonical


def test_components_name_each_part_by_its_smallest_node() -> None:
    mapping = components(["c", "a", "b", "d"], [("c", "a"), ("b", "c")])
    assert mapping == {"a": "a", "b": "a", "c": "a", "d": "d"}
