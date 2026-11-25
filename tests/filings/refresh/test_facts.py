"""Fact overrides, resolution, synthetic subtotals and dedup."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import polars as pl

from filings.refresh.stages import facts_normalized, facts_overridden

from .builders import company, frame

D = Decimal


def filing(id: int, form: str, period: str, filed: str, year: int) -> dict:
    return {
        "id": id,
        "filing_entity_id": 1,
        "registry": "SEC",
        "number": str(id),
        "form_type": form,
        "filing_date": filed,
        "fiscal_period_end": period,
        "fiscal_year": year,
        "fiscal_quarter": 4,
    }


def fact(id: int, filing: int, period_end: str, value: int, **extra: Any) -> dict:
    return {
        "id": id,
        "filing_id": filing,
        "form_type": extra.pop("form_type", "10-K"),
        "concept": extra.pop("concept", "us-gaap:Revenues"),
        "label": extra.pop("label", "Revenue"),
        "value": value,
        "period_end": period_end,
        "period": extra.pop("period", "YTD"),
        **extra,
    }


def normalized(data: Any) -> pl.DataFrame:
    data.put("financial_facts_overridden", facts_overridden.run(data))
    return facts_normalized.run(data)


# --- Fact overrides ---------------------------------------------------------


def test_most_specific_rewrite_wins() -> None:
    data = company(
        financial_facts=[fact(1, 1, "2023-12-31", 10)],
        filings=[filing(1, "10-K", "2023-12-31", "2024-02-01", 2023)],
        financial_facts_overrides=[
            {
                "id": 1,
                "concept": "us-gaap:Revenues",
                "statement": "Income Statement",
                "to_concept": "x:Broad",
                "action": "rewrite",
                "is_global": True,
                "company_id": 0,
            },
            {
                "id": 2,
                "concept": "us-gaap:Revenues",
                "statement": "Income Statement",
                "form_type": "10-K",
                "to_concept": "x:Narrow",
                "action": "rewrite",
            },
        ],
    )
    out = facts_overridden.run(data)
    assert out.select("id", "concept", "fact_override_id").rows() == [
        (1, "x:Narrow", 2)
    ]


def test_clone_keeps_the_fact_and_adds_a_copy_on_an_axis() -> None:
    """Alphabet's undimensioned hedging gains, also placed on the segment axis."""
    data = company(
        financial_facts=[fact(1, 1, "2023-12-31", 10)],
        filings=[filing(1, "10-K", "2023-12-31", "2024-02-01", 2023)],
        financial_facts_overrides=[
            {
                "id": 7,
                "concept": "us-gaap:Revenues",
                "statement": "Income Statement",
                "axis": "",
                "member": "",
                "to_concept": "us-gaap:Revenues",
                "to_axis": "seg:Axis",
                "to_member": "seg:Hedging",
                "to_member_label": "Hedging",
                "action": "clone",
            },
        ],
    )
    rows = normalized(data).sort("id")
    assert rows.select("axis", "member", "value").rows() == [
        ("seg:Axis", "Hedging", D(10)),
        ("", "", D(10)),
    ]
    assert rows["id"][0] < 0


# --- Resolution -------------------------------------------------------------


def test_override_weight_flips_the_sign() -> None:
    data = company(
        financial_facts=[fact(1, 1, "2023-12-31", 10, concept="us-gaap:CostOfRevenue")],
        filings=[filing(1, "10-K", "2023-12-31", "2024-02-01", 2023)],
        concept_normalization_overrides=[
            {
                "company_id": 1,
                "concept": "us-gaap:CostOfRevenue",
                "statement": "Income Statement",
                "normalized_label": "Cost of Revenue",
                "weight": -1,
            },
        ],
    )
    row = normalized(data).row(0, named=True)
    assert (row["normalized_label"], row["value"], row["weight"]) == (
        "Cost of Revenue",
        D(-10),
        D(-1),
    )


def test_missing_parent_becomes_a_synthetic_rollup() -> None:
    """An override hangs two lines under a subtotal the filing never tagged."""
    overrides = [
        {
            "company_id": 1,
            "concept": c,
            "statement": "Balance Sheet",
            "normalized_label": label,
            "parent_concept": parent,
            "weight": 1,
            "is_abstract": False,
        }
        for c, label, parent in [
            ("us-gaap:Goodwill", "Goodwill", "us-gaap:AssetsNoncurrent"),
            ("us-gaap:Intangibles", "Intangibles", "us-gaap:AssetsNoncurrent"),
            ("us-gaap:AssetsNoncurrent", "Non-current Assets", None),
        ]
    ]
    data = company(
        financial_facts=[
            fact(
                1,
                1,
                "2023-12-31",
                30,
                concept="us-gaap:Goodwill",
                statement="Balance Sheet",
                period=None,
            ),
            fact(
                2,
                1,
                "2023-12-31",
                12,
                concept="us-gaap:Intangibles",
                statement="Balance Sheet",
                period=None,
            ),
        ],
        filings=[filing(1, "10-K", "2023-12-31", "2024-02-01", 2023)],
        concept_normalization_overrides=overrides,
    )
    rows = normalized(data)
    subtotal = rows.filter(pl.col("is_synthetic")).row(0, named=True)
    assert (subtotal["normalized_label"], subtotal["value"]) == (
        "Non-current Assets",
        D(42),
    )
    assert set(rows.filter(~pl.col("is_synthetic"))["parent_id"]) == {subtotal["id"]}


# --- Dedup -------------------------------------------------------------------


def winners(rows: pl.DataFrame) -> list[tuple]:
    return sorted(rows.filter(~pl.col("is_duplicate")).select("id", "value").rows())


def test_amendment_beats_the_original() -> None:
    data = company(
        financial_facts=[
            fact(1, 1, "2023-12-31", 10),
            fact(2, 2, "2023-12-31", 11, form_type="10-K/A"),
        ],
        filings=[
            filing(1, "10-K", "2023-12-31", "2024-02-01", 2023),
            filing(2, "10-K/A", "2023-12-31", "2024-05-01", 2023),
        ],
    )
    assert winners(normalized(data)) == [(2, D(11))]


def test_period_own_filing_beats_a_later_reprint() -> None:
    data = company(
        financial_facts=[
            fact(1, 1, "2022-12-31", 10),
            fact(2, 2, "2022-12-31", 12),
        ],
        filings=[
            filing(1, "10-K", "2022-12-31", "2023-02-01", 2022),
            filing(2, "10-K", "2023-12-31", "2024-02-01", 2023),
        ],
    )
    assert winners(normalized(data)) == [(1, D(10))]


def test_later_filing_that_recasts_a_split_hides_all_its_members() -> None:
    """NVIDIA's fiscal 2025 10-K carved Singapore out of earlier years."""
    geo = {"axis": "srt:StatementGeographicalAxis", "concept": "us-gaap:Revenues"}
    data = company(
        financial_facts=[
            fact(1, 1, "2023-12-31", 60, member="x:US", member_label="US", **geo),
            fact(2, 1, "2023-12-31", 40, member="x:Other", member_label="Other", **geo),
            fact(3, 2, "2023-12-31", 60, member="x:US", member_label="US", **geo),
            fact(4, 2, "2023-12-31", 25, member="x:Other", member_label="Other", **geo),
            fact(
                5, 2, "2023-12-31", 15, member="x:SG", member_label="Singapore", **geo
            ),
        ],
        filings=[
            filing(1, "10-K", "2023-12-31", "2024-02-01", 2023),
            filing(2, "10-K", "2024-12-31", "2025-02-01", 2024),
        ],
    )
    assert winners(normalized(data)) == [(1, D(60)), (2, D(40))]


def test_rounded_reprint_does_not_replace_the_original_digits() -> None:
    data = company(
        financial_facts=[
            fact(
                1,
                1,
                "2022-12-31",
                1_234_567,
                member="x:A",
                member_label="A",
                axis="x:Axis",
            ),
            fact(
                2,
                2,
                "2022-12-31",
                1_235_000,
                member="x:A",
                member_label="A",
                axis="x:Axis",
            ),
        ],
        filings=[
            filing(1, "10-K", "2022-12-31", "2023-02-01", 2022),
            filing(2, "10-K", "2023-12-31", "2024-02-01", 2023),
        ],
    )
    assert winners(normalized(data)) == [(1, D(1_234_567))]


def test_frame_builder_fills_defaults() -> None:
    f = frame("financial_facts", [{"id": 1}])
    assert f.row(0, named=True)["axis"] == ""
