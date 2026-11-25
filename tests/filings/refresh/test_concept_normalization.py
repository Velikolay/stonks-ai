"""Label inference: grouping, chaining, and overrides inherited by a group."""

from __future__ import annotations

from typing import Any

import polars as pl

from filings.refresh.stages import concept_normalization

from .builders import company

STATEMENT = "Income Statement"


def fact(
    id: int, filing: int, concept: str, label: str, year: int, **extra: Any
) -> dict:
    return {
        "id": id,
        "filing_id": filing,
        "form_type": "10-K",
        "concept": concept,
        "label": label,
        "value": extra.pop("value", 100 + id),
        "period_end": f"{year}-12-31",
        "period": "YTD",
        **extra,
    }


def labels(rows: Any) -> dict[str, tuple[str, str]]:
    return {
        r["concept"]: (r["normalized_label"], r["source"])
        for r in rows.iter_rows(named=True)
    }


def test_grouping_takes_the_latest_label() -> None:
    data = company(
        financial_facts=[
            fact(1, 1, "us-gaap:Revenues", "Revenue", 2022),
            fact(2, 2, "us-gaap:Revenues", "Total revenue", 2023),
        ]
    )
    assert labels(concept_normalization.run(data)) == {
        "us-gaap:Revenues": ("Total revenue", "grouping")
    }


def test_two_labels_in_one_filing_disable_grouping() -> None:
    """A statement-of-equity line (beginning and ending balance) keeps its labels."""
    data = company(
        financial_facts=[
            fact(1, 1, "us-gaap:StockholdersEquity", "Beginning balance", 2022),
            fact(2, 1, "us-gaap:StockholdersEquity", "Ending balance", 2022, value=5),
            fact(3, 2, "us-gaap:StockholdersEquity", "Ending balance", 2023, value=6),
        ]
    )
    assert concept_normalization.run(data).is_empty()


def test_chaining_links_a_renamed_concept() -> None:
    """A 2023 comparative equal to the old concept's 2022 value is the same line."""
    data = company(
        financial_facts=[
            fact(1, 1, "us-gaap:SalesRevenueNet", "Net sales", 2022, value=100),
            fact(
                2,
                2,
                "us-gaap:Revenues",
                "Revenue",
                2023,
                value=120,
                comparative_value=100,
                comparative_period_end="2022-12-31",
            ),
        ]
    )
    out = concept_normalization.run(data)
    assert labels(out) == {
        "us-gaap:SalesRevenueNet": ("Revenue", "chaining"),
        "us-gaap:Revenues": ("Revenue", "chaining"),
    }
    assert out["group_id"].n_unique() == 1


def test_zero_never_bridges() -> None:
    """Every line that did not occur that year shares zero."""
    data = company(
        financial_facts=[
            fact(1, 1, "us-gaap:A", "A", 2022, value=0),
            fact(
                2,
                2,
                "us-gaap:B",
                "B",
                2023,
                value=5,
                comparative_value=0,
                comparative_period_end="2022-12-31",
            ),
        ]
    )
    assert concept_normalization.run(data).is_empty()


def test_concepts_that_coexist_are_not_a_rename() -> None:
    data = company(
        financial_facts=[
            fact(1, 1, "us-gaap:A", "A", 2022, value=100),
            fact(2, 1, "us-gaap:B", "B", 2022, value=7),
            fact(
                3,
                2,
                "us-gaap:B",
                "B",
                2023,
                value=9,
                comparative_value=100,
                comparative_period_end="2022-12-31",
            ),
        ]
    )
    assert (
        concept_normalization.run(data)
        .filter(pl.col("source") == "chaining")
        .is_empty()
    )


def test_group_inherits_an_override_its_members_agree_on() -> None:
    data = company(
        financial_facts=[
            fact(1, 1, "us-gaap:SalesRevenueNet", "Net sales", 2022, value=100),
            fact(
                2,
                2,
                "us-gaap:Revenues",
                "Revenue",
                2023,
                value=120,
                comparative_value=100,
                comparative_period_end="2022-12-31",
            ),
        ],
        concept_normalization_overrides=[
            {
                "company_id": 0,
                "is_global": True,
                "concept": "us-gaap:Revenues",
                "statement": STATEMENT,
                "normalized_label": "Revenue (total)",
            }
        ],
    )
    out = concept_normalization.run(data)
    assert set(out["normalized_label"]) == {"Revenue (total)"}
    assert out["overridden"].all()
