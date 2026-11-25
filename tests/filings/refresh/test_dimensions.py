"""Dimension labels, hierarchy expansion and breakdown inference."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import polars as pl

from filings.refresh.stages import (
    dimension_breakdowns,
    dimension_normalization,
    hierarchy_normalization,
)
from filings.refresh.stages.dimension_breakdowns.flat import _Group, matching_subsets

from .builders import company

D = Decimal
AXIS = "srt:ProductOrServiceAxis"


def filing(id: int, year: int) -> dict:
    return {
        "id": id,
        "filing_entity_id": 1,
        "registry": "SEC",
        "number": str(id),
        "form_type": "10-K",
        "filing_date": f"{year + 1}-02-01",
        "fiscal_period_end": f"{year}-12-31",
        "fiscal_year": year,
        "fiscal_quarter": 4,
    }


# --- Dimension normalization -------------------------------------------------


def member_fact(
    id: int, filing: int, year: int, member: str, label: str, value: int, **extra: Any
) -> dict:
    return {
        "id": id,
        "filing_id": filing,
        "form_type": "10-K",
        "concept": "us-gaap:Revenues",
        "label": "Revenue",
        "value": value,
        "axis": AXIS,
        "member": member,
        "member_label": label,
        "period_end": f"{year}-12-31",
        "period": "YTD",
        **extra,
    }


def test_members_sharing_a_name_take_the_latest_label() -> None:
    data = company(
        financial_facts=[
            member_fact(1, 1, 2022, "x:CloudMember", "Cloud services", 10),
            member_fact(2, 2, 2023, "x:CloudMember", "Cloud", 12),
        ]
    )
    out = dimension_normalization.run(data)
    assert set(out["normalized_member_label"]) == {"Cloud"}
    assert out["group_id"].n_unique() == 1


def test_override_relabels_the_axis_and_member() -> None:
    data = company(
        financial_facts=[member_fact(1, 1, 2023, "uber:RidesMember", "Rides", 10)],
        dimension_normalization_overrides=[
            {
                "id": 1,
                "company_id": 1,
                "axis": AXIS,
                "member": "uber:RidesMember",
                "normalized_axis_label": "Product",
                "normalized_member_label": "Mobility",
            }
        ],
    )
    row = dimension_normalization.run(data).row(0, named=True)
    assert (row["normalized_axis_label"], row["normalized_member_label"]) == (
        "Product",
        "Mobility",
    )
    assert (row["override_priority"], row["override_level"]) == ("company", "member")


def test_renamed_product_member_is_chained() -> None:
    """Rides becomes Mobility: the 2023 comparative equals the 2022 Rides value."""
    product = {
        "company_id": 0,
        "is_global": True,
        "axis": AXIS,
        "normalized_axis_label": "Product",
    }
    data = company(
        financial_facts=[
            member_fact(1, 1, 2022, "x:RidesMember", "Rides", 100),
            member_fact(
                2,
                2,
                2023,
                "x:MobilityMember",
                "Mobility",
                130,
                comparative_value=100,
                comparative_period_end="2022-12-31",
            ),
        ],
        dimension_normalization_overrides=[{"id": 1, **product}],
    )
    out = dimension_normalization.run(data)
    assert set(out["normalized_member_label"]) == {"Mobility"}
    assert out["group_id"].n_unique() == 1


# --- Hierarchy ---------------------------------------------------------------


def test_parent_override_follows_both_concepts_renames() -> None:
    normalized = [
        {
            "concept": c,
            "statement": "Balance Sheet",
            "normalized_label": label,
            "group_id": g,
            "source": "chaining",
            "overridden": False,
        }
        for c, label, g in [
            ("x:OldPayables", "Accounts Payable", "child"),
            ("us-gaap:AccountsPayable", "Accounts Payable", "child"),
            ("x:OldCurrentLiabilities", "Current Liabilities", "parent"),
            ("us-gaap:LiabilitiesCurrent", "Current Liabilities", "parent"),
        ]
    ]
    data = company(
        concept_normalization=normalized,
        concept_normalization_overrides=[
            {
                "company_id": 0,
                "is_global": True,
                "statement": "Balance Sheet",
                "concept": "us-gaap:AccountsPayable",
                "normalized_label": "Accounts Payable",
                "parent_concept": "us-gaap:LiabilitiesCurrent",
                "weight": 1,
            }
        ],
    )
    edges = set(
        hierarchy_normalization.run(data).select("concept", "parent_concept").rows()
    )
    assert edges == {
        (child, parent)
        for child in ("x:OldPayables", "us-gaap:AccountsPayable")
        for parent in ("x:OldCurrentLiabilities", "us-gaap:LiabilitiesCurrent")
    }


def _hierarchy(groups: list[tuple[str, str]], children: list[tuple[str, str]]):
    normalized = [
        {
            "concept": c,
            "statement": "Balance Sheet",
            "normalized_label": g,
            "group_id": g,
            "source": "chaining",
            "overridden": False,
        }
        for c, g in groups
    ]
    overrides = [
        {
            "company_id": 0,
            "is_global": True,
            "statement": "Balance Sheet",
            "concept": child,
            "normalized_label": child,
            "parent_concept": parent,
            "weight": 1,
        }
        for child, parent in children
    ]
    data = company(
        concept_normalization=normalized, concept_normalization_overrides=overrides
    )
    return set(
        hierarchy_normalization.run(data).select("concept", "parent_concept").rows()
    )


def test_every_child_follows_its_parents_rename() -> None:
    edges = _hierarchy(
        [
            ("x:OldCurrentLiabilities", "current"),
            ("us-gaap:LiabilitiesCurrent", "current"),
        ],
        [
            ("us-gaap:AccountsPayableCurrent", "us-gaap:LiabilitiesCurrent"),
            ("us-gaap:AccruedLiabilitiesCurrent", "us-gaap:LiabilitiesCurrent"),
        ],
    )
    for child in (
        "us-gaap:AccountsPayableCurrent",
        "us-gaap:AccruedLiabilitiesCurrent",
    ):
        assert (child, "x:OldCurrentLiabilities") in edges


def test_a_line_never_hangs_under_its_own_group() -> None:
    """A part once chained into its subtotal's group is not its own parent."""
    edges = _hierarchy(
        [
            ("us-gaap:LiabilitiesNoncurrent", "noncurrent"),
            ("us-gaap:OtherLiabilitiesNoncurrent", "noncurrent"),
        ],
        [("us-gaap:OtherLiabilitiesNoncurrent", "us-gaap:LiabilitiesNoncurrent")],
    )
    assert edges == set()


# --- Breakdowns --------------------------------------------------------------

#: Products is a subtotal; Domestic + International is a second split.
REVENUE = {
    2023: {
        "": 1000,
        "Products": 800,
        "iPhone": 530,
        "iPad": 270,
        "Services": 200,
        "Domestic": 610,
        "International": 390,
    },
    2024: {
        "": 1200,
        "Products": 950,
        "iPhone": 640,
        "iPad": 310,
        "Services": 250,
        "Domestic": 730,
        "International": 470,
    },
}


def revenue_rows(years: dict[int, dict[str, int]]) -> list[dict]:
    rows = []
    for year, members in years.items():
        for i, (member, value) in enumerate(members.items()):
            rows.append(
                {
                    "id": year * 100 + i,
                    "filing_id": year,
                    "fiscal_filing_id": year,
                    "form_type": "10-K",
                    "concept": "us-gaap:Revenues",
                    "label": "Revenue",
                    "normalized_label": "Revenue",
                    "value": value,
                    "axis": "Product" if member else "",
                    "member": member,
                    "period_end": f"{year}-12-31",
                    "period": "YTD",
                }
            )
    return rows


def breakdowns(years: dict[int, dict[str, int]]) -> list[tuple]:
    data = company(
        filings=[filing(y, y) for y in years],
        financial_facts_normalized=revenue_rows(years),
    )
    out = dimension_breakdowns.run(data)
    return sorted(out.select("parent_member", "members", "fiscal_years").rows())


def test_infers_subtotal_and_alternative_splits() -> None:
    """Apple: Products = iPhone + iPad, beside Products + Services and a region split."""
    assert breakdowns(REVENUE) == [
        ("", ["Domestic", "International"], [2023, 2024]),
        ("", ["Products", "Services"], [2023, 2024]),
        ("Products", ["iPad", "iPhone"], [2023, 2024]),
    ]


def test_a_set_seen_in_one_year_is_a_coincidence() -> None:
    assert breakdowns({2023: REVENUE[2023]}) == []


def test_coarser_members_may_miss_by_half_a_unit_each() -> None:
    group = _Group(["A", "B"], [D(1_000_000), D(2_000_000)], [1_000_000, 1_000_000])
    assert list(matching_subsets(group, D(3_000_400), 1, "")) == [["A", "B"]]
    assert list(matching_subsets(group, D(3_001_000), 1_000, "")) == [["A", "B"]]
    assert list(matching_subsets(group, D(3_000_400), 1_000_000, "")) == []


def test_parent_never_breaks_down_into_itself() -> None:
    group = _Group(["A", "B", "C"], [D(5), D(5), D(10)], [1, 1, 1])
    found = list(matching_subsets(group, D(10), 1, "C"))
    assert found == [["A", "B"]]


def test_declared_breakdown_replaces_inference_for_its_parent() -> None:
    data = company(
        filings=[filing(y, y) for y in REVENUE],
        financial_facts_normalized=revenue_rows(REVENUE),
        dimension_breakdown_overrides=[
            {
                "id": 1,
                "company_id": 1,
                "normalized_label": "Revenue",
                "axis": "Product",
                "parent_member": "",
                "members": ["Services", "Products"],
            }
        ],
    )
    out = dimension_breakdowns.run(data).filter(pl.col("parent_member") == "")
    assert out.select("members", "source").rows() == [
        (["Products", "Services"], "override")
    ]
