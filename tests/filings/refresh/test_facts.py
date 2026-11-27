"""Fact overrides, resolution, synthetic subtotals and dedup."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import polars as pl

from filings.refresh.rules.facts import with_fact_overrides
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


def test_a_printed_quarter_rolls_up_apart_from_the_year_to_date() -> None:
    """A 10-Q prints each line's quarter beside its year to date.

    Each has its own parent: the quarter's subtotal adds the quarters, the
    year to date's adds the years to date, and a tagged parent is matched
    the same way.
    """
    rnd = "us-gaap:ResearchAndDevelopmentExpense"
    sga = "us-gaap:SellingGeneralAndAdministrativeExpense"
    opex = "us-gaap:OperatingExpenses"
    costs = "us-gaap:CostsAndExpenses"
    overrides = [
        {
            "company_id": 1,
            "concept": concept,
            "statement": "Income Statement",
            "normalized_label": concept.split(":")[1],
            "parent_concept": parent,
            "weight": 1,
            "is_abstract": False,
        }
        for concept, parent in [(rnd, opex), (sga, opex), (opex, costs), (costs, None)]
    ]

    def line(id: int, concept: str, period: str, value: int) -> dict:
        return fact(
            id,
            1,
            "2026-09-30",
            value,
            concept=concept,
            period=period,
            form_type="10-Q",
            statement="Income Statement",
        )

    data = company(
        financial_facts=[
            line(1, rnd, "YTD", 30),
            line(2, rnd, "Q", 10),
            line(3, sga, "YTD", 60),
            line(4, sga, "Q", 20),
            line(5, costs, "YTD", 95),
            line(6, costs, "Q", 32),
        ],
        filings=[filing(1, "10-Q", "2026-09-30", "2026-10-30", 2026)],
        concept_normalization_overrides=overrides,
    )
    rows = normalized(data)
    subtotals = {
        r["period"]: r
        for r in rows.filter(pl.col("is_synthetic")).iter_rows(named=True)
    }
    assert {p: r["value"] for p, r in subtotals.items()} == {"YTD": D(90), "Q": D(30)}
    parents = {
        (r["concept"], r["period"]): r["parent_id"]
        for r in rows.filter(~pl.col("is_synthetic")).iter_rows(named=True)
    }
    assert parents[(rnd, "Q")] == subtotals["Q"]["id"]
    assert parents[(sga, "YTD")] == subtotals["YTD"]["id"]
    assert subtotals["Q"]["parent_id"] == 6
    assert subtotals["YTD"]["parent_id"] == 5


def test_segment_rows_split_their_line_and_stay_out_of_the_tree() -> None:
    """Goodwill and assets by segment do not roll into the subtotals.

    The parent of a line is the parent's undimensioned fact, never one of its
    segment members.
    """
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
            ("us-gaap:AssetsNoncurrent", "Non-current Assets", "us-gaap:Assets"),
            ("us-gaap:Assets", "Total Assets", None),
        ]
    ]
    bs = {"statement": "Balance Sheet", "period": None}
    segment = "us-gaap:StatementBusinessSegmentsAxis"
    data = company(
        financial_facts=[
            fact(1, 1, "2023-12-31", 30, concept="us-gaap:Goodwill", **bs),
            fact(
                2,
                1,
                "2023-12-31",
                20,
                concept="us-gaap:Goodwill",
                axis=segment,
                member="x:A",
                **bs,
            ),
            fact(
                3,
                1,
                "2023-12-31",
                10,
                concept="us-gaap:Goodwill",
                axis=segment,
                member="x:B",
                **bs,
            ),
            fact(4, 1, "2023-12-31", 100, concept="us-gaap:Assets", **bs),
            fact(
                5,
                1,
                "2023-12-31",
                60,
                concept="us-gaap:Assets",
                axis=segment,
                member="x:A",
                **bs,
            ),
        ],
        filings=[filing(1, "10-K", "2023-12-31", "2024-02-01", 2023)],
        concept_normalization_overrides=overrides,
    )
    rows = normalized(data)
    subtotal = rows.filter(pl.col("is_synthetic"))
    assert subtotal.height == 1
    assert subtotal["value"].to_list() == [D(30)]
    assert subtotal["parent_id"].to_list() == [4]
    members = rows.filter(pl.col("axis") != "")
    assert members["parent_id"].null_count() == members.height


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


M = 1_000_000


def _reprinted_quarter(google: int) -> Any:
    """Alphabet's first quarter of 2016 in millions, reprinted with its parts a year later."""
    seg = {"axis": "x:Segments", "concept": "us-gaap:Revenues", "form_type": "10-Q"}

    def member(id: int, filing: int, value: int, name: str) -> dict:
        return fact(
            id,
            filing,
            "2016-03-31",
            value * M,
            member=f"x:{name}",
            member_label=name,
            **seg,
        )

    return company(
        financial_facts=[
            fact(
                1,
                1,
                "2016-03-31",
                20257 * M,
                concept="us-gaap:Revenues",
                form_type="10-Q",
            ),
            member(2, 1, 20091, "Google"),
            member(3, 1, 166, "OtherBets"),
            member(4, 2, google, "Google"),
            member(5, 2, 165, "OtherBets"),
            member(6, 2, 14328, "Properties"),
        ],
        filings=[
            filing(1, "10-Q", "2016-03-31", "2016-05-03", 2016),
            filing(2, "10-Q", "2017-03-31", "2017-04-28", 2017),
        ],
    )


def test_later_filing_that_rerounds_a_member_by_one_unit_extends_it() -> None:
    """One million on figures in millions is a re-rounding, not a recast."""
    rows = normalized(_reprinted_quarter(20092))
    expected = [(1, 20257), (2, 20091), (3, 166), (6, 14328)]
    assert winners(rows) == [(i, D(v * M)) for i, v in expected]


def test_later_filing_that_moves_a_member_by_more_than_a_unit_recasts_it() -> None:
    rows = normalized(_reprinted_quarter(20089))
    expected = [(1, 20257), (2, 20091), (3, 166)]
    assert winners(rows) == [(i, D(v * M)) for i, v in expected]


def _segment_profit(
    restated_na: int, quarter: tuple[list[dict], list[dict]] = ([], [])
) -> Any:
    """Amazon's 2015 segment operating income, as filed and restated in 2016."""
    oi = {"concept": "us-gaap:OperatingIncomeLoss", "label": "Operating income"}
    seg = {"axis": "x:Segments", **oi}

    def member(id: int, filing: int, value: int, name: str) -> dict:
        return fact(
            id,
            filing,
            "2015-12-31",
            value,
            member=f"x:{name}",
            member_label=name,
            **seg,
        )

    return company(
        financial_facts=[
            fact(1, 1, "2015-12-31", 2233, **oi),
            member(2, 1, 2751, "NA"),
            member(3, 1, -91, "Intl"),
            member(4, 1, 1863, "AWS"),
            fact(5, 2, "2016-12-31", 4186, **oi),
            member(6, 2, restated_na, "NA"),
            member(7, 2, -699, "Intl"),
            member(8, 2, 1507, "AWS"),
            *quarter[0],
        ],
        filings=[
            filing(1, "10-K", "2015-12-31", "2016-01-29", 2015),
            filing(2, "10-K", "2016-12-31", "2017-02-10", 2016),
            *quarter[1],
        ],
    )


def _nine_months(reprinted: bool) -> tuple[list[dict], list[dict]]:
    """The year's third-quarter 10-Q, and next year's reprint on the new basis."""
    seg = {
        "axis": "x:Segments",
        "concept": "us-gaap:OperatingIncomeLoss",
        "label": "Operating income",
        "form_type": "10-Q",
    }
    q3 = [
        fact(11, 11, "2015-09-30", 2000, member="x:NA", member_label="NA", **seg),
        fact(12, 11, "2015-09-30", 773, member="x:AWS", member_label="AWS", **seg),
    ]
    filings = [filing(11, "10-Q", "2015-09-30", "2015-10-23", 2015)]
    if reprinted:
        q3 += [
            fact(13, 12, "2015-09-30", 900, member="x:NA", member_label="NA", **seg),
            fact(14, 12, "2015-09-30", 224, member="x:AWS", member_label="AWS", **seg),
        ]
        filings.append(filing(12, "10-Q", "2016-09-30", "2016-10-28", 2016))
    return q3, filings


def test_restated_split_that_adds_up_replaces_one_that_does_not() -> None:
    rows = normalized(_segment_profit(1425))
    restated = [(6, D(1425)), (7, D(-699)), (8, D(1507))]
    assert winners(rows) == [(1, D(2233)), (5, D(4186)), *restated]


def test_restated_year_takes_its_quarters_onto_the_new_basis() -> None:
    rows = normalized(_segment_profit(1425, _nine_months(reprinted=True)))
    shown = {id for id, _ in winners(rows)}
    assert {6, 7, 8, 13, 14} <= shown and not {2, 3, 4, 11, 12} & shown


def test_year_whose_quarters_cannot_follow_stays_as_filed() -> None:
    """A restated 10-K beside quarters on the old basis would skew Q4."""
    rows = normalized(_segment_profit(1425, _nine_months(reprinted=False)))
    shown = {id for id, _ in winners(rows)}
    assert {2, 3, 4, 11, 12} <= shown and not {6, 7, 8} & shown


def test_restated_split_that_misses_the_line_leaves_the_split_as_filed() -> None:
    """Members that add up to another total restated the line itself."""
    rows = normalized(_segment_profit(1500))
    as_filed = [(2, D(2751)), (3, D(-91)), (4, D(1863))]
    assert winners(rows) == [(1, D(2233)), *as_filed, (5, D(4186))]


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


# --- Facts filed under an axis total -----------------------------------------


def _segment_fact(id: int, value: int, member: str, **extra: Any) -> dict:
    return fact(
        id,
        1,
        "2024-12-31",
        value,
        concept="us-gaap:OperatingIncomeLoss",
        label="Operating income",
        axis="srt:ConsolidationItemsAxis",
        member=member,
        member_label=member,
        **extra,
    )


def _dims(fact_id: int, slice_member: str, label: str) -> list[dict]:
    return [
        {
            "fact_id": fact_id,
            "ordinal": 0,
            "axis": "srt:ConsolidationItemsAxis",
            "member": "us-gaap:OperatingSegmentsMember",
            "member_label": "Operating Segments",
            "is_default": True,
        },
        {
            "fact_id": fact_id,
            "ordinal": 1,
            "axis": "us-gaap:StatementBusinessSegmentsAxis",
            "member": slice_member,
            "member_label": label,
            "is_default": False,
        },
    ]


def test_fact_under_an_axis_total_moves_onto_its_slice() -> None:
    """Segment operating income joins the segment's revenue member.

    "Operating Segments - Cloud" is OperatingSegmentsMember beside x:CloudMember;
    the total says nothing, so the fact is the segment's. An elimination of
    the segment is a real slice beside it and stays where it was filed.
    """
    elimination = {
        "fact_id": 3,
        "ordinal": 0,
        "axis": "srt:ConsolidationItemsAxis",
        "member": "us-gaap:IntersegmentEliminationMember",
        "member_label": "Intersegment Elimination",
        "is_default": False,
    }
    data = company(
        financial_facts=[
            _segment_fact(1, 40, "Operating Segments - Cloud"),
            _segment_fact(2, 60, "Operating Segments - Services"),
            _segment_fact(3, -5, "Intersegment Elimination - Cloud"),
        ],
        financial_fact_dimensions=[
            *_dims(1, "x:CloudMember", "Cloud"),
            *_dims(2, "x:ServicesMember", "Services"),
            elimination,
            {**_dims(3, "x:CloudMember", "Cloud")[1], "fact_id": 3},
        ],
        filings=[filing(1, "10-K", "2024-12-31", "2025-02-01", 2024)],
    )
    rows = with_fact_overrides(data, include_clones=False).sort("id")
    assert rows.select("axis", "member", "member_label").rows() == [
        ("us-gaap:StatementBusinessSegmentsAxis", "x:CloudMember", "Cloud"),
        ("us-gaap:StatementBusinessSegmentsAxis", "x:ServicesMember", "Services"),
        (
            "srt:ConsolidationItemsAxis",
            "Intersegment Elimination - Cloud",
            "Intersegment Elimination - Cloud",
        ),
    ]


def test_fact_under_an_axis_total_keeps_its_axis_beside_a_different_twin() -> None:
    """The filing's own single-dimension fact on that slot wins the slot.

    Moving the total's fact onto it would make the slot disagree and drop both.
    """
    twin = fact(
        2,
        1,
        "2024-12-31",
        35,
        concept="us-gaap:OperatingIncomeLoss",
        label="Operating income",
        axis="us-gaap:StatementBusinessSegmentsAxis",
        member="x:CloudMember",
        member_label="Cloud",
    )
    data = company(
        financial_facts=[_segment_fact(1, 40, "Operating Segments - Cloud"), twin],
        financial_fact_dimensions=_dims(1, "x:CloudMember", "Cloud"),
        filings=[filing(1, "10-K", "2024-12-31", "2025-02-01", 2024)],
    )
    rows = with_fact_overrides(data, include_clones=False).sort("id")
    assert rows["axis"].to_list() == [
        "srt:ConsolidationItemsAxis",
        "us-gaap:StatementBusinessSegmentsAxis",
    ]


def test_finer_cut_beside_its_own_slot_is_dropped() -> None:
    """Microsoft's Corporate and Other, and its impairment part, on one member."""
    corporate = {
        "concept": "us-gaap:OperatingIncomeLoss",
        "label": "Operating income",
        "axis": "us-gaap:ConsolidationItemsAxis",
        "member": "msft:CorporateAndReconcilingItemsMember",
        "member_label": "Corporate and Other",
    }

    def coordinate(fact_id: int, ordinal: int, axis: str, member: str) -> dict:
        return {
            "fact_id": fact_id,
            "ordinal": ordinal,
            "axis": axis,
            "member": member,
            "member_label": member,
            "is_default": False,
        }

    data = company(
        financial_facts=[
            fact(1, 1, "2015-09-30", -1274, **corporate),
            fact(2, 1, "2015-09-30", 0, **corporate),
        ],
        financial_fact_dimensions=[
            coordinate(1, 0, corporate["axis"], corporate["member"]),
            coordinate(2, 0, corporate["axis"], corporate["member"]),
            coordinate(
                2, 1, "us-gaap:IncomeStatementLocationAxis", "msft:ImpairmentMember"
            ),
        ],
        filings=[filing(1, "10-Q", "2015-09-30", "2015-10-22", 2016)],
    )
    rows = with_fact_overrides(data, include_clones=False)
    assert rows["id"].to_list() == [1]


def test_restated_split_with_a_subtotal_replaces_one_that_does_not_add_up() -> None:
    """Alphabet's FY2014 10-K left other revenues untagged; its FY2015 10-K
    restated 2014 as Google (with its parts) beside Other Bets."""
    rev = {"concept": "us-gaap:Revenues", "label": "Revenues"}
    product = {"axis": "x:Product", **rev}

    def member(id: int, filing: int, value: int, name: str) -> dict:
        return fact(
            id,
            filing,
            "2014-12-31",
            value,
            member=f"x:{name}",
            member_label=name,
            **product,
        )

    data = company(
        financial_facts=[
            fact(1, 1, "2014-12-31", 66001, **rev),
            member(2, 1, 45085, "Properties"),
            member(3, 1, 13971, "Network"),
            fact(4, 2, "2015-12-31", 74989, **rev),
            member(5, 2, 65674, "Google"),
            member(6, 2, 45085, "Properties"),
            member(7, 2, 14539, "Network"),
            member(8, 2, 6050, "Other"),
            member(9, 2, 327, "OtherBets"),
        ],
        filings=[
            filing(1, "10-K", "2014-12-31", "2015-02-06", 2014),
            filing(2, "10-K", "2015-12-31", "2016-02-11", 2015),
        ],
    )
    shown = {id for id, _ in winners(normalized(data))}
    assert {5, 6, 7, 8, 9} <= shown and not {2, 3} & shown


def test_child_of_a_dropped_twin_points_at_the_kept_one() -> None:
    """A line printed twice in one statement keeps its children (SoFi's VIE block)."""
    from filings.refresh.stages.facts_normalized.dedup import (
        with_parents_on_kept_twins,
    )

    base = {
        "filing_id": 1,
        "statement": "Balance Sheet",
        "axis": "",
        "member": "",
        "period_end": "2022-12-31",
        "period": "YTD",
        "value": 5,
        "abstract_id": None,
    }
    rows = pl.DataFrame(
        [
            {
                **base,
                "id": 10,
                "concept": "us-gaap:Assets",
                "parent_id": None,
                "is_duplicate": False,
            },
            {
                **base,
                "id": 20,
                "concept": "us-gaap:Assets",
                "parent_id": None,
                "is_duplicate": True,
            },
            {
                **base,
                "id": 30,
                "concept": "us-gaap:Cash",
                "parent_id": 20,
                "is_duplicate": False,
            },
            {
                **base,
                "id": 40,
                "concept": "us-gaap:Goodwill",
                "parent_id": 10,
                "is_duplicate": False,
            },
            # The kept twin of row 50's parent is row 50 itself: it keeps its pointer.
            {
                **base,
                "id": 50,
                "concept": "us-gaap:Revenues",
                "parent_id": 60,
                "is_duplicate": False,
            },
            {
                **base,
                "id": 60,
                "concept": "us-gaap:Revenues",
                "parent_id": None,
                "is_duplicate": True,
            },
            # Same concept at another value is not a twin.
            {
                **base,
                "id": 70,
                "concept": "us-gaap:Debt",
                "parent_id": None,
                "is_duplicate": False,
            },
            {
                **base,
                "id": 80,
                "concept": "us-gaap:Debt",
                "value": 9,
                "parent_id": None,
                "is_duplicate": True,
            },
            {
                **base,
                "id": 90,
                "concept": "us-gaap:Loans",
                "parent_id": 80,
                "is_duplicate": False,
            },
        ],
        schema_overrides={"parent_id": pl.Int64, "abstract_id": pl.Int64},
    )
    out = with_parents_on_kept_twins(rows).sort("id")
    assert out["parent_id"].to_list() == [
        None,
        None,
        10,
        10,
        60,
        None,
        None,
        None,
        80,
    ]
    assert out.columns == rows.columns
