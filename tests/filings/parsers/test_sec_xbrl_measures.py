"""Where segment measures from the notes are stored, and which stay out."""

from datetime import date
from decimal import Decimal
from typing import Any, Optional

from filings.models import FactDimension, FinancialFactCreate, PeriodType
from filings.parsers.sec_xbrl import FilingContext
from filings.parsers.sec_xbrl.measures import SegmentMeasures

SEGMENTS = "us-gaap:StatementBusinessSegmentsAxis"
COUNTRY = "srt:StatementGeographicalAxis"
END = date(2024, 12, 31)


def fact(
    concept: str,
    value: Optional[int],
    axis: Optional[str] = None,
    member: Optional[str] = None,
    **extra: Any,
) -> FinancialFactCreate:
    fields: dict[str, Any] = dict(
        key=f"{concept}|{axis}|{member}",
        company_id=0,
        filing_id=0,
        form_type="",
        concept=concept,
        label=concept.split(":")[1],
        period_end=END,
        is_abstract=False,
        value=None if value is None else Decimal(value),
        unit="usd",
        axis=axis,
        member=member,
        member_label=member,
        statement="Income Statement",
        period=PeriodType.YTD,
        dimensions=(
            [FactDimension(axis=axis, member=member, member_label=member)]
            if axis and member
            else []
        ),
    )
    fields.update(extra)
    return FinancialFactCreate(**fields)


FACE = [
    fact("us-gaap:Revenues", 1000),
    fact("us-gaap:Revenues", 600, SEGMENTS, "x:AmericasMember"),
    fact("us-gaap:Goodwill", 50, statement="Balance Sheet", period=None),
    fact(
        "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
        30,
        statement="Cash Flow Statement",
    ),
]


def added(note: list[FinancialFactCreate]) -> list[tuple]:
    measures = SegmentMeasures(FACE, FilingContext())
    measures.read_note(note)
    return sorted((f.statement, f.concept, f.member) for f in measures.added.values())


def test_a_face_concept_joins_its_face_statement() -> None:
    note = [
        fact("us-gaap:Goodwill", 20, SEGMENTS, "x:AmericasMember", period=None),
        fact(
            "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
            10,
            SEGMENTS,
            "x:AmericasMember",
        ),
    ]
    assert added(note) == [
        ("Balance Sheet", "us-gaap:Goodwill", "x:AmericasMember"),
        (
            "Cash Flow Statement",
            "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
            "x:AmericasMember",
        ),
    ]


def test_segment_expenses_join_the_income_statement_beside_segment_revenue() -> None:
    """ASU 2023-07: the segment note reports revenue and expenses per segment."""
    note = [
        fact("us-gaap:Revenues", 600, SEGMENTS, "x:AmericasMember"),
        fact("us-gaap:SellingAndMarketingExpense", 90, SEGMENTS, "x:AmericasMember"),
    ]
    assert added(note) == [
        ("Income Statement", "us-gaap:SellingAndMarketingExpense", "x:AmericasMember")
    ]


def test_a_roll_forward_without_revenue_in_its_note_stays_out() -> None:
    note = [
        fact("us-gaap:GoodwillAcquiredDuringPeriod", 5, SEGMENTS, "x:AmericasMember")
    ]
    assert added(note) == []


def test_a_balance_on_revenue_members_joins_the_balance_sheet() -> None:
    """Long-lived assets by country, on the members revenue is reported on."""
    face = [*FACE, fact("us-gaap:Revenues", 400, COUNTRY, "country:US")]
    measures = SegmentMeasures(face, FilingContext())
    measures.read_note(
        [
            fact("us-gaap:NoncurrentAssets", 70, COUNTRY, "country:US", period=None),
            fact("us-gaap:NoncurrentAssets", 9, COUNTRY, "country:FR", period=None),
        ]
    )
    assert [(f.statement, f.member) for f in measures.added.values()] == [
        ("Balance Sheet", "country:US")
    ]


def test_percentages_and_further_cuts_stay_out() -> None:
    pct = fact(
        "us-gaap:ConcentrationRiskPercentage1",
        12,
        SEGMENTS,
        "x:AmericasMember",
        unit="pure",
    )
    by_debt = fact("us-gaap:Goodwill", 3, SEGMENTS, "x:AmericasMember", period=None)
    by_debt.dimensions = [
        *by_debt.dimensions,
        FactDimension(
            axis="us-gaap:DebtInstrumentAxis", member="x:Notes", member_label="Notes"
        ),
    ]
    assert added([pct, by_debt]) == []


def test_a_member_the_face_already_reports_is_not_added_again() -> None:
    note = [fact("us-gaap:Revenues", 600, SEGMENTS, "x:AmericasMember")]
    assert added(note) == []
