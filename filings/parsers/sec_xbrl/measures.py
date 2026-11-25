"""Segment measures beyond revenue, from any note that reports them.

A segment amount is stored as a dimension row of the statement that holds
its concept, so it joins that line's series the way segment revenue joins
the face revenue line:

* A concept on a face statement goes to that statement: goodwill and total
  assets by segment to the balance sheet, depreciation by segment to the cash
  flow statement. Income statement first when a concept is on several.
* A concept on no face statement goes by its period. A balance goes to the
  balance sheet when its members are ones the filing reports revenue on
  (long-lived assets by country). An amount for a period goes to the income
  statement when the same note reports revenue on those members: the
  segment expenses ASU 2023-07 requires, and segment capital spending.

Either way the fact is a currency amount whose every dimension is a segment
axis or that axis's total; a percentage, a count, or a segment cut further by
debt instrument or plan stays out. A goodwill roll-forward by segment stays
out because its note reports no revenue. Revenue is left to
:mod:`.segments`, which rewrites note revenue onto the face line.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Optional

from ...models import FinancialFactCreate
from .concepts import SEGMENT_AXES, is_monetary, is_revenue_concept
from .context import FilingContext
from .facts import rekey
from .segments import Note

logger = logging.getLogger(__name__)

#: Face statements, in the order a concept on several is placed.
FACE_STATEMENTS = (
    "Income Statement",
    "Balance Sheet",
    "Cash Flow Statement",
    "Comprehensive Income",
    "Statement of Equity",
)

#: One member of one line and period on one statement.
Slot = tuple


def _slot(fact: FinancialFactCreate, statement: str) -> Slot:
    return (
        statement,
        fact.concept,
        fact.axis,
        fact.member,
        fact.period_end,
        fact.period,
    )


class SegmentMeasures:
    """Segment measures the face statements and the revenue rules did not keep."""

    def __init__(self, facts: list[FinancialFactCreate], context: FilingContext):
        self.context = context
        #: Concept to the face statement and label that hold it.
        self.home: dict[str, tuple[str, str]] = {}
        for statement in FACE_STATEMENTS:
            for f in facts:
                if f.statement == statement and not f.axis and not f.is_abstract:
                    self.home.setdefault(f.concept, (statement, f.label))
        self.captured = {_slot(f, f.statement or "") for f in facts if f.axis}
        self.revenue_members = {
            (f.axis, f.member)
            for f in facts
            if f.axis and is_revenue_concept(f.concept)
        }
        self.added: dict[Slot, FinancialFactCreate] = {}

    def collect(self, notes: list[Note]) -> list[FinancialFactCreate]:
        for note in notes:
            if note.has_segment_axis:
                self.read_note(note.facts(self.context))
        if self.added:
            logger.info("Added %s segment measures from the notes", len(self.added))
        return list(self.added.values())

    def read_note(self, parsed: list[FinancialFactCreate]) -> None:
        note_revenue = {
            (f.axis, f.member)
            for f in parsed
            if f.axis and is_revenue_concept(f.concept)
        }
        for fact in parsed:
            if not self.is_segment_amount(fact):
                continue
            placed = self.placement(fact, note_revenue)
            if placed is None:
                continue
            statement, label = placed
            slot = _slot(fact, statement)
            if slot in self.captured or slot in self.added:
                continue
            fact.label = label
            rekey(fact, statement)
            self.added[slot] = fact

    def is_segment_amount(self, fact: FinancialFactCreate) -> bool:
        """A currency amount cut only by segment axes."""
        if fact.is_abstract or not isinstance(fact.value, Decimal):
            return False
        if fact.axis not in SEGMENT_AXES or is_revenue_concept(fact.concept):
            return False
        if not is_monetary(fact.unit):
            return False
        return all(
            d.axis in SEGMENT_AXES or self.context.is_axis_total(d.member)
            for d in fact.dimensions or []
        )

    def placement(
        self, fact: FinancialFactCreate, note_revenue: set[tuple]
    ) -> Optional[tuple[str, str]]:
        """The statement and label the fact is stored under, or None to leave it out."""
        if fact.concept in self.home:
            return self.home[fact.concept]
        member = (fact.axis, fact.member)
        if fact.period is None:
            if member in self.revenue_members:
                return "Balance Sheet", fact.label
            return None
        if member in note_revenue:
            return "Income Statement", fact.label
        return None


def segment_measures(
    notes: list[Note], facts: list[FinancialFactCreate], context: FilingContext
) -> list[FinancialFactCreate]:
    """Segment measures from the notes to add beside the face statements."""
    return SegmentMeasures(facts, context).collect(notes)
