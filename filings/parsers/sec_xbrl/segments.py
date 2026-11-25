"""Segment members a filing tags only in a note, added beside the face statement.

Some filings tag market, geography and reportable-segment amounts only in a
note. The face income statement then has the total and none of the members,
which leaves a one-year hole in every series. These rules decide which note
rows join the income statement:

* A row on a segment axis is kept when it reuses a face concept or label, or
  tags the revenue line with another revenue concept than the face. That
  revenue row is rewritten onto the face line so it joins that series.
* A company extension axis is kept on the same terms when its revenue members
  for a period add up to the face revenue for that period.
* A member the face already reports is not added again. Other sales tagged on
  a segment member that already carries the total is that segment's residual,
  stored as its own member.
* Undimensioned advertising and other-revenue lines that add up to the face
  total are kept as filed, with no axis, so a fact override can place them on
  the product axis.
* Any other undimensioned row is kept only when it is the plug between the
  face total and one axis of members for that period. A repeated total, and
  every other note line, stays out.
"""

from __future__ import annotations

import logging
import math
from decimal import Decimal
from typing import Optional

from ...models import FinancialFactCreate
from .concepts import (
    COMPONENT_REVENUE_CONCEPTS,
    PRODUCT_AXES,
    RESIDUAL_MEMBER,
    RESIDUAL_REVENUE_CONCEPT,
    SEGMENT_AXES,
    TOTAL_REVENUE_CONCEPTS,
    is_revenue_concept,
    is_total_revenue_concept,
)
from .context import FilingContext
from .statement import parse_statement, statement_frame

logger = logging.getLogger(__name__)

#: ``(concept, axis, member, period_end)``: one member of one line and period.
MemberKey = tuple


def is_segment_disclosure(name: str) -> bool:
    """A note that disaggregates revenue or segments, by its name."""
    if "Discontinued" in name:
        return False
    return any(token in name for token in ("Segment", "Geographic", "Revenue"))


def face_revenue_fact(
    income_facts: list[FinancialFactCreate],
) -> Optional[FinancialFactCreate]:
    """The undimensioned revenue total on the face: the largest revenue line."""
    candidates = [
        f
        for f in income_facts
        if not f.axis and f.concept in TOTAL_REVENUE_CONCEPTS and f.value is not None
    ]
    return max(candidates, key=lambda f: abs(f.value)) if candidates else None  # type: ignore[arg-type]


class SegmentSupplement:
    """Note rows that extend the face income statement, collected note by note."""

    def __init__(self, income_facts: list[FinancialFactCreate], context: FilingContext):
        self.income_facts = income_facts
        self.context = context
        face = [f for f in income_facts if not f.axis]
        self.face_concepts = {f.concept for f in face}
        self.face_labels = {f.label.strip().lower() for f in face if f.label}
        self.revenue = face_revenue_fact(income_facts)
        self.revenue_by_period = {
            (f.period_end, f.period): f.value
            for f in face
            if self.revenue is not None
            and not f.is_abstract
            and f.concept == self.revenue.concept
            and f.value is not None
        }
        self.seen = self._face_members()
        #: Accepted facts, keyed by member (or by concept and period when
        #: undimensioned), and the concept each had before it was rewritten.
        self.added: dict[tuple, FinancialFactCreate] = {}
        self.origin: dict[tuple, str] = {}

    def _face_members(self) -> set[MemberKey]:
        """Members the face reports. A member under another revenue concept is
        the same member once both are rewritten onto the face revenue line."""
        seen: set[MemberKey] = set()
        for f in self.income_facts:
            if not f.axis:
                continue
            seen.add((f.concept, f.axis, f.member, f.period_end))
            if (
                self.revenue is not None
                and is_revenue_concept_strict(f.concept)
                and f.concept != self.revenue.concept
            ):
                seen.add((self.revenue.concept, f.axis, f.member, f.period_end))
        return seen

    # --- Reading the notes ----------------------------------------------------

    def collect(self, xbrl: object) -> list[FinancialFactCreate]:
        """Read every segment note of the filing; return the facts to add."""
        if not self.face_concepts and not self.face_labels:
            return []
        try:
            available = xbrl.statements.list_available()  # type: ignore[attr-defined]
        except Exception:
            logger.exception("Could not list statements for segment disclosures")
            return []
        for _, row in available.iterrows():
            if str(row.get("category", "")) != "disclosure":
                continue
            if not is_segment_disclosure(str(row.get("name", ""))):
                continue
            role = row.get("role_name")
            if not role or (isinstance(role, float) and math.isnan(role)):
                continue
            try:
                statement = xbrl.statements.get(role)  # type: ignore[attr-defined]
                if statement is None:
                    continue
                frame = statement_frame(statement)
            except Exception:
                logger.exception("Could not read segment disclosure %s", role)
                continue
            self.read_note(
                parse_statement(frame, "Income Statement", True, self.context)
            )
        if self.added:
            logger.info(
                "Added %s segment-disclosure facts missing from the income statement",
                len(self.added),
            )
        return list(self.added.values())

    def read_note(self, parsed: list[FinancialFactCreate]) -> None:
        """Apply the rules to the facts of one note."""
        before = set(self.added)
        undimensioned = [f for f in parsed if not f.axis]
        for fact in parsed:
            if fact.axis in SEGMENT_AXES:
                self.adopt(fact)
        for fact in self.extension_revenue_members(parsed):
            self.adopt(fact)
        self.keep_revenue_components(undimensioned)

        members = [
            f
            for f in self.income_facts
            if self.revenue is not None
            and f.axis
            and f.concept == self.revenue.concept
            and isinstance(f.value, Decimal)
        ]
        members.extend(
            self.added[key]
            for key in self.added.keys() - before
            if self.revenue is not None
            and self.added[key].concept == self.revenue.concept
        )
        for fact in self.undimensioned_plugs(undimensioned, members):
            self.added.setdefault((fact.concept, fact.period_end, fact.period), fact)

    # --- The rules ------------------------------------------------------------

    def adopt(self, fact: FinancialFactCreate) -> None:
        """Keep a note fact that extends a face line, unless the face has it."""
        original = fact.concept
        if (
            self.revenue is not None
            and is_revenue_concept(original)
            and (original not in self.face_concepts)
        ):
            fact.concept = self.revenue.concept
            fact.label = self.revenue.label
        else:
            label = fact.label.strip().lower() if fact.label else ""
            if fact.concept not in self.face_concepts and label not in self.face_labels:
                return

        key = (fact.concept, fact.axis, fact.member, fact.period_end)
        if key in self.seen or key in self.added:
            if original != RESIDUAL_REVENUE_CONCEPT:
                # A revenue total replaces a component that claimed the member first.
                if (
                    key in self.added
                    and is_total_revenue_concept(original)
                    and self.origin.get(key) in COMPONENT_REVENUE_CONCEPTS
                ):
                    self.added[key] = fact
                    self.origin[key] = original
                return
            fact.member = RESIDUAL_MEMBER
            fact.member_label = RESIDUAL_MEMBER
            key = (fact.concept, fact.axis, fact.member, fact.period_end)
            if key in self.seen or key in self.added:
                return
        self.added[key] = fact
        self.origin[key] = original

    def extension_revenue_members(
        self, facts: list[FinancialFactCreate]
    ) -> list[FinancialFactCreate]:
        """Members of a company axis whose revenue adds up to the face revenue.

        A filer often reports the standard split on its own axis, and only in
        a note, after the face concept changes. An axis that falls short is
        left out, which drops partial country lists, customers and asset
        breakdowns.
        """
        grouped: dict[tuple, list[FinancialFactCreate]] = {}
        for f in facts:
            if not f.axis or f.axis in SEGMENT_AXES:
                continue
            if f.is_abstract or not is_total_revenue_concept(f.concept):
                continue
            if not isinstance(f.value, Decimal) or f.period_end is None:
                continue
            grouped.setdefault((f.axis, f.period_end, f.period), []).append(f)

        kept: list[FinancialFactCreate] = []
        for (_, period_end, period), members in grouped.items():
            face_value = self.revenue_by_period.get((period_end, period))
            if not isinstance(face_value, Decimal) or len(members) < 2:
                continue
            if sum((m.value or Decimal(0) for m in members), Decimal(0)) == face_value:
                kept.extend(members)
        return kept

    def keep_revenue_components(self, undimensioned: list[FinancialFactCreate]) -> None:
        """Keep undimensioned advertising and other-revenue lines, as filed.

        Only when the components of a period add up to the face total and the
        period has no product split yet.
        """
        if self.revenue is None:
            return
        grouped: dict[tuple, list[FinancialFactCreate]] = {}
        for f in undimensioned:
            if f.is_abstract or f.concept not in COMPONENT_REVENUE_CONCEPTS:
                continue
            if not isinstance(f.value, Decimal) or f.value == 0:
                continue
            grouped.setdefault((f.period_end, f.period), []).append(f)

        for (period_end, period), components in grouped.items():
            if len(components) < 2:
                continue
            face_value = self.revenue_by_period.get((period_end, period))
            if not isinstance(face_value, Decimal):
                continue
            if (
                sum((c.value or Decimal(0) for c in components), Decimal(0))
                != face_value
            ):
                continue
            already_split = any(
                f.period_end == period_end
                and f.period == period
                and f.concept == self.revenue.concept
                and f.axis in PRODUCT_AXES
                and isinstance(f.value, Decimal)
                for f in [*self.income_facts, *self.added.values()]
            )
            if already_split:
                continue
            for c in components:
                self.added.setdefault((c.concept, c.period_end, c.period), c)

    def undimensioned_plugs(
        self,
        undimensioned: list[FinancialFactCreate],
        members: list[FinancialFactCreate],
    ) -> list[FinancialFactCreate]:
        """Note rows that close one axis of members to the face total."""
        sums: dict[tuple, Decimal] = {}
        for m in members:
            if isinstance(m.value, Decimal):
                key = (m.period_end, m.period, m.axis)
                sums[key] = sums.get(key, Decimal(0)) + m.value

        kept = []
        for f in undimensioned:
            if f.is_abstract or f.value is None or f.value == 0:
                continue
            face_value = self.revenue_by_period.get((f.period_end, f.period))
            if face_value is None:
                continue
            if any(
                face_value - total == f.value
                for (period_end, period, _), total in sums.items()
                if period_end == f.period_end and period == f.period
            ):
                kept.append(f)
        return kept


def is_revenue_concept_strict(concept: str) -> bool:
    """A listed total or component revenue concept, without capitalization variants."""
    return concept in TOTAL_REVENUE_CONCEPTS or concept in COMPONENT_REVENUE_CONCEPTS


def segment_disclosure_facts(
    xbrl: object, income_facts: list[FinancialFactCreate], context: FilingContext
) -> list[FinancialFactCreate]:
    """Facts from the filing's segment notes to add beside its income statement."""
    return SegmentSupplement(income_facts, context).collect(xbrl)
