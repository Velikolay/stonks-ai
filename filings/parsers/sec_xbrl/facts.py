"""One statement row and period column as a :class:`FinancialFactCreate`.

A face row becomes a fact with its place in the statement tree: keys for
its parent line and its section header. A dimension row becomes a fact on
one axis and member, with every XBRL dimension it carries, and no place in
the tree.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional, cast

from ...models import FactDimension, FinancialFactCreate
from .columns import column_end, period_type
from .context import FilingContext
from .values import (
    as_date,
    fact_key,
    filed_unit,
    is_nullish,
    optional_decimal,
    print_sign,
    sec_concept,
    sec_member,
    to_decimal,
)

logger = logging.getLogger(__name__)

#: Presentation scaffolding, not section headers a reader sees.
_SCAFFOLD_MARKERS = ("[Abstract]", "[Table]", "[Line Items]", "[Axis]", "[Domain]")


#: A row's ``(value, comparative, weight)``.
Amounts = tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal]]


def _amounts(
    row: Any,
    concept: str,
    is_abstract: object,
    period_col: str,
    comparative_col: Optional[str],
) -> Optional[Amounts]:
    """``(value, comparative, weight)`` of a row, or None when the value is bad.

    Headers carry no amounts. A row without a value in ``period_col`` has no
    fact for that period, and neither does one whose value is not a number.
    """
    if is_abstract:
        return None, None, None
    value = row.get(period_col)
    if is_nullish(value):
        return None
    try:
        amount = to_decimal(value)
    except (ValueError, TypeError):
        logger.warning("Invalid value for concept %s: %s", concept, value)
        return None
    comparative = row.get(comparative_col) if comparative_col else None
    return (
        amount,
        optional_decimal(comparative, "comparative value", concept),
        optional_decimal(row.get("weight"), "weight", concept),
    )


def face_fact(
    row: Any,
    statement: str,
    period_col: str,
    comparative_col: Optional[str],
    position: int,
    context: FilingContext = FilingContext(),
) -> Optional[FinancialFactCreate]:
    """A face-statement row's fact for ``period_col``, or None when it has none."""
    try:
        concept = sec_concept(row.get("concept", "")) or ""
        if not concept:
            return None
        label = row.get("label", concept)
        is_abstract = row.get("abstract")
        if is_abstract and any(marker in label for marker in _SCAFFOLD_MARKERS):
            return None
        amounts = _amounts(row, concept, is_abstract, period_col, comparative_col)
        if amounts is None:
            return None
        value, comparative, weight = amounts

        unit = filed_unit(row.get("unit"), "usd" if not is_abstract else None)
        period_end = as_date(column_end(period_col))
        if period_end is None:
            return None
        comparative_end = (
            as_date(column_end(comparative_col)) if comparative_col else None
        )
        value, unit = context.reporting_amount(concept, period_end, value, unit)
        comparative, _ = context.reporting_amount(
            concept, comparative_end, comparative, unit
        )

        end = period_end.isoformat()
        parent = sec_concept(row.get("parent_concept"))
        header = sec_concept(row.get("parent_abstract_concept"))
        return FinancialFactCreate(
            key=fact_key(concept, statement, end),
            parent_key=fact_key(parent, statement, end) if parent else None,
            abstract_key=fact_key(header, statement, end) if header else None,
            company_id=0,
            filing_id=0,
            form_type="",
            concept=concept,
            label=label,
            value=value,
            is_abstract=is_abstract,
            comparative_value=comparative,
            weight=weight,
            print_sign=print_sign(row),
            unit=unit,
            axis=None,
            member=None,
            statement=statement,
            period_end=period_end,
            comparative_period_end=comparative_end,
            period=period_type(period_col, statement, row.get("point_in_time")),
            position=position,
        )
    except Exception:
        logger.exception("Error creating financial fact from row %s", row)
        return None


def dimension_coordinates(
    row: Any, axis: object, member: object, member_label: object, context: FilingContext
) -> list[FactDimension]:
    """Every XBRL dimension on a row, one per axis.

    The display member can be a collapsed label; these coordinates are the
    members the instance reported. Without raw coordinates on the row, the
    display axis and member are the only coordinate.
    """
    raw = row.get("dimension_metadata") if hasattr(row, "get") else None
    coordinates: list[FactDimension] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            dim_axis = sec_member(item.get("dimension") or "")
            dim_member = sec_member(item.get("member") or "")
            if not dim_axis or not dim_member:
                continue
            coordinates.append(
                FactDimension(
                    axis=dim_axis,
                    member=dim_member,
                    member_label=str(item.get("member_label") or dim_member),
                    is_default=context.is_axis_total(dim_member),
                )
            )
    if coordinates:
        return coordinates
    if axis and member:
        return [
            FactDimension(
                axis=str(axis),
                member=str(member),
                member_label=str(member_label or member),
                is_default=context.is_axis_total(str(member)),
            )
        ]
    return []


def _display_member(
    row: Any,
    member: str,
    member_label: object,
    collapsed_members: Optional[set[str]],
    ambiguous_suffixes: Optional[set[str]],
) -> tuple[str, object]:
    """The member a reader sees, undoing edgartools' collapsed members.

    edgartools stamps the parent member on every child and puts the child
    name after "{parent label} - ". Only those reused member ids are
    rewritten. A distinct member labeled "{member} - {group}" already names
    itself; the suffix is the group, not a new member. A suffix two parents
    share keeps the full label, so the parents stay distinct.
    """
    label_text = row.get("label")
    prefix = f"{member_label} - " if isinstance(member_label, str) else None
    if not (
        collapsed_members
        and member in collapsed_members
        and prefix
        and isinstance(label_text, str)
        and label_text.startswith(prefix)
    ):
        return member, member_label
    suffix = label_text[len(prefix) :].strip()
    if ambiguous_suffixes and suffix in ambiguous_suffixes:
        name = label_text.strip()
    else:
        name = label_text[len(prefix) :]
    return name, name


def dimension_fact(
    row: Any,
    statement: str,
    period_col: str,
    comparative_col: Optional[str],
    base_label_by_concept: dict[str, str],
    context: FilingContext,
    collapsed_members: Optional[set[str]] = None,
    ambiguous_suffixes: Optional[set[str]] = None,
) -> Optional[FinancialFactCreate]:
    """A dimension row's fact for ``period_col``, or None when it has none.

    It takes the label of the face line with the same concept, so it joins
    that line's series, and has no parent or header key.
    """
    try:
        concept = sec_concept(row.get("concept", ""))
        if not concept:
            return None
        axis = row.get("dimension_axis")
        raw_member = row.get("dimension_member")
        if not isinstance(raw_member, str):
            return None
        member, member_label = _display_member(
            row,
            sec_member(raw_member),
            row.get("dimension_member_label"),
            collapsed_members,
            ambiguous_suffixes,
        )
        if not axis or member is None:
            return None
        is_abstract = row.get("abstract")
        amounts = _amounts(row, concept, is_abstract, period_col, comparative_col)
        if amounts is None:
            return None
        value, comparative, weight = amounts

        period_end = as_date(column_end(period_col))
        if period_end is None:
            return None
        comparative_end = (
            as_date(column_end(comparative_col)) if comparative_col else None
        )
        end = period_end.isoformat()
        return FinancialFactCreate(
            key=fact_key(concept, statement, end, axis, member, member_label),
            parent_key=None,
            abstract_key=None,
            company_id=0,
            filing_id=0,
            form_type="",
            concept=concept,
            label=base_label_by_concept.get(concept, row.get("label", concept)),
            value=value,
            is_abstract=is_abstract,
            comparative_value=comparative,
            weight=weight,
            print_sign=print_sign(row),
            unit=filed_unit(row.get("unit"), "usd" if not is_abstract else None),
            axis=axis,
            member=member,
            member_label=cast(Optional[str], member_label),
            dimensions=dimension_coordinates(row, axis, member, member_label, context),
            statement=statement,
            period_end=period_end,
            comparative_period_end=comparative_end,
            period=period_type(period_col, statement, row.get("point_in_time")),
            position=None,
        )
    except Exception:
        logger.exception("Error creating dimension fact from row %s", row)
        return None
