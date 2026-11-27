"""Async dimension breakdown database operations."""

import logging
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from sqlalchemy import MetaData, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from ..refresh.rules.periods import ANNUAL_FORMS, FOREIGN_ANNUAL_FORMS

logger = logging.getLogger(__name__)

# (statement, normalized_label, axis, member) of the series being broken down.
# Axis and member are '' for the undimensioned line.
SeriesKey = Tuple[str, str, str, str]


def filed_years(fiscal_years: Sequence[int], first_year: Optional[int]) -> List[int]:
    """The years a breakdown adds up in, from the company's first annual report on.

    The chart dates a split by its first year to tell an earlier presentation
    from an alternative view. The year before the first annual report only
    exists as that report's comparative (see ``filings/refresh/rules/
    prior_periods.py``), and a split the filer printed beside it starts there
    while one it began the same year does not: Apple's regional segments
    have FY2008, its U.S., China and other countries split only FY2009, and
    the regions would become an earlier presentation of that view. A split
    whose only years are earlier keeps them.
    """
    years = sorted(fiscal_years)
    if first_year is None:
        return years
    filed = [year for year in years if year >= first_year]
    return filed or years


class DimensionBreakdownsOperationsAsync:
    """Read the breakdowns written by refresh_dimension_breakdowns."""

    def __init__(self, engine: AsyncEngine, metadata: MetaData):
        """Initialize with async engine and metadata."""
        self.engine = engine
        self.dimension_breakdowns = metadata.tables["dimension_breakdowns"]
        self.filings = metadata.tables["filings"]

    async def get_breakdowns(
        self, company_id: int
    ) -> Dict[SeriesKey, List[Dict[str, Any]]]:
        """Return a company's breakdowns grouped by the series they split.

        Args:
            company_id: Company to read.

        Returns:
            For each series, its alternative breakdowns as dicts with ``axis``,
            ``members`` and ``fiscal_years``, the years from the company's
            first annual report on (:func:`filed_years`).
        """
        table = self.dimension_breakdowns
        filings = self.filings
        try:
            async with self.engine.connect() as conn:
                first_year = await conn.scalar(
                    select(func.min(filings.c.fiscal_year)).where(
                        (filings.c.company_id == company_id)
                        & filings.c.form_type.in_(
                            [*ANNUAL_FORMS, *FOREIGN_ANNUAL_FORMS]
                        )
                        & ~filings.c.is_shell
                    )
                )
                result = await conn.execute(
                    select(
                        table.c.statement,
                        table.c.normalized_label,
                        table.c.axis,
                        table.c.parent_member,
                        table.c.members,
                        table.c.fiscal_years,
                    ).where(table.c.company_id == company_id)
                )
                rows = sorted(
                    result,
                    key=lambda row: (
                        -max(row.fiscal_years or [0]),
                        -len(row.fiscal_years or []),
                        row.axis,
                        row.members,
                    ),
                )
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving dimension breakdowns: {e}")
            return {}

        breakdowns: Dict[SeriesKey, List[Dict[str, Any]]] = {}
        for row in rows:
            key = (
                row.statement,
                row.normalized_label,
                row.axis if row.parent_member else "",
                row.parent_member,
            )
            breakdowns.setdefault(key, []).append(
                {
                    "axis": row.axis,
                    "members": list(row.members),
                    "fiscal_years": filed_years(row.fiscal_years or [], first_year),
                }
            )
        return breakdowns

    async def get_reported_members(
        self, company_id: int, financials: Any
    ) -> Set[Tuple[str, str, str, str]]:
        """Return members that exist for one company in a presentation table.

        Args:
            company_id: Company to read.
            financials: ``quarterly_financials`` or ``yearly_financials`` table.

        Returns:
            ``(statement, normalized_label, axis, member)`` for dimensional rows.
        """
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(
                    select(
                        financials.c.statement,
                        financials.c.normalized_label,
                        financials.c.axis,
                        financials.c.member,
                    )
                    .where(financials.c.company_id == company_id)
                    .where(financials.c.axis != "")
                    .distinct()
                )
                return {
                    (row.statement, row.normalized_label, row.axis, row.member)
                    for row in result
                }
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving reported dimension members: {e}")
            return set()
