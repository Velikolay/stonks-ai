"""Async dimension breakdown database operations."""

import logging
from typing import Any, Dict, List, Set, Tuple

from sqlalchemy import MetaData, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

logger = logging.getLogger(__name__)

# (statement, normalized_label, axis, member) of the series being broken down.
# Axis and member are '' for the undimensioned line.
SeriesKey = Tuple[str, str, str, str]


class DimensionBreakdownsOperationsAsync:
    """Read the breakdowns written by refresh_dimension_breakdowns."""

    def __init__(self, engine: AsyncEngine, metadata: MetaData):
        """Initialize with async engine and metadata."""
        self.engine = engine
        self.dimension_breakdowns = metadata.tables["dimension_breakdowns"]

    async def get_breakdowns(
        self, company_id: int
    ) -> Dict[SeriesKey, List[Dict[str, Any]]]:
        """Return a company's breakdowns grouped by the series they split.

        Args:
            company_id: Company to read.

        Returns:
            For each series, its alternative breakdowns as dicts with ``axis``,
            ``members`` and ``fiscal_years``.
        """
        table = self.dimension_breakdowns
        try:
            async with self.engine.connect() as conn:
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
                    "fiscal_years": list(row.fiscal_years or []),
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
