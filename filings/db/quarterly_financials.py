"""Async quarterly financial metrics database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, and_, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from filings.models.quarterly_financials import (
    QuarterlyFinancial,
    QuarterlyFinancialsFilter,
)

logger = logging.getLogger(__name__)


class QuarterlyFinancialsOperationsAsync:
    """Async quarterly financial metrics database operations."""

    def __init__(self, engine: AsyncEngine, metadata: MetaData):
        """Initialize with async engine and metadata."""
        self.engine = engine
        self.quarterly_financials_view = metadata.tables["quarterly_financials"]

    async def get_quarterly_financials(
        self, filter_params: QuarterlyFinancialsFilter
    ) -> List[QuarterlyFinancial]:
        """Get quarterly financial metrics based on filter parameters."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.quarterly_financials_view)
                conditions = []

                conditions.append(
                    self.quarterly_financials_view.c.company_id
                    == filter_params.company_id
                )

                if filter_params.fiscal_year_start is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.fiscal_year
                        >= filter_params.fiscal_year_start
                    )
                if filter_params.fiscal_year_end is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.fiscal_year
                        <= filter_params.fiscal_year_end
                    )

                if filter_params.fiscal_quarter_start is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.fiscal_quarter
                        >= filter_params.fiscal_quarter_start
                    )
                if filter_params.fiscal_quarter_end is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.fiscal_quarter
                        <= filter_params.fiscal_quarter_end
                    )

                if filter_params.labels is not None:
                    label_conditions = []
                    for label in filter_params.labels:
                        label_conditions.append(
                            self.quarterly_financials_view.c.label.ilike(f"%{label}%")
                        )
                    if label_conditions:
                        conditions.append(or_(*label_conditions))
                        conditions.append(
                            self.quarterly_financials_view.c.is_abstract.is_(False)
                        )

                if filter_params.normalized_labels is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.normalized_label.in_(
                            filter_params.normalized_labels
                        )
                    )
                    conditions.append(
                        self.quarterly_financials_view.c.is_abstract.is_(False)
                    )

                if filter_params.statement is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.statement
                        == filter_params.statement
                    )

                if filter_params.axis is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.axis == filter_params.axis
                    )
                else:
                    conditions.append(self.quarterly_financials_view.c.axis == "")

                stmt = stmt.where(and_(*conditions)).order_by(
                    self.quarterly_financials_view.c.company_id,
                    self.quarterly_financials_view.c.statement,
                    self.quarterly_financials_view.c.position,
                    self.quarterly_financials_view.c.period_end.desc(),
                )

                result = await conn.execute(stmt)
                rows = result.fetchall()

                financials = []
                for row in rows:
                    financial = QuarterlyFinancial(
                        id=row.id,
                        company_id=row.company_id,
                        filing_id=row.filing_id,
                        fiscal_year=row.fiscal_year,
                        fiscal_quarter=row.fiscal_quarter,
                        label=row.label,
                        normalized_label=row.normalized_label,
                        value=row.value,
                        weight=row.weight,
                        unit=row.unit,
                        statement=row.statement if row.statement else None,
                        axis=row.axis if row.axis else None,
                        member=row.member if row.member else None,
                        abstract_id=row.abstract_id,
                        is_abstract=row.is_abstract,
                        is_synthetic=row.is_synthetic,
                        period_end=row.period_end,
                        source_type=row.source_type,
                        concept=getattr(row, "concept", None),
                    )
                    financials.append(financial)

                logger.info(f"Retrieved {len(financials)} quarterly metrics")
                return financials

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving quarterly metrics: {e}")
            return []

    async def get_normalized_labels(
        self, company_id: int, statement: Optional[str] = None
    ) -> List[dict]:
        """Get all normalized labels and their counts for quarterly financials."""
        try:
            async with self.engine.connect() as conn:
                stmt = (
                    select(
                        self.quarterly_financials_view.c.normalized_label,
                        self.quarterly_financials_view.c.statement,
                        self.quarterly_financials_view.c.axis,
                        self.quarterly_financials_view.c.member,
                        func.count().label("count"),
                    )
                    .where(
                        self.quarterly_financials_view.c.normalized_label.is_not(None),
                        self.quarterly_financials_view.c.company_id == company_id,
                        self.quarterly_financials_view.c.is_abstract.is_(False),
                    )
                    .group_by(
                        self.quarterly_financials_view.c.normalized_label,
                        self.quarterly_financials_view.c.statement,
                        self.quarterly_financials_view.c.axis,
                        self.quarterly_financials_view.c.member,
                    )
                    .order_by(
                        self.quarterly_financials_view.c.statement,
                        func.count().desc(),
                    )
                )

                if statement:
                    stmt = stmt.where(
                        self.quarterly_financials_view.c.statement == statement
                    )

                result = await conn.execute(stmt)
                rows = result.fetchall()

                labels = []
                for row in rows:
                    label_info = {
                        "normalized_label": row.normalized_label,
                        "statement": row.statement if row.statement else None,
                        "axis": row.axis if row.axis else None,
                        "member": row.member if row.member else None,
                        "count": row.count,
                    }
                    labels.append(label_info)

                logger.info(
                    f"Retrieved {len(labels)} normalized labels for quarterly financials"
                )
                return labels

        except SQLAlchemyError as e:
            logger.error(
                f"Error retrieving normalized labels for quarterly financials: {e}"
            )
            return []

    async def get_metrics_by_company(self, company_id: int) -> List[QuarterlyFinancial]:
        """Get all quarterly metrics for a company."""
        return await self.get_quarterly_financials(
            QuarterlyFinancialsFilter(company_id=company_id)
        )

    async def get_metrics_by_company_and_year(
        self, company_id: int, fiscal_year: int
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics for a company in a specific year."""
        return await self.get_quarterly_financials(
            QuarterlyFinancialsFilter(
                company_id=company_id,
                fiscal_year_start=fiscal_year,
                fiscal_year_end=fiscal_year,
            )
        )

    async def get_metrics_by_label(
        self, company_id: int, label: str
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics for a company filtered by label."""
        return await self.get_quarterly_financials(
            QuarterlyFinancialsFilter(company_id=company_id, labels=[label])
        )

    async def get_metrics_by_statement(
        self, company_id: int, statement: str
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics for a company filtered by statement."""
        return await self.get_quarterly_financials(
            QuarterlyFinancialsFilter(company_id=company_id, statement=statement)
        )

    async def get_latest_metrics_by_company(
        self, company_id: int, limit: int = 10
    ) -> List[QuarterlyFinancial]:
        """Get latest quarterly metrics for a company by period_end."""
        try:
            async with self.engine.connect() as conn:
                stmt = (
                    select(self.quarterly_financials_view)
                    .where(self.quarterly_financials_view.c.company_id == company_id)
                    .order_by(self.quarterly_financials_view.c.period_end.desc())
                    .limit(limit * 100)
                )
                result = await conn.execute(stmt)
                rows = result.fetchall()
                financials = []
                seen_periods = set()
                for row in rows:
                    key = (row.period_end, row.statement, row.normalized_label)
                    if key in seen_periods:
                        continue
                    seen_periods.add(key)
                    if len(seen_periods) > limit * 20:
                        break
                    financial = QuarterlyFinancial(
                        id=row.id,
                        company_id=row.company_id,
                        filing_id=row.filing_id,
                        fiscal_year=row.fiscal_year,
                        fiscal_quarter=row.fiscal_quarter,
                        label=row.label,
                        normalized_label=row.normalized_label,
                        value=row.value,
                        weight=row.weight,
                        unit=row.unit,
                        statement=row.statement if row.statement else None,
                        axis=row.axis if row.axis else None,
                        member=row.member if row.member else None,
                        abstract_id=row.abstract_id,
                        is_abstract=row.is_abstract,
                        is_synthetic=row.is_synthetic,
                        period_end=row.period_end,
                        source_type=row.source_type,
                        concept=getattr(row, "concept", None),
                    )
                    financials.append(financial)
                    if len(financials) >= limit * 50:
                        break
                return financials[: limit * 50]
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving latest quarterly metrics: {e}")
            return []


# Alias for backward compatibility
QuarterlyFinancialsOperations = QuarterlyFinancialsOperationsAsync
