"""Quarterly financial metrics database operations."""

import logging
from typing import List

from sqlalchemy import MetaData, Table, and_, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ..models.quarterly_financials import QuarterlyFinancial, QuarterlyFinancialsFilter

logger = logging.getLogger(__name__)


class QuarterlyFinancialsOperations:
    """Quarterly financial metrics database operations."""

    def __init__(self, engine: Engine):
        """Initialize with database engine."""
        self.engine = engine
        # Create table metadata
        metadata = MetaData()
        self.quarterly_financials_view = Table(
            "quarterly_financials", metadata, autoload_with=engine
        )

    def get_quarterly_financials(
        self, filter_params: QuarterlyFinancialsFilter
    ) -> List[QuarterlyFinancial]:
        """Get quarterly financial metrics based on filter parameters."""
        try:
            with self.engine.connect() as conn:
                # Build the query with filters
                stmt = select(self.quarterly_financials_view)
                conditions = []

                if filter_params.company_id is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.company_id
                        == filter_params.company_id
                    )

                if filter_params.fiscal_year is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.fiscal_year
                        == filter_params.fiscal_year
                    )

                if filter_params.fiscal_quarter is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.fiscal_quarter
                        == filter_params.fiscal_quarter
                    )

                if filter_params.label is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.label.ilike(
                            f"%{filter_params.label}%"
                        )
                    )

                if filter_params.normalized_label is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.normalized_label
                        == filter_params.normalized_label
                    )

                if filter_params.statement is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.statement
                        == filter_params.statement
                    )

                if filter_params.source_type is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.source_type
                        == filter_params.source_type
                    )

                if conditions:
                    stmt = stmt.where(and_(*conditions))

                # Add ordering and limit
                stmt = stmt.order_by(
                    self.quarterly_financials_view.c.company_id,
                    self.quarterly_financials_view.c.fiscal_year.desc(),
                    self.quarterly_financials_view.c.fiscal_quarter.desc(),
                )

                if filter_params.limit:
                    stmt = stmt.limit(filter_params.limit)

                result = conn.execute(stmt)
                rows = result.fetchall()

                financials = []
                for row in rows:
                    financial = QuarterlyFinancial(
                        company_id=row.company_id,
                        fiscal_year=row.fiscal_year,
                        fiscal_quarter=row.fiscal_quarter,
                        label=row.label,
                        normalized_label=row.normalized_label,
                        value=row.value,
                        unit=row.unit,
                        statement=row.statement,
                        period_end=row.period_end,
                        period_start=row.period_start,
                        source_type=row.source_type,
                    )
                    financials.append(financial)

                logger.info(f"Retrieved {len(financials)} quarterly metrics")
                return financials

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving quarterly metrics: {e}")
            return []

    def get_metrics_by_company_and_year(
        self, company_id: int, fiscal_year: int
    ) -> List[QuarterlyFinancial]:
        """Get all quarterly metrics for a specific company and fiscal year."""
        filter_params = QuarterlyFinancialsFilter(
            company_id=company_id, fiscal_year=fiscal_year
        )
        return self.get_quarterly_financials(filter_params)

    def get_metrics_by_company(
        self, company_id: int, limit: int = 100
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics for a specific company."""
        filter_params = QuarterlyFinancialsFilter(company_id=company_id, limit=limit)
        return self.get_quarterly_financials(filter_params)

    def get_metrics_by_label(
        self, label: str, limit: int = 100
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics by label (metric name)."""
        filter_params = QuarterlyFinancialsFilter(label=label, limit=limit)
        return self.get_quarterly_financials(filter_params)

    def get_metrics_by_statement(
        self, statement: str, limit: int = 100
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics by financial statement."""
        filter_params = QuarterlyFinancialsFilter(statement=statement, limit=limit)
        return self.get_quarterly_financials(filter_params)

    def get_metrics_by_normalized_label(
        self, normalized_label: str, limit: int = 100
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics by normalized label."""
        filter_params = QuarterlyFinancialsFilter(
            normalized_label=normalized_label, limit=limit
        )
        return self.get_quarterly_financials(filter_params)

    def get_latest_metrics_by_company(
        self, company_id: int, limit: int = 20
    ) -> List[QuarterlyFinancial]:
        """Get the latest quarterly metrics for a specific company."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    select(self.quarterly_financials_view)
                    .where(self.quarterly_financials_view.c.company_id == company_id)
                    .order_by(
                        self.quarterly_financials_view.c.fiscal_year.desc(),
                        self.quarterly_financials_view.c.fiscal_quarter.desc(),
                        self.quarterly_financials_view.c.label,
                    )
                    .limit(limit)
                )

                result = conn.execute(stmt)
                rows = result.fetchall()

                metrics = []
                for row in rows:
                    metric = QuarterlyFinancial(
                        company_id=row.company_id,
                        fiscal_year=row.fiscal_year,
                        fiscal_quarter=row.fiscal_quarter,
                        label=row.label,
                        normalized_label=row.normalized_label,
                        value=row.value,
                        unit=row.unit,
                        statement=row.statement,
                        period_end=row.period_end,
                        period_start=row.period_start,
                        source_type=row.source_type,
                    )
                    metrics.append(metric)

                logger.info(
                    f"Retrieved {len(metrics)} latest quarterly metrics for company {company_id}"
                )
                return metrics

        except SQLAlchemyError as e:
            logger.error(
                f"Error retrieving latest quarterly metrics for company {company_id}: {e}"
            )
            return []
