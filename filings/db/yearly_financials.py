"""Yearly financial metrics database operations."""

import logging
from typing import List

from sqlalchemy import MetaData, Table, and_, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ..models.yearly_financials import YearlyFinancial, YearlyFinancialsFilter

logger = logging.getLogger(__name__)


class YearlyFinancialsOperations:
    """Yearly financial metrics database operations."""

    def __init__(self, engine: Engine):
        """Initialize with database engine."""
        self.engine = engine
        # Create table metadata
        metadata = MetaData()
        self.yearly_financials_view = Table(
            "yearly_financials", metadata, autoload_with=engine
        )

    def get_yearly_financials(
        self, filter_params: YearlyFinancialsFilter
    ) -> List[YearlyFinancial]:
        """Get yearly financial metrics based on filter parameters."""
        try:
            with self.engine.connect() as conn:
                # Build the query with filters
                stmt = select(self.yearly_financials_view)
                conditions = []

                # Company ID is mandatory
                conditions.append(
                    self.yearly_financials_view.c.company_id == filter_params.company_id
                )

                # Handle fiscal year range
                if filter_params.fiscal_year_start is not None:
                    conditions.append(
                        self.yearly_financials_view.c.fiscal_year
                        >= filter_params.fiscal_year_start
                    )
                if filter_params.fiscal_year_end is not None:
                    conditions.append(
                        self.yearly_financials_view.c.fiscal_year
                        <= filter_params.fiscal_year_end
                    )

                if filter_params.label is not None:
                    conditions.append(
                        self.yearly_financials_view.c.label.ilike(
                            f"%{filter_params.label}%"
                        )
                    )

                if filter_params.normalized_label is not None:
                    conditions.append(
                        self.yearly_financials_view.c.normalized_label
                        == filter_params.normalized_label
                    )

                if filter_params.statement is not None:
                    conditions.append(
                        self.yearly_financials_view.c.statement
                        == filter_params.statement
                    )

                stmt = stmt.where(and_(*conditions))

                # Add ordering
                stmt = stmt.order_by(
                    self.yearly_financials_view.c.company_id,
                    self.yearly_financials_view.c.fiscal_year.desc(),
                    self.yearly_financials_view.c.label,
                )

                result = conn.execute(stmt)
                rows = result.fetchall()

                financials = []
                for row in rows:
                    financial = YearlyFinancial(
                        company_id=row.company_id,
                        fiscal_year=row.fiscal_year,
                        label=row.label,
                        normalized_label=row.normalized_label,
                        value=row.value,
                        unit=row.unit,
                        statement=row.statement,
                        concept=row.concept,
                        axis=row.axis,
                        member=row.member,
                        period_end=row.period_end,
                        period_start=row.period_start,
                        source_type=row.source_type,
                        fiscal_period_end=row.fiscal_period_end,
                    )
                    financials.append(financial)

                logger.info(f"Retrieved {len(financials)} yearly metrics")
                return financials

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving yearly metrics: {e}")
            return []

    def get_metrics_by_company_and_year(
        self, company_id: int, fiscal_year: int
    ) -> List[YearlyFinancial]:
        """Get all yearly metrics for a specific company and fiscal year."""
        filter_params = YearlyFinancialsFilter(
            company_id=company_id,
            fiscal_year_start=fiscal_year,
            fiscal_year_end=fiscal_year,
        )
        return self.get_yearly_financials(filter_params)

    def get_metrics_by_company(self, company_id: int) -> List[YearlyFinancial]:
        """Get yearly metrics for a specific company."""
        filter_params = YearlyFinancialsFilter(company_id=company_id)
        return self.get_yearly_financials(filter_params)

    def get_metrics_by_label(
        self, company_id: int, label: str
    ) -> List[YearlyFinancial]:
        """Get yearly metrics by label (metric name) for a specific company."""
        filter_params = YearlyFinancialsFilter(company_id=company_id, label=label)
        return self.get_yearly_financials(filter_params)

    def get_metrics_by_statement(
        self, company_id: int, statement: str
    ) -> List[YearlyFinancial]:
        """Get yearly metrics by financial statement for a specific company."""
        filter_params = YearlyFinancialsFilter(
            company_id=company_id, statement=statement
        )
        return self.get_yearly_financials(filter_params)

    def get_metrics_by_normalized_label(
        self, company_id: int, normalized_label: str
    ) -> List[YearlyFinancial]:
        """Get yearly metrics by normalized label for a specific company."""
        filter_params = YearlyFinancialsFilter(
            company_id=company_id, normalized_label=normalized_label
        )
        return self.get_yearly_financials(filter_params)

    def get_latest_metrics_by_company(
        self, company_id: int, limit: int = 20
    ) -> List[YearlyFinancial]:
        """Get the latest yearly metrics for a specific company."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    select(self.yearly_financials_view)
                    .where(self.yearly_financials_view.c.company_id == company_id)
                    .order_by(
                        self.yearly_financials_view.c.fiscal_year.desc(),
                        self.yearly_financials_view.c.label,
                    )
                    .limit(limit)
                )

                result = conn.execute(stmt)
                rows = result.fetchall()

                metrics = []
                for row in rows:
                    metric = YearlyFinancial(
                        company_id=row.company_id,
                        fiscal_year=row.fiscal_year,
                        label=row.label,
                        normalized_label=row.normalized_label,
                        value=row.value,
                        unit=row.unit,
                        statement=row.statement,
                        concept=row.concept,
                        axis=row.axis,
                        member=row.member,
                        period_end=row.period_end,
                        period_start=row.period_start,
                        source_type=row.source_type,
                        fiscal_period_end=row.fiscal_period_end,
                    )
                    metrics.append(metric)

                logger.info(
                    f"Retrieved {len(metrics)} latest yearly metrics for company {company_id}"
                )
                return metrics

        except SQLAlchemyError as e:
            logger.error(
                f"Error retrieving latest yearly metrics for company {company_id}: {e}"
            )
            return []

    def refresh_view(self) -> None:
        """Refresh the yearly financials materialized view."""
        try:
            with self.engine.connect() as conn:
                conn.execute("REFRESH MATERIALIZED VIEW yearly_financials")
                conn.commit()
                logger.info("Successfully refreshed yearly_financials view")
        except SQLAlchemyError as e:
            logger.error(f"Error refreshing yearly_financials view: {e}")
            raise
