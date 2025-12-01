"""Quarterly financial metrics database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, Table, and_, func, or_, select, text
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

                # Company ID is mandatory
                conditions.append(
                    self.quarterly_financials_view.c.company_id
                    == filter_params.company_id
                )

                # Handle fiscal year range
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

                # Handle fiscal quarter range
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

                if filter_params.normalized_labels is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.normalized_label.in_(
                            filter_params.normalized_labels
                        )
                    )

                if filter_params.statement is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.statement
                        == filter_params.statement
                    )

                # Handle axis filter
                if filter_params.axis is not None:
                    conditions.append(
                        self.quarterly_financials_view.c.axis == filter_params.axis
                    )
                else:
                    # If axis filter is not provided, only return records where axis is empty
                    conditions.append(self.quarterly_financials_view.c.axis == "")

                stmt = stmt.where(and_(*conditions))

                result = conn.execute(stmt)
                rows = result.fetchall()

                financials = []
                for row in rows:
                    financial = QuarterlyFinancial(
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
                        abstracts=row.abstracts,
                        period_end=row.period_end,
                        aggregation=getattr(row, "aggregation", None),
                        source_type=row.source_type,
                        concept=getattr(row, "concept", None),
                        abstract_concepts=getattr(row, "abstract_concepts", None),
                    )
                    financials.append(financial)

                logger.info(f"Retrieved {len(financials)} quarterly metrics")
                return financials

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving quarterly metrics: {e}")
            return []

    def get_metrics_by_company(self, company_id: int) -> List[QuarterlyFinancial]:
        """Get quarterly metrics for a specific company."""
        filter_params = QuarterlyFinancialsFilter(company_id=company_id)
        return self.get_quarterly_financials(filter_params)

    def get_metrics_by_company_and_year(
        self, company_id: int, fiscal_year: int
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics for a specific company and fiscal year.

        Args:
            company_id: Company ID
            fiscal_year: Fiscal year to filter by

        Returns:
            List of quarterly financial metrics
        """
        filter_params = QuarterlyFinancialsFilter(
            company_id=company_id,
            fiscal_year_start=fiscal_year,
            fiscal_year_end=fiscal_year,
        )
        return self.get_quarterly_financials(filter_params)

    def get_metrics_by_label(
        self, company_id: int, label: str
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics by label (metric name) for a specific company."""
        filter_params = QuarterlyFinancialsFilter(company_id=company_id, labels=[label])
        return self.get_quarterly_financials(filter_params)

    def get_metrics_by_statement(
        self, company_id: int, statement: str
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics by financial statement for a specific company."""
        filter_params = QuarterlyFinancialsFilter(
            company_id=company_id, statement=statement
        )
        return self.get_quarterly_financials(filter_params)

    def get_metrics_by_normalized_label(
        self, company_id: int, normalized_label: str
    ) -> List[QuarterlyFinancial]:
        """Get quarterly metrics by normalized label for a specific company."""
        filter_params = QuarterlyFinancialsFilter(
            company_id=company_id, normalized_labels=[normalized_label]
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
                        self.quarterly_financials_view.c.period_end.desc(),
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
                        abstracts=row.abstracts,
                        period_end=row.period_end,
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

    def get_normalized_labels(
        self, company_id: int, statement: Optional[str] = None
    ) -> List[dict]:
        """Get all normalized labels and their counts for quarterly financials."""
        try:
            with self.engine.connect() as conn:
                # Build query using SQLAlchemy
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

                # Add statement filter if provided
                if statement:
                    stmt = stmt.where(
                        self.quarterly_financials_view.c.statement == statement
                    )

                result = conn.execute(stmt)
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

    def refresh_materialized_view(self, concurrent: bool = False) -> None:
        """Refresh the quarterly_financials materialized view."""
        view_name = "quarterly_financials"
        with self.engine.connect() as conn:
            if concurrent:
                sql = text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
            else:
                sql = text(f"REFRESH MATERIALIZED VIEW {view_name}")

            conn.execute(sql)
            conn.commit()
