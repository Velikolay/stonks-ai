"""Yearly financial metrics database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, Table, and_, func, or_, select, text
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

                if filter_params.labels is not None:
                    label_conditions = []
                    for label in filter_params.labels:
                        label_conditions.append(
                            self.yearly_financials_view.c.label.ilike(f"%{label}%")
                        )
                    if label_conditions:
                        conditions.append(or_(*label_conditions))
                        conditions.append(
                            self.yearly_financials_view.c.is_abstract.is_(False)
                        )

                if filter_params.normalized_labels is not None:
                    conditions.append(
                        self.yearly_financials_view.c.normalized_label.in_(
                            filter_params.normalized_labels
                        )
                    )
                    conditions.append(
                        self.yearly_financials_view.c.is_abstract.is_(False)
                    )

                if filter_params.statement is not None:
                    conditions.append(
                        self.yearly_financials_view.c.statement
                        == filter_params.statement
                    )

                # Handle axis filter
                if filter_params.axis is not None:
                    # Specific axis value
                    conditions.append(
                        self.yearly_financials_view.c.axis == filter_params.axis
                    )
                else:
                    # If axis filter is not provided, only return records where axis is empty
                    conditions.append(self.yearly_financials_view.c.axis == "")

                stmt = stmt.where(and_(*conditions)).order_by(
                    self.yearly_financials_view.c.company_id,
                    self.yearly_financials_view.c.statement,
                    self.yearly_financials_view.c.position,
                    self.yearly_financials_view.c.period_end.desc(),
                )

                result = conn.execute(stmt)
                rows = result.fetchall()

                financials = []
                for row in rows:
                    financial = YearlyFinancial(
                        id=row.id,
                        company_id=row.company_id,
                        filing_id=row.filing_id,
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
                        fiscal_year=row.fiscal_year,
                        source_type=row.source_type,
                        concept=getattr(row, "concept", None),
                    )
                    financials.append(financial)

                logger.info(f"Retrieved {len(financials)} yearly metrics")
                return financials

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving yearly metrics: {e}")
            return []

    def get_metrics_by_company(self, company_id: int) -> List[YearlyFinancial]:
        """Get yearly metrics for a specific company."""
        filter_params = YearlyFinancialsFilter(company_id=company_id)
        return self.get_yearly_financials(filter_params)

    def get_metrics_by_company_and_year(
        self, company_id: int, fiscal_year: int
    ) -> List[YearlyFinancial]:
        """Get yearly metrics for a specific company and fiscal year."""
        filter_params = YearlyFinancialsFilter(
            company_id=company_id,
            fiscal_year_start=fiscal_year,
            fiscal_year_end=fiscal_year,
        )
        return self.get_yearly_financials(filter_params)

    def get_metrics_by_label(
        self, company_id: int, label: str
    ) -> List[YearlyFinancial]:
        """Get yearly metrics by label (metric name) for a specific company."""
        filter_params = YearlyFinancialsFilter(company_id=company_id, labels=[label])
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
            company_id=company_id, normalized_labels=[normalized_label]
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
                    .where(
                        self.yearly_financials_view.c.company_id == company_id,
                    )
                    .order_by(
                        self.yearly_financials_view.c.period_end.desc(),
                        self.yearly_financials_view.c.label,
                    )
                    .limit(limit)
                )

                result = conn.execute(stmt)
                rows = result.fetchall()

                metrics = []
                for row in rows:
                    metric = YearlyFinancial(
                        id=row.id,
                        company_id=row.company_id,
                        filing_id=row.filing_id,
                        fiscal_year=row.fiscal_year,
                        label=row.label,
                        normalized_label=row.normalized_label,
                        value=row.value,
                        weight=row.weight,
                        unit=row.unit,
                        statement=row.statement if row.statement else None,
                        period_end=row.period_end,
                        abstract_id=row.abstract_id,
                        is_abstract=row.is_abstract,
                        is_synthetic=row.is_synthetic,
                        source_type=row.source_type,
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

    def get_normalized_labels(
        self, company_id: int, statement: Optional[str] = None
    ) -> List[dict]:
        """Get all normalized labels and their counts for yearly financials."""
        try:
            with self.engine.connect() as conn:
                # Build query using SQLAlchemy
                stmt = (
                    select(
                        self.yearly_financials_view.c.normalized_label,
                        self.yearly_financials_view.c.statement,
                        self.yearly_financials_view.c.axis,
                        self.yearly_financials_view.c.member,
                        func.count().label("count"),
                    )
                    .where(
                        self.yearly_financials_view.c.normalized_label.is_not(None),
                        self.yearly_financials_view.c.company_id == company_id,
                    )
                    .group_by(
                        self.yearly_financials_view.c.normalized_label,
                        self.yearly_financials_view.c.statement,
                        self.yearly_financials_view.c.axis,
                        self.yearly_financials_view.c.member,
                    )
                    .order_by(
                        self.yearly_financials_view.c.statement,
                        func.count().desc(),
                    )
                )

                # Add statement filter if provided
                if statement:
                    stmt = stmt.where(
                        self.yearly_financials_view.c.statement == statement
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
                    f"Retrieved {len(labels)} normalized labels for yearly financials"
                )
                return labels

        except SQLAlchemyError as e:
            logger.error(
                f"Error retrieving normalized labels for yearly financials: {e}"
            )
            return []

    def refresh_materialized_view(self, concurrent: bool = False) -> None:
        """Refresh the yearly_financials materialized view."""
        view_name = "yearly_financials"
        with self.engine.connect() as conn:
            if concurrent:
                sql = text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}")
            else:
                sql = text(f"REFRESH MATERIALIZED VIEW {view_name}")

            conn.execute(sql)
            conn.commit()
