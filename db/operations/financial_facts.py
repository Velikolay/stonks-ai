"""Financial facts database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, Table, insert, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ..models.base import FinancialFact, FinancialFactCreate

logger = logging.getLogger(__name__)


class FinancialFactOperations:
    """Financial facts database operations."""

    def __init__(self, engine: Engine):
        """Initialize with database engine."""
        self.engine = engine
        # Create table metadata
        metadata = MetaData()
        self.financial_facts_table = Table(
            "financial_facts", metadata, autoload_with=engine
        )
        self.filings_table = Table("filings", metadata, autoload_with=engine)

    def insert_financial_fact(self, fact: FinancialFactCreate) -> Optional[int]:
        """Insert a new financial fact and return its ID."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    insert(self.financial_facts_table)
                    .values(
                        filing_id=fact.filing_id,
                        metric=fact.metric,
                        value=fact.value,
                        unit=fact.unit,
                        axis=fact.axis,
                        member=fact.member,
                        statement=fact.statement,
                        period_end=fact.period_end,
                        period_start=fact.period_start,
                    )
                    .returning(self.financial_facts_table.c.id)
                )

                result = conn.execute(stmt)
                fact_id = result.scalar()
                conn.commit()

                logger.info(
                    f"Inserted financial fact: {fact.metric} with ID: {fact_id}"
                )
                return fact_id

        except SQLAlchemyError as e:
            logger.error(f"Error inserting financial fact: {e}")
            return None

    def insert_financial_facts_batch(
        self, facts: List[FinancialFactCreate]
    ) -> List[int]:
        """Insert multiple financial facts and return their IDs."""
        try:
            with self.engine.connect() as conn:
                fact_ids = []
                for fact in facts:
                    stmt = (
                        insert(self.financial_facts_table)
                        .values(
                            filing_id=fact.filing_id,
                            metric=fact.metric,
                            value=fact.value,
                            unit=fact.unit,
                            axis=fact.axis,
                            member=fact.member,
                            statement=fact.statement,
                            period_end=fact.period_end,
                            period_start=fact.period_start,
                        )
                        .returning(self.financial_facts_table.c.id)
                    )

                    result = conn.execute(stmt)
                    fact_id = result.scalar()
                    fact_ids.append(fact_id)

                conn.commit()
                logger.info(f"Inserted {len(facts)} financial facts")
                return fact_ids

        except SQLAlchemyError as e:
            logger.error(f"Error inserting financial facts batch: {e}")
            return []

    def get_financial_facts_by_filing(self, filing_id: int) -> List[FinancialFact]:
        """Get all financial facts for a filing."""
        try:
            with self.engine.connect() as conn:
                stmt = select(self.financial_facts_table).where(
                    self.financial_facts_table.c.filing_id == filing_id
                )

                result = conn.execute(stmt)

                facts = []
                for row in result:
                    facts.append(
                        FinancialFact(
                            id=row.id,
                            filing_id=row.filing_id,
                            metric=row.metric,
                            value=row.value,
                            unit=row.unit,
                            axis=row.axis,
                            member=row.member,
                            statement=row.statement,
                            period_end=row.period_end,
                            period_start=row.period_start,
                        )
                    )
                return facts

        except SQLAlchemyError as e:
            logger.error(f"Error getting financial facts by filing: {e}")
            return []

    def get_financial_facts_by_metric(
        self, company_id: int, metric: str, limit: int = 10
    ) -> List[FinancialFact]:
        """Get financial facts by company and metric."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    select(self.financial_facts_table)
                    .join(
                        self.filings_table,
                        self.financial_facts_table.c.filing_id
                        == self.filings_table.c.id,
                    )
                    .where(
                        (self.filings_table.c.company_id == company_id)
                        & (self.financial_facts_table.c.metric == metric)
                    )
                    .order_by(self.filings_table.c.filing_date.desc())
                    .limit(limit)
                )

                result = conn.execute(stmt)

                facts = []
                for row in result:
                    facts.append(
                        FinancialFact(
                            id=row.id,
                            filing_id=row.filing_id,
                            metric=row.metric,
                            value=row.value,
                            unit=row.unit,
                            axis=row.axis,
                            member=row.member,
                            statement=row.statement,
                            period_end=row.period_end,
                            period_start=row.period_start,
                        )
                    )
                return facts

        except SQLAlchemyError as e:
            logger.error(f"Error getting financial facts by metric: {e}")
            return []
