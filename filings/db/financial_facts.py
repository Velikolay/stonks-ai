"""Financial facts database operations."""

import logging
from typing import Optional

from sqlalchemy import MetaData, Table, insert, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ..models import FinancialFact, FinancialFactCreate, PeriodType

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

    def insert_financial_fact(self, fact: FinancialFact) -> Optional[int]:
        """Insert a new financial fact and return its ID."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    insert(self.financial_facts_table)
                    .values(
                        parent_id=fact.parent_id,
                        filing_id=fact.filing_id,
                        concept=fact.concept,
                        label=fact.label,
                        is_abstract=fact.is_abstract,
                        value=fact.value,
                        comparative_value=fact.comparative_value,
                        weight=fact.weight,
                        unit=fact.unit,
                        axis=fact.axis,
                        member=fact.member,
                        parsed_axis=fact.parsed_axis,
                        parsed_member=fact.parsed_member,
                        statement=fact.statement,
                        period_end=fact.period_end,
                        comparative_period_end=fact.comparative_period_end,
                        period=fact.period.value if fact.period is not None else None,
                        position=fact.position,
                    )
                    .returning(self.financial_facts_table.c.id)
                )

                result = conn.execute(stmt)
                fact_id = result.scalar()
                conn.commit()

                return fact_id

        except SQLAlchemyError as e:
            logger.error(f"Error inserting financial fact ${fact}: {e}")
            return None

    def insert_financial_facts_batch(
        self, facts: list[FinancialFactCreate]
    ) -> list[int]:
        """Insert multiple financial facts and return their IDs."""
        try:
            with self.engine.connect() as conn:
                fact_ids = []
                key_id_map = {}
                for fact in facts:
                    stmt = (
                        insert(self.financial_facts_table)
                        .values(
                            filing_id=fact.filing_id,
                            concept=fact.concept,
                            label=fact.label,
                            is_abstract=fact.is_abstract,
                            value=fact.value,
                            comparative_value=fact.comparative_value,
                            weight=fact.weight,
                            unit=fact.unit,
                            axis=fact.axis,
                            member=fact.member,
                            parsed_axis=fact.parsed_axis,
                            parsed_member=fact.parsed_member,
                            statement=fact.statement,
                            period_end=fact.period_end,
                            comparative_period_end=fact.comparative_period_end,
                            period=(
                                fact.period.value if fact.period is not None else None
                            ),
                            position=fact.position,
                        )
                        .returning(self.financial_facts_table.c.id)
                    )

                    result = conn.execute(stmt)
                    fact_id = result.scalar()
                    fact_ids.append(fact_id)
                    key_id_map[fact.key] = fact_id

                for fact in facts:
                    if fact.parent_key:
                        id = key_id_map.get(fact.key)
                        parent_id = key_id_map[fact.parent_key]

                        conn.execute(
                            update(self.financial_facts_table)
                            .where(self.financial_facts_table.c.id == id)
                            .values(parent_id=parent_id)
                        )

                conn.commit()
                logger.info(f"Inserted {len(fact_ids)} financial facts")
                return fact_ids

        except SQLAlchemyError as e:
            logger.error(f"Error inserting financial facts batch: {e}")
            return []

    def get_financial_facts_by_filing(self, filing_id: int) -> list[FinancialFact]:
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
                            parent_id=row.parent_id,
                            filing_id=row.filing_id,
                            concept=row.concept,
                            label=row.label,
                            is_abstract=row.is_abstract,
                            value=row.value,
                            comparative_value=row.comparative_value,
                            weight=row.weight,
                            unit=row.unit,
                            axis=row.axis,
                            member=row.member,
                            parsed_axis=row.parsed_axis,
                            parsed_member=row.parsed_member,
                            statement=row.statement,
                            period_end=row.period_end,
                            comparative_period_end=row.comparative_period_end,
                            period=(
                                PeriodType(row.period)
                                if row.period is not None
                                else None
                            ),
                            position=row.position,
                        )
                    )
                return facts

        except SQLAlchemyError as e:
            logger.error(f"Error getting financial facts by filing: {e}")
            return []

    def get_financial_facts_by_concept(
        self, company_id: int, concept: str, limit: int = 10
    ) -> list[FinancialFact]:
        """Get financial facts by company and concept."""
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
                        & (self.financial_facts_table.c.concept == concept)
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
                            parent_id=row.parent_id,
                            filing_id=row.filing_id,
                            concept=row.concept,
                            label=row.label,
                            is_abstract=row.is_abstract,
                            value=row.value,
                            comparative_value=row.comparative_value,
                            weight=row.weight,
                            unit=row.unit,
                            axis=row.axis,
                            member=row.member,
                            parsed_axis=row.parsed_axis,
                            parsed_member=row.parsed_member,
                            statement=row.statement,
                            period_end=row.period_end,
                            comparative_period_end=row.comparative_period_end,
                            period=(
                                PeriodType(row.period)
                                if row.period is not None
                                else None
                            ),
                            position=row.position,
                        )
                    )
                return facts

        except SQLAlchemyError as e:
            logger.error(f"Error getting financial facts by concept: {e}")
            return []

    def get_financial_facts_by_filing_id(self, filing_id: int) -> list[FinancialFact]:
        """Get all financial facts for a specific filing."""
        try:
            with self.engine.connect() as conn:
                stmt = select(self.financial_facts_table).where(
                    self.financial_facts_table.c.filing_id == filing_id
                )
                result = conn.execute(stmt)
                rows = result.fetchall()

                facts = []
                for row in rows:
                    fact = FinancialFact(
                        id=row.id,
                        parent_id=row.parent_id,
                        filing_id=row.filing_id,
                        concept=row.concept,
                        is_abstract=row.is_abstract,
                        label=row.label,
                        value=row.value,
                        comparative_value=row.comparative_value,
                        weight=row.weight,
                        unit=row.unit,
                        axis=row.axis,
                        member=row.member,
                        parsed_axis=row.parsed_axis,
                        parsed_member=row.parsed_member,
                        statement=row.statement,
                        period_end=row.period_end,
                        comparative_period_end=row.comparative_period_end,
                        period=(
                            PeriodType(row.period) if row.period is not None else None
                        ),
                        position=row.position,
                    )
                    facts.append(fact)

                logger.info(
                    f"Retrieved {len(facts)} financial facts for filing {filing_id}"
                )
                return facts

        except SQLAlchemyError as e:
            logger.error(
                f"Error retrieving financial facts for filing {filing_id}: {e}"
            )
            return []

    def delete_facts_by_filing_id(self, filing_id: int) -> bool:
        """Delete all financial facts for a specific filing.

        Args:
            filing_id: ID of the filing whose facts should be deleted

        Returns:
            True if deletion was successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                stmt = self.financial_facts_table.delete().where(
                    self.financial_facts_table.c.filing_id == filing_id
                )
                result = conn.execute(stmt)
                deleted_count = result.rowcount
                conn.commit()

                logger.info(
                    f"Deleted {deleted_count} financial facts for filing {filing_id}"
                )
                return True

        except SQLAlchemyError as e:
            logger.error(f"Error deleting financial facts for filing {filing_id}: {e}")
            return False
