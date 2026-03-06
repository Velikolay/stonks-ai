"""Async financial facts database operations."""

import logging
from typing import Optional

from sqlalchemy import MetaData, insert, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from filings.models import FinancialFact, FinancialFactCreate, PeriodType

logger = logging.getLogger(__name__)


class FinancialFactOperationsAsync:
    """Async financial facts database operations."""

    def __init__(self, engine: AsyncEngine, metadata: MetaData):
        """Initialize with async engine and metadata."""
        self.engine = engine
        self.financial_facts_table = metadata.tables["financial_facts"]
        self.filings_table = metadata.tables["filings"]

    async def insert_financial_fact(self, fact: FinancialFact) -> Optional[int]:
        """Insert a new financial fact and return its ID."""
        try:
            async with self.engine.connect() as conn:
                stmt = (
                    insert(self.financial_facts_table)
                    .values(
                        parent_id=fact.parent_id,
                        abstract_id=fact.abstract_id,
                        company_id=fact.company_id,
                        filing_id=fact.filing_id,
                        form_type=fact.form_type,
                        concept=fact.concept,
                        label=fact.label,
                        is_abstract=fact.is_abstract,
                        value=fact.value,
                        comparative_value=fact.comparative_value,
                        weight=fact.weight,
                        unit=fact.unit,
                        axis=fact.axis if fact.axis is not None else "",
                        member=fact.member if fact.member is not None else "",
                        member_label=(
                            fact.member_label if fact.member_label is not None else ""
                        ),
                        statement=fact.statement if fact.statement is not None else "",
                        period_end=fact.period_end,
                        comparative_period_end=fact.comparative_period_end,
                        period=fact.period.value if fact.period is not None else None,
                        position=fact.position,
                    )
                    .returning(self.financial_facts_table.c.id)
                )

                result = await conn.execute(stmt)
                fact_id = result.scalar()
                if fact_id is None:
                    await conn.rollback()
                    return None

                await conn.commit()
                logger.info(f"Inserted financial fact with ID: {fact_id}")
                return fact_id

        except SQLAlchemyError as e:
            logger.error(f"Error inserting financial fact: {e}")
            return None

    async def insert_financial_facts_batch(
        self, facts: list[FinancialFactCreate]
    ) -> list[int]:
        """Insert multiple financial facts and return their IDs."""
        try:
            async with self.engine.connect() as conn:
                fact_ids = []
                key_id_map = {}
                for fact in facts:
                    stmt = (
                        insert(self.financial_facts_table)
                        .values(
                            company_id=fact.company_id,
                            filing_id=fact.filing_id,
                            form_type=fact.form_type,
                            concept=fact.concept,
                            label=fact.label,
                            is_abstract=fact.is_abstract,
                            value=fact.value,
                            comparative_value=fact.comparative_value,
                            weight=fact.weight,
                            unit=fact.unit,
                            axis=fact.axis if fact.axis is not None else "",
                            member=fact.member if fact.member is not None else "",
                            member_label=(
                                fact.member_label
                                if fact.member_label is not None
                                else ""
                            ),
                            statement=(
                                fact.statement if fact.statement is not None else ""
                            ),
                            period_end=fact.period_end,
                            comparative_period_end=fact.comparative_period_end,
                            period=(
                                fact.period.value if fact.period is not None else None
                            ),
                            position=fact.position,
                        )
                        .returning(self.financial_facts_table.c.id)
                    )

                    result = await conn.execute(stmt)
                    fact_id = result.scalar()
                    fact_ids.append(fact_id)
                    key_id_map[fact.key] = fact_id

                for fact in facts:
                    if fact.abstract_key or fact.parent_key:
                        id_val = key_id_map.get(fact.key)
                        parent_id = key_id_map.get(fact.parent_key)
                        abstract_id = key_id_map.get(fact.abstract_key)

                        await conn.execute(
                            update(self.financial_facts_table)
                            .where(self.financial_facts_table.c.id == id_val)
                            .values(parent_id=parent_id, abstract_id=abstract_id)
                        )

                await conn.commit()
                logger.info(f"Inserted {len(fact_ids)} financial facts")
                return fact_ids

        except SQLAlchemyError as e:
            logger.error(f"Error inserting financial facts batch: {e}")
            return []

    async def get_financial_facts_by_filing(
        self, filing_id: int
    ) -> list[FinancialFact]:
        """Get all financial facts for a filing."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.financial_facts_table).where(
                    self.financial_facts_table.c.filing_id == filing_id
                )

                result = await conn.execute(stmt)

                facts = []
                for row in result:
                    facts.append(
                        FinancialFact(
                            id=row.id,
                            parent_id=row.parent_id,
                            abstract_id=row.abstract_id,
                            company_id=row.company_id,
                            filing_id=row.filing_id,
                            form_type=row.form_type,
                            concept=row.concept,
                            label=row.label,
                            is_abstract=row.is_abstract,
                            value=row.value,
                            comparative_value=row.comparative_value,
                            weight=row.weight,
                            unit=row.unit,
                            axis=row.axis if row.axis else None,
                            member=row.member if row.member else None,
                            member_label=row.member_label if row.member_label else None,
                            statement=row.statement if row.statement else None,
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

    async def get_financial_facts_by_concept(
        self, company_id: int, concept: str, limit: int = 10
    ) -> list[FinancialFact]:
        """Get financial facts by company and concept."""
        try:
            async with self.engine.connect() as conn:
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

                result = await conn.execute(stmt)

                facts = []
                for row in result:
                    facts.append(
                        FinancialFact(
                            id=row.id,
                            parent_id=row.parent_id,
                            abstract_id=row.abstract_id,
                            company_id=row.company_id,
                            filing_id=row.filing_id,
                            form_type=row.form_type,
                            concept=row.concept,
                            label=row.label,
                            is_abstract=row.is_abstract,
                            value=row.value,
                            comparative_value=row.comparative_value,
                            weight=row.weight,
                            unit=row.unit,
                            axis=row.axis if row.axis else None,
                            member=row.member if row.member else None,
                            member_label=row.member_label if row.member_label else None,
                            statement=row.statement if row.statement else None,
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

    async def get_financial_facts_by_filing_id(
        self, filing_id: int
    ) -> list[FinancialFact]:
        """Get all financial facts for a specific filing."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.financial_facts_table).where(
                    self.financial_facts_table.c.filing_id == filing_id
                )
                result = await conn.execute(stmt)
                rows = result.fetchall()

                facts = []
                for row in rows:
                    fact = FinancialFact(
                        id=row.id,
                        parent_id=row.parent_id,
                        abstract_id=row.abstract_id,
                        company_id=row.company_id,
                        filing_id=row.filing_id,
                        form_type=row.form_type,
                        concept=row.concept,
                        is_abstract=row.is_abstract,
                        label=row.label,
                        value=row.value,
                        comparative_value=row.comparative_value,
                        weight=row.weight,
                        unit=row.unit,
                        axis=row.axis if row.axis else None,
                        member=row.member if row.member else None,
                        member_label=row.member_label if row.member_label else None,
                        statement=row.statement if row.statement else None,
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

    async def delete_facts_by_filing_id(self, filing_id: int) -> bool:
        """Delete all financial facts for a specific filing."""
        try:
            async with self.engine.connect() as conn:
                stmt = self.financial_facts_table.delete().where(
                    self.financial_facts_table.c.filing_id == filing_id
                )
                result = await conn.execute(stmt)
                deleted_count = result.rowcount
                await conn.commit()

                logger.info(
                    f"Deleted {deleted_count} financial facts for filing {filing_id}"
                )
                return True

        except SQLAlchemyError as e:
            logger.error(f"Error deleting financial facts for filing {filing_id}: {e}")
            return False
