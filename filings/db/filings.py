"""Async filing database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, insert, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from filings.models.filing import Filing, FilingCreate

logger = logging.getLogger(__name__)


class FilingOperationsAsync:
    """Async database operations for filings."""

    def __init__(self, engine: AsyncEngine, metadata: MetaData):
        """Initialize with async engine and metadata."""
        self.engine = engine
        self.filings_table = metadata.tables["filings"]

    async def insert_filing(self, filing: FilingCreate) -> Optional[int]:
        """Insert a new filing and return its ID."""
        try:
            async with self.engine.connect() as conn:
                stmt = (
                    insert(self.filings_table)
                    .values(
                        company_id=filing.company_id,
                        filing_entity_id=filing.filing_entity_id,
                        registry=filing.registry,
                        number=filing.number,
                        form_type=filing.form_type,
                        filing_date=filing.filing_date,
                        fiscal_period_end=filing.fiscal_period_end,
                        fiscal_year=filing.fiscal_year,
                        fiscal_quarter=filing.fiscal_quarter,
                        public_url=filing.public_url,
                    )
                    .returning(self.filings_table.c.id)
                )
                result = await conn.execute(stmt)
                filing_id = result.scalar()
                await conn.commit()
                logger.info(
                    f"Inserted filing: {filing.registry}:{filing.number} with ID: {filing_id}"
                )
                return filing_id
        except SQLAlchemyError as e:
            logger.error(f"Error inserting filing: {e}")
            return None

    async def get_filing_by_id(self, filing_id: int) -> Optional[Filing]:
        """Get filing by ID."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.filings_table).where(
                    self.filings_table.c.id == filing_id
                )
                result = await conn.execute(stmt)
                row = result.fetchone()
                if row:
                    return Filing(
                        id=row.id,
                        company_id=row.company_id,
                        filing_entity_id=row.filing_entity_id,
                        registry=row.registry,
                        number=row.number,
                        form_type=row.form_type,
                        filing_date=row.filing_date,
                        fiscal_period_end=row.fiscal_period_end,
                        fiscal_year=row.fiscal_year,
                        fiscal_quarter=row.fiscal_quarter,
                        public_url=row.public_url,
                    )
                return None
        except SQLAlchemyError as e:
            logger.error(f"Error getting filing by ID: {e}")
            return None

    async def get_filing_by_number(
        self, source: str, filing_number: str
    ) -> Optional[Filing]:
        """Get filing by registry and number."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.filings_table).where(
                    (self.filings_table.c.registry == source)
                    & (self.filings_table.c.number == filing_number)
                )
                result = await conn.execute(stmt)
                row = result.fetchone()
                if row:
                    return Filing(
                        id=row.id,
                        company_id=row.company_id,
                        filing_entity_id=row.filing_entity_id,
                        registry=row.registry,
                        number=row.number,
                        form_type=row.form_type,
                        filing_date=row.filing_date,
                        fiscal_period_end=row.fiscal_period_end,
                        fiscal_year=row.fiscal_year,
                        fiscal_quarter=row.fiscal_quarter,
                        public_url=row.public_url,
                    )
                return None
        except SQLAlchemyError as e:
            logger.error(f"Error getting filing by number: {e}")
            return None

    async def get_or_create_filing(self, filing: FilingCreate) -> Optional[Filing]:
        """Get existing filing or create new one."""
        existing = await self.get_filing_by_number(filing.registry, filing.number)
        if existing:
            return existing
        filing_id = await self.insert_filing(filing)
        if filing_id:
            return await self.get_filing_by_id(filing_id)
        return None

    async def delete_filing(self, filing_id: int) -> bool:
        """Delete a filing by ID."""
        try:
            async with self.engine.connect() as conn:
                stmt = self.filings_table.delete().where(
                    self.filings_table.c.id == filing_id
                )
                result = await conn.execute(stmt)
                deleted_count = result.rowcount
                await conn.commit()
                if deleted_count > 0:
                    logger.info(f"Deleted filing with ID {filing_id}")
                    return True
                return False
        except SQLAlchemyError as e:
            logger.error(f"Error deleting filing {filing_id}: {e}")
            return False

    async def get_filings_by_company(
        self, company_id: int, form_type: Optional[str] = None
    ) -> List[Filing]:
        """Get filings by company ID and optionally form type."""
        try:
            async with self.engine.connect() as conn:
                if form_type:
                    stmt = (
                        select(self.filings_table)
                        .where(
                            (self.filings_table.c.company_id == company_id)
                            & (self.filings_table.c.form_type == form_type)
                        )
                        .order_by(self.filings_table.c.filing_date.desc())
                    )
                else:
                    stmt = (
                        select(self.filings_table)
                        .where(self.filings_table.c.company_id == company_id)
                        .order_by(self.filings_table.c.filing_date.desc())
                    )

                result = await conn.execute(stmt)

                filings = []
                for row in result:
                    filings.append(
                        Filing(
                            id=row.id,
                            company_id=row.company_id,
                            filing_entity_id=row.filing_entity_id,
                            registry=row.registry,
                            number=row.number,
                            form_type=row.form_type,
                            filing_date=row.filing_date,
                            fiscal_period_end=row.fiscal_period_end,
                            fiscal_year=row.fiscal_year,
                            fiscal_quarter=row.fiscal_quarter,
                            public_url=row.public_url,
                        )
                    )
                return filings

        except SQLAlchemyError as e:
            logger.error(f"Error getting filings by company: {e}")
            return []

    async def align_january_year_end(self, company_id: int) -> int:
        """Move a January year-end's interim quarters onto the year they close.

        April, July and October belong to the fiscal year that ends the
        following January. January 10-Ks already carry that year. Other
        year-ends are left as stored.

        Args:
            company_id: Company whose filings to realign.

        Returns:
            Number of filings whose fiscal year changed.
        """
        try:
            async with self.engine.begin() as conn:
                year_end = await conn.execute(
                    text(
                        """
                        SELECT MODE() WITHIN GROUP (
                            ORDER BY EXTRACT(MONTH FROM fiscal_period_end)::int
                        )
                        FROM filings
                        WHERE company_id = :company_id
                          AND form_type IN ('10-K', '10-K/A')
                          AND fiscal_period_end IS NOT NULL
                        """
                    ),
                    {"company_id": company_id},
                )
                if year_end.scalar() != 1:
                    return 0
                result = await conn.execute(
                    text(
                        """
                        UPDATE filings
                        SET fiscal_year = EXTRACT(YEAR FROM fiscal_period_end)::int + 1
                        WHERE company_id = :company_id
                          AND form_type IN ('10-Q', '10-Q/A')
                          AND fiscal_period_end IS NOT NULL
                          AND EXTRACT(MONTH FROM fiscal_period_end) NOT IN (1, 2)
                          AND fiscal_year IS DISTINCT FROM
                              EXTRACT(YEAR FROM fiscal_period_end)::int + 1
                        """
                    ),
                    {"company_id": company_id},
                )
                return result.rowcount or 0
        except SQLAlchemyError as e:
            logger.error("Error aligning January year-end for %s: %s", company_id, e)
            return 0
