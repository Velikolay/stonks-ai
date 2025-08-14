"""Filing database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, Table, insert, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ..models.base import Filing, FilingCreate

logger = logging.getLogger(__name__)


class FilingOperations:
    """Filing database operations."""

    def __init__(self, engine: Engine):
        """Initialize with database engine."""
        self.engine = engine
        # Create table metadata
        metadata = MetaData()
        self.filings_table = Table("filings", metadata, autoload_with=engine)

    def insert_filing(self, filing: FilingCreate) -> Optional[int]:
        """Insert a new filing and return its ID."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    insert(self.filings_table)
                    .values(
                        company_id=filing.company_id,
                        source=filing.source,
                        filing_number=filing.filing_number,
                        form_type=filing.form_type,
                        filing_date=filing.filing_date,
                        fiscal_period_end=filing.fiscal_period_end,
                        fiscal_year=filing.fiscal_year,
                        fiscal_quarter=filing.fiscal_quarter,
                        public_url=filing.public_url,
                    )
                    .returning(self.filings_table.c.id)
                )

                result = conn.execute(stmt)
                filing_id = result.scalar()
                conn.commit()

                logger.info(
                    f"Inserted filing: {filing.filing_number} with ID: {filing_id}"
                )
                return filing_id

        except SQLAlchemyError as e:
            logger.error(f"Error inserting filing: {e}")
            return None

    def get_filing_by_id(self, filing_id: int) -> Optional[Filing]:
        """Get filing by ID."""
        try:
            with self.engine.connect() as conn:
                stmt = select(self.filings_table).where(
                    self.filings_table.c.id == filing_id
                )

                result = conn.execute(stmt)
                row = result.fetchone()

                if row:
                    return Filing(
                        id=row.id,
                        company_id=row.company_id,
                        source=row.source,
                        filing_number=row.filing_number,
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

    def get_filings_by_company(
        self, company_id: int, form_type: Optional[str] = None
    ) -> List[Filing]:
        """Get filings by company ID and optionally form type."""
        try:
            with self.engine.connect() as conn:
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

                result = conn.execute(stmt)

                filings = []
                for row in result:
                    filings.append(
                        Filing(
                            id=row.id,
                            company_id=row.company_id,
                            source=row.source,
                            filing_number=row.filing_number,
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

    def get_latest_filing(self, company_id: int, form_type: str) -> Optional[Filing]:
        """Get the latest filing for a company and form type."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    select(self.filings_table)
                    .where(
                        (self.filings_table.c.company_id == company_id)
                        & (self.filings_table.c.form_type == form_type)
                    )
                    .order_by(self.filings_table.c.filing_date.desc())
                    .limit(1)
                )

                result = conn.execute(stmt)
                row = result.fetchone()

                if row:
                    return Filing(
                        id=row.id,
                        company_id=row.company_id,
                        source=row.source,
                        filing_number=row.filing_number,
                        form_type=row.form_type,
                        filing_date=row.filing_date,
                        fiscal_period_end=row.fiscal_period_end,
                        fiscal_year=row.fiscal_year,
                        fiscal_quarter=row.fiscal_quarter,
                        public_url=row.public_url,
                    )
                return None

        except SQLAlchemyError as e:
            logger.error(f"Error getting latest filing: {e}")
            return None

    def get_filing_by_number(self, source: str, filing_number: str) -> Optional[Filing]:
        """Get filing by source and filing number."""
        try:
            with self.engine.connect() as conn:
                stmt = select(self.filings_table).where(
                    (self.filings_table.c.source == source)
                    & (self.filings_table.c.filing_number == filing_number)
                )

                result = conn.execute(stmt)
                row = result.fetchone()

                if row:
                    return Filing(
                        id=row.id,
                        company_id=row.company_id,
                        source=row.source,
                        filing_number=row.filing_number,
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

    def get_or_create_filing(self, filing: FilingCreate) -> Optional[Filing]:
        """Get existing filing or create new one."""
        # Try to find existing filing
        existing = self.get_filing_by_number(filing.source, filing.filing_number)
        if existing:
            return existing

        # Create new filing
        filing_id = self.insert_filing(filing)
        if filing_id:
            return self.get_filing_by_id(filing_id)

        return None
