"""Company database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, Table, insert, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ..models import Company, CompanyCreate

logger = logging.getLogger(__name__)


class CompanyOperations:
    """Company database operations."""

    def __init__(self, engine: Engine):
        """Initialize with database engine."""
        self.engine = engine
        # Create table metadata
        metadata = MetaData()
        self.companies_table = Table("companies", metadata, autoload_with=engine)

    def insert_company(self, company: CompanyCreate) -> Optional[int]:
        """Insert a new company and return its ID."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    insert(self.companies_table)
                    .values(
                        ticker=company.ticker,
                        exchange=company.exchange,
                        name=company.name,
                    )
                    .returning(self.companies_table.c.id)
                )

                result = conn.execute(stmt)
                company_id = result.scalar()
                conn.commit()

                logger.info(f"Inserted company: {company.name} with ID: {company_id}")
                return company_id

        except SQLAlchemyError as e:
            logger.error(f"Error inserting company: {e}")
            return None

    def get_company_by_id(self, company_id: int) -> Optional[Company]:
        """Get company by ID."""
        try:
            with self.engine.connect() as conn:
                stmt = select(self.companies_table).where(
                    self.companies_table.c.id == company_id
                )

                result = conn.execute(stmt)
                row = result.fetchone()

                if row:
                    return Company(
                        id=row.id,
                        ticker=row.ticker,
                        exchange=row.exchange,
                        name=row.name,
                    )
                return None

        except SQLAlchemyError as e:
            logger.error(f"Error getting company by ID: {e}")
            return None

    def get_company_by_ticker(
        self, ticker: str, exchange: Optional[str] = None
    ) -> Optional[Company]:
        """Get company by ticker and optionally exchange."""
        try:
            with self.engine.connect() as conn:
                if exchange:
                    stmt = select(self.companies_table).where(
                        (self.companies_table.c.ticker == ticker)
                        & (self.companies_table.c.exchange == exchange)
                    )
                else:
                    stmt = select(self.companies_table).where(
                        self.companies_table.c.ticker == ticker
                    )

                result = conn.execute(stmt)
                row = result.fetchone()

                if row:
                    return Company(
                        id=row.id,
                        ticker=row.ticker,
                        exchange=row.exchange,
                        name=row.name,
                    )
                return None

        except SQLAlchemyError as e:
            logger.error(f"Error getting company by ticker: {e}")
            return None

    def get_all_companies(self) -> List[Company]:
        """Get all companies."""
        try:
            with self.engine.connect() as conn:
                stmt = select(self.companies_table)
                result = conn.execute(stmt)

                companies = []
                for row in result:
                    companies.append(
                        Company(
                            id=row.id,
                            ticker=row.ticker,
                            exchange=row.exchange,
                            name=row.name,
                        )
                    )
                return companies

        except SQLAlchemyError as e:
            logger.error(f"Error getting all companies: {e}")
            return []

    def get_or_create_company(self, company: CompanyCreate) -> Optional[Company]:
        """Get existing company or create new one."""
        # Try to find existing company
        existing = self.get_company_by_ticker(company.ticker, company.exchange)
        if existing:
            return existing

        # Create new company
        company_id = self.insert_company(company)
        if company_id:
            return self.get_company_by_id(company_id)

        return None
