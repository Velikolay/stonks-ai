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
        self.tickers_table = Table("tickers", metadata, autoload_with=engine)
        self.filing_registry_table = Table(
            "filing_registry", metadata, autoload_with=engine
        )

    def insert_company(self, company: CompanyCreate) -> Optional[int]:
        """Insert a new company and return its ID."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    insert(self.companies_table)
                    .values(name=company.name, industry=company.industry)
                    .returning(self.companies_table.c.id)
                )
                company_id = conn.execute(stmt).scalar()
                if company_id is None:
                    conn.rollback()
                    return None

                conn.commit()
                logger.info(f"Inserted company: {company.name} with ID: {company_id}")
                return int(company_id)

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
                        name=row.name,
                        industry=getattr(row, "industry", None),
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
                if exchange is not None:
                    ticker_stmt = select(self.tickers_table).where(
                        (self.tickers_table.c.ticker == ticker)
                        & (self.tickers_table.c.exchange == exchange)
                    )
                else:
                    ticker_stmt = select(self.tickers_table).where(
                        self.tickers_table.c.ticker == ticker
                    )
                ticker_row = conn.execute(ticker_stmt).fetchone()
                if ticker_row is None:
                    return None

                company_row = conn.execute(
                    select(self.companies_table).where(
                        self.companies_table.c.id == ticker_row.company_id
                    )
                ).fetchone()
                if company_row is None:
                    return None

                return Company(
                    id=company_row.id,
                    name=company_row.name,
                    industry=getattr(company_row, "industry", None),
                )

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
                            name=row.name,
                            industry=getattr(row, "industry", None),
                        )
                    )
                return companies

        except SQLAlchemyError as e:
            logger.error(f"Error getting all companies: {e}")
            return []

    def get_or_create_company(self, company: CompanyCreate) -> Optional[Company]:
        """Create a company row in `companies`.

        Note:
            With the new schema, ticker identity lives in `tickers`, not `companies`.
        """
        company_id = self.insert_company(company)
        if company_id is None:
            return None
        return self.get_company_by_id(company_id)

    def upsert_ticker(
        self,
        *,
        company_id: int,
        ticker: str,
        exchange: str,
        status: str = "active",
    ) -> bool:
        """Create a (ticker, exchange) -> company mapping if it doesn't exist.

        Returns:
            True if the mapping exists (created or already present), False otherwise.
        """
        try:
            with self.engine.connect() as conn:
                existing = conn.execute(
                    select(
                        self.tickers_table.c.id, self.tickers_table.c.company_id
                    ).where(
                        (self.tickers_table.c.ticker == ticker)
                        & (self.tickers_table.c.exchange == exchange)
                    )
                ).fetchone()
                if existing is not None:
                    if int(existing.company_id) != int(company_id):
                        logger.warning(
                            "Ticker %s on %s already mapped to company_id=%s (wanted %s)",
                            ticker,
                            exchange,
                            existing.company_id,
                            company_id,
                        )
                        return False
                    return True

                conn.execute(
                    insert(self.tickers_table).values(
                        ticker=ticker,
                        exchange=exchange,
                        status=status,
                        company_id=int(company_id),
                    )
                )
                conn.commit()
                return True
        except SQLAlchemyError as e:
            logger.error(f"Error upserting ticker {ticker} ({exchange}): {e}")
            return False

    def get_or_create_filing_registry_id(
        self,
        *,
        company_id: int,
        registry: str,
        number: str,
        status: str = "active",
    ) -> Optional[int]:
        """Get or create a filing_registry row and return its ID.

        This is a company-owned record (similar to tickers):
        - For SEC, number should be the company's CIK.
        - Repeated calls are safe and will not create duplicates.
        """
        try:
            with self.engine.connect() as conn:
                existing = conn.execute(
                    select(
                        self.filing_registry_table.c.id,
                        self.filing_registry_table.c.company_id,
                    ).where(
                        (self.filing_registry_table.c.registry == registry)
                        & (self.filing_registry_table.c.number == number)
                    )
                ).fetchone()

                if existing is not None:
                    if int(existing.company_id) != int(company_id):
                        logger.error(
                            "filing_registry mismatch for %s:%s (existing company_id=%s, wanted=%s)",
                            registry,
                            number,
                            existing.company_id,
                            company_id,
                        )
                        return None
                    return int(existing.id)

                insert_stmt = (
                    insert(self.filing_registry_table)
                    .values(
                        registry=registry,
                        number=number,
                        status=status,
                        company_id=company_id,
                    )
                    .returning(self.filing_registry_table.c.id)
                )
                new_id = conn.execute(insert_stmt).scalar()
                conn.commit()
                return int(new_id) if new_id is not None else None
        except SQLAlchemyError as e:
            logger.error(f"Error getting/creating filing_registry: {e}")
            return None
