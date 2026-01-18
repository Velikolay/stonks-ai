"""Company database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, Table, insert, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ..models import Company, CompanyCreate, FilingEntity

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
        self.filing_entities_table = Table(
            "filing_entities", metadata, autoload_with=engine
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

    def get_or_create_filing_entities_id(
        self,
        *,
        company_id: int,
        registry: str,
        number: str,
        status: str = "active",
    ) -> Optional[int]:
        """Get or create a filing_entities row and return its ID.

        This is a company-owned record (similar to tickers):
        - For SEC, number should be the company's CIK.
        - Repeated calls are safe and will not create duplicates.
        """
        try:
            with self.engine.connect() as conn:
                existing = conn.execute(
                    select(
                        self.filing_entities_table.c.id,
                        self.filing_entities_table.c.company_id,
                    ).where(
                        (self.filing_entities_table.c.registry == registry)
                        & (self.filing_entities_table.c.number == number)
                    )
                ).fetchone()

                if existing is not None:
                    if int(existing.company_id) != int(company_id):
                        logger.error(
                            "filing_entities mismatch for %s:%s (existing company_id=%s, wanted=%s)",
                            registry,
                            number,
                            existing.company_id,
                            company_id,
                        )
                        return None
                    return int(existing.id)

                insert_stmt = (
                    insert(self.filing_entities_table)
                    .values(
                        registry=registry,
                        number=number,
                        status=status,
                        company_id=company_id,
                    )
                    .returning(self.filing_entities_table.c.id)
                )
                new_id = conn.execute(insert_stmt).scalar()
                conn.commit()
                return int(new_id) if new_id is not None else None
        except SQLAlchemyError as e:
            logger.error(f"Error getting/creating filing_entities: {e}")
            return None

    def get_filing_entities_by_company_id(
        self,
        *,
        company_id: int,
        registry: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[FilingEntity]:
        """Get all filing_entities records for a company.

        Args:
            company_id: Company ID.
            registry: Optional registry filter (e.g. "SEC").
            status: Optional status filter (e.g. "active").

        Returns:
            List of FilingEntity models.
        """
        try:
            with self.engine.connect() as conn:
                stmt = select(
                    self.filing_entities_table.c.id,
                    self.filing_entities_table.c.registry,
                    self.filing_entities_table.c.number,
                    self.filing_entities_table.c.status,
                    self.filing_entities_table.c.company_id,
                ).where(self.filing_entities_table.c.company_id == company_id)

                if registry is not None:
                    stmt = stmt.where(self.filing_entities_table.c.registry == registry)
                if status is not None:
                    stmt = stmt.where(self.filing_entities_table.c.status == status)

                rows = conn.execute(stmt).fetchall()
                return [
                    FilingEntity(
                        id=int(r.id),
                        registry=str(r.registry),
                        number=str(r.number),
                        status=str(r.status),
                        company_id=int(r.company_id),
                    )
                    for r in rows
                ]
        except SQLAlchemyError as e:
            logger.error(
                "Error getting filing_entities records for company_id=%s: %s",
                company_id,
                e,
            )
            return []
