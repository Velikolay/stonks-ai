"""Company database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, Table, delete, func, insert, literal, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import union_all

from ..models import (
    Company,
    CompanyCreate,
    CompanySearch,
    CompanyUpdate,
    FilingEntity,
    FilingEntityCreate,
    FilingEntityUpdate,
    Ticker,
    TickerCreate,
    TickerUpdate,
)

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
                        name=row.name,
                        industry=getattr(row, "industry", None),
                    )
                return None

        except SQLAlchemyError as e:
            logger.error(f"Error getting company by ID: {e}")
            return None

    def get_companies_by_ids(self, *, company_ids: List[int]) -> List[Company]:
        """Get companies for multiple IDs in one query."""
        if not company_ids:
            return []

        try:
            with self.engine.connect() as conn:
                stmt = select(self.companies_table).where(
                    self.companies_table.c.id.in_(company_ids)
                )
                rows = conn.execute(stmt).fetchall()
                return [
                    Company(
                        id=int(r.id),
                        name=str(r.name),
                        industry=getattr(r, "industry", None),
                    )
                    for r in rows
                ]
        except SQLAlchemyError as e:
            logger.error(
                "Error getting companies for company_ids=%s: %s", company_ids, e
            )
            return []

    def search_companies_by_prefix(
        self, *, prefix: str, limit: int = 20
    ) -> List[CompanySearch]:
        """Search companies by name or ticker prefix (case-insensitive).

        This follows the Postgres pattern:
        - UNION ALL name matches (rank=1, ticker=NULL) with ticker matches (rank=0)
        - DISTINCT ON (company_id) with ORDER BY company_id, rank, ticker
          so ticker matches win, and the "first" ticker (alphabetical) is chosen.
        """

        def _escape_like(s: str) -> str:
            """Escape LIKE wildcards so user input is treated literally."""
            return s.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")

        normalized = prefix.strip().lower()
        if not normalized:
            return []

        # Keep the user's prefix semantics, but escape LIKE metacharacters.
        like_pattern = f"{_escape_like(normalized)}%"
        limit = max(1, min(int(limit), 20))

        try:
            with self.engine.connect() as conn:
                name_rows = (
                    select(
                        self.companies_table.c.id.label("company_id"),
                        self.companies_table.c.name.label("company_name"),
                        self.tickers_table.c.ticker.label("ticker"),
                        literal(1).label("rank"),
                    )
                    .select_from(
                        self.companies_table.join(
                            self.tickers_table,
                            self.tickers_table.c.company_id
                            == self.companies_table.c.id,
                        )
                    )
                    .where(
                        func.lower(self.companies_table.c.name).like(
                            like_pattern, escape="\\"
                        )
                    )
                )

                ticker_rows = (
                    select(
                        self.companies_table.c.id.label("company_id"),
                        self.companies_table.c.name.label("company_name"),
                        self.tickers_table.c.ticker.label("ticker"),
                        literal(0).label("rank"),
                    )
                    .select_from(
                        self.companies_table.join(
                            self.tickers_table,
                            self.tickers_table.c.company_id
                            == self.companies_table.c.id,
                        )
                    )
                    .where(
                        func.lower(self.tickers_table.c.ticker).like(
                            like_pattern, escape="\\"
                        )
                    )
                )

                s = union_all(name_rows, ticker_rows).subquery()

                stmt = (
                    select(
                        s.c.company_id,
                        s.c.company_name,
                        s.c.ticker,
                    )
                    # Postgres DISTINCT ON(company_id)
                    .distinct(s.c.company_id)
                    .order_by(s.c.company_id, s.c.rank, s.c.ticker)
                    .limit(limit)
                )
                rows = conn.execute(stmt).fetchall()
                return [
                    CompanySearch(
                        id=int(r.company_id),
                        name=str(r.company_name),
                        ticker=str(r.ticker) if r.ticker is not None else None,
                    )
                    for r in rows
                ]
        except SQLAlchemyError as e:
            logger.error("Error searching companies by prefix=%s: %s", prefix, e)
            return []

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

    def update_company(
        self, *, company_id: int, company: CompanyUpdate
    ) -> Optional[Company]:
        """Update company fields and return the updated company."""
        values = {}
        if company.name is not None:
            values["name"] = company.name
        if company.industry is not None:
            values["industry"] = company.industry

        if not values:
            return self.get_company_by_id(company_id)

        try:
            with self.engine.connect() as conn:
                res = conn.execute(
                    update(self.companies_table)
                    .where(self.companies_table.c.id == company_id)
                    .values(**values)
                )
                if res.rowcount == 0:
                    conn.rollback()
                    return None
                conn.commit()
                return self.get_company_by_id(company_id)
        except SQLAlchemyError as e:
            logger.error("Error updating company_id=%s: %s", company_id, e)
            return None

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
                    if existing.company_id != company_id:
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
                        company_id=company_id,
                    )
                )
                conn.commit()
                return True
        except SQLAlchemyError as e:
            logger.error(f"Error upserting ticker {ticker} ({exchange}): {e}")
            return False

    def get_tickers_by_company_id(
        self,
        *,
        company_id: int,
        status: Optional[str] = None,
    ) -> List[Ticker]:
        """Get all tickers for a company."""
        try:
            with self.engine.connect() as conn:
                stmt = select(
                    self.tickers_table.c.id,
                    self.tickers_table.c.ticker,
                    self.tickers_table.c.exchange,
                    self.tickers_table.c.status,
                    self.tickers_table.c.company_id,
                ).where(self.tickers_table.c.company_id == company_id)

                if status is not None:
                    stmt = stmt.where(self.tickers_table.c.status == status)

                rows = conn.execute(stmt).fetchall()
                return [
                    Ticker(
                        id=int(r.id),
                        ticker=str(r.ticker),
                        exchange=str(r.exchange),
                        status=str(r.status),
                        company_id=int(r.company_id),
                    )
                    for r in rows
                ]
        except SQLAlchemyError as e:
            logger.error("Error getting tickers for company_id=%s: %s", company_id, e)
            return []

    def get_tickers_by_company_ids(
        self,
        *,
        company_ids: List[int],
        status: Optional[str] = None,
    ) -> dict[int, List[Ticker]]:
        """Get tickers for multiple companies in one query.

        Returns:
            Mapping of company_id -> list of tickers.
        """
        if not company_ids:
            return {}

        try:
            with self.engine.connect() as conn:
                stmt = select(
                    self.tickers_table.c.id,
                    self.tickers_table.c.ticker,
                    self.tickers_table.c.exchange,
                    self.tickers_table.c.status,
                    self.tickers_table.c.company_id,
                ).where(self.tickers_table.c.company_id.in_(company_ids))

                if status is not None:
                    stmt = stmt.where(self.tickers_table.c.status == status)

                rows = conn.execute(stmt).fetchall()
                grouped: dict[int, List[Ticker]] = {}
                for r in rows:
                    cid = int(r.company_id)
                    grouped.setdefault(cid, []).append(
                        Ticker(
                            id=int(r.id),
                            ticker=str(r.ticker),
                            exchange=str(r.exchange),
                            status=str(r.status),
                            company_id=cid,
                        )
                    )
                return grouped
        except SQLAlchemyError as e:
            logger.error("Error getting tickers for company_ids=%s: %s", company_ids, e)
            return {}

    def create_ticker(
        self, *, company_id: int, ticker: TickerCreate
    ) -> Optional[Ticker]:
        """Create a ticker mapping for a company, returning the created (or existing) row."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    insert(self.tickers_table)
                    .values(
                        ticker=ticker.ticker,
                        exchange=ticker.exchange,
                        status=ticker.status,
                        company_id=company_id,
                    )
                    .returning(
                        self.tickers_table.c.id,
                        self.tickers_table.c.ticker,
                        self.tickers_table.c.exchange,
                        self.tickers_table.c.status,
                        self.tickers_table.c.company_id,
                    )
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return None

                conn.commit()
                return Ticker(
                    id=int(row.id),
                    ticker=str(row.ticker),
                    exchange=str(row.exchange),
                    status=str(row.status),
                    company_id=int(row.company_id),
                )
        except SQLAlchemyError as e:
            logger.error("Error creating ticker for company_id=%s: %s", company_id, e)
            return None

    def update_ticker(
        self,
        *,
        company_id: int,
        ticker_id: int,
        ticker: TickerUpdate,
    ) -> Optional[Ticker]:
        """Update a ticker mapping for a company."""
        values = {}
        if ticker.ticker is not None:
            values["ticker"] = ticker.ticker
        if ticker.exchange is not None:
            values["exchange"] = ticker.exchange
        if ticker.status is not None:
            values["status"] = ticker.status

        if not values:
            return self._get_ticker_by_id(company_id=company_id, ticker_id=ticker_id)

        try:
            with self.engine.connect() as conn:
                res = conn.execute(
                    update(self.tickers_table)
                    .where(
                        (self.tickers_table.c.id == ticker_id)
                        & (self.tickers_table.c.company_id == company_id)
                    )
                    .values(**values)
                )
                if res.rowcount == 0:
                    conn.rollback()
                    return None
                conn.commit()
                return self._get_ticker_by_id(
                    company_id=company_id, ticker_id=ticker_id
                )
        except SQLAlchemyError as e:
            logger.error(
                "Error updating ticker_id=%s for company_id=%s: %s",
                ticker_id,
                company_id,
                e,
            )
            return None

    def delete_ticker(self, *, company_id: int, ticker_id: int) -> bool:
        """Delete a ticker mapping for a company."""
        try:
            with self.engine.connect() as conn:
                res = conn.execute(
                    delete(self.tickers_table).where(
                        (self.tickers_table.c.id == ticker_id)
                        & (self.tickers_table.c.company_id == company_id)
                    )
                )
                if res.rowcount == 0:
                    conn.rollback()
                    return False
                conn.commit()
                return True
        except SQLAlchemyError as e:
            logger.error(
                "Error deleting ticker_id=%s for company_id=%s: %s",
                ticker_id,
                company_id,
                e,
            )
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
                    if existing.company_id != company_id:
                        logger.error(
                            "filing_entities mismatch for %s:%s (existing company_id=%s, wanted=%s)",
                            registry,
                            number,
                            existing.company_id,
                            company_id,
                        )
                        return None
                    return existing.id

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

    def create_filing_entity(
        self, *, company_id: int, filing_entity: FilingEntityCreate
    ) -> Optional[FilingEntity]:
        """Create a filing entity for a company."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    insert(self.filing_entities_table)
                    .values(
                        registry=filing_entity.registry,
                        number=filing_entity.number,
                        status=filing_entity.status,
                        company_id=company_id,
                    )
                    .returning(
                        self.filing_entities_table.c.id,
                        self.filing_entities_table.c.registry,
                        self.filing_entities_table.c.number,
                        self.filing_entities_table.c.status,
                        self.filing_entities_table.c.company_id,
                    )
                ).fetchone()
                if row is None:
                    conn.rollback()
                    return None

                conn.commit()
                return FilingEntity(
                    id=int(row.id),
                    registry=str(row.registry),
                    number=str(row.number),
                    status=str(row.status),
                    company_id=int(row.company_id),
                )
        except SQLAlchemyError as e:
            logger.error(
                "Error creating filing_entity for company_id=%s: %s", company_id, e
            )
            return None

    def update_filing_entity(
        self,
        *,
        company_id: int,
        filing_entity_id: int,
        filing_entity: FilingEntityUpdate,
    ) -> Optional[FilingEntity]:
        """Update a filing entity for a company."""
        values = {}
        if filing_entity.registry is not None:
            values["registry"] = filing_entity.registry
        if filing_entity.number is not None:
            values["number"] = filing_entity.number
        if filing_entity.status is not None:
            values["status"] = filing_entity.status

        if not values:
            return self._get_filing_entity_by_id(
                company_id=company_id, filing_entity_id=filing_entity_id
            )

        try:
            with self.engine.connect() as conn:
                res = conn.execute(
                    update(self.filing_entities_table)
                    .where(
                        (self.filing_entities_table.c.id == filing_entity_id)
                        & (self.filing_entities_table.c.company_id == company_id)
                    )
                    .values(**values)
                )
                if res.rowcount == 0:
                    conn.rollback()
                    return None
                conn.commit()
                return self._get_filing_entity_by_id(
                    company_id=company_id, filing_entity_id=filing_entity_id
                )
        except SQLAlchemyError as e:
            logger.error(
                "Error updating filing_entity_id=%s for company_id=%s: %s",
                filing_entity_id,
                company_id,
                e,
            )
            return None

    def delete_filing_entity(self, *, company_id: int, filing_entity_id: int) -> bool:
        """Delete a filing entity for a company."""
        try:
            with self.engine.connect() as conn:
                res = conn.execute(
                    delete(self.filing_entities_table).where(
                        (self.filing_entities_table.c.id == filing_entity_id)
                        & (self.filing_entities_table.c.company_id == company_id)
                    )
                )
                if res.rowcount == 0:
                    conn.rollback()
                    return False
                conn.commit()
                return True
        except SQLAlchemyError as e:
            logger.error(
                "Error deleting filing_entity_id=%s for company_id=%s: %s",
                filing_entity_id,
                company_id,
                e,
            )
            return False

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

    def get_filing_entities_by_company_ids(
        self,
        *,
        company_ids: List[int],
        registry: Optional[str] = None,
        status: Optional[str] = None,
    ) -> dict[int, List[FilingEntity]]:
        """Get filing_entities for multiple companies in one query.

        Returns:
            Mapping of company_id -> list of filing entities.
        """
        if not company_ids:
            return {}

        try:
            with self.engine.connect() as conn:
                stmt = select(
                    self.filing_entities_table.c.id,
                    self.filing_entities_table.c.registry,
                    self.filing_entities_table.c.number,
                    self.filing_entities_table.c.status,
                    self.filing_entities_table.c.company_id,
                ).where(self.filing_entities_table.c.company_id.in_(company_ids))

                if registry is not None:
                    stmt = stmt.where(self.filing_entities_table.c.registry == registry)
                if status is not None:
                    stmt = stmt.where(self.filing_entities_table.c.status == status)

                rows = conn.execute(stmt).fetchall()
                grouped: dict[int, List[FilingEntity]] = {}
                for r in rows:
                    cid = int(r.company_id)
                    grouped.setdefault(cid, []).append(
                        FilingEntity(
                            id=int(r.id),
                            registry=str(r.registry),
                            number=str(r.number),
                            status=str(r.status),
                            company_id=cid,
                        )
                    )
                return grouped
        except SQLAlchemyError as e:
            logger.error(
                "Error getting filing_entities records for company_ids=%s: %s",
                company_ids,
                e,
            )
            return {}

    def _get_ticker_by_id(self, *, company_id: int, ticker_id: int) -> Optional[Ticker]:
        """Get a ticker row by ID scoped to company."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    select(
                        self.tickers_table.c.id,
                        self.tickers_table.c.ticker,
                        self.tickers_table.c.exchange,
                        self.tickers_table.c.status,
                        self.tickers_table.c.company_id,
                    ).where(
                        (self.tickers_table.c.id == ticker_id)
                        & (self.tickers_table.c.company_id == company_id)
                    )
                ).fetchone()
                if row is None:
                    return None
                return Ticker(
                    id=int(row.id),
                    ticker=str(row.ticker),
                    exchange=str(row.exchange),
                    status=str(row.status),
                    company_id=int(row.company_id),
                )
        except SQLAlchemyError as e:
            logger.error(
                "Error getting ticker_id=%s for company_id=%s: %s",
                ticker_id,
                company_id,
                e,
            )
            return None

    def _get_filing_entity_by_id(
        self, *, company_id: int, filing_entity_id: int
    ) -> Optional[FilingEntity]:
        """Get a filing entity row by ID scoped to company."""
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    select(
                        self.filing_entities_table.c.id,
                        self.filing_entities_table.c.registry,
                        self.filing_entities_table.c.number,
                        self.filing_entities_table.c.status,
                        self.filing_entities_table.c.company_id,
                    ).where(
                        (self.filing_entities_table.c.id == filing_entity_id)
                        & (self.filing_entities_table.c.company_id == company_id)
                    )
                ).fetchone()
                if row is None:
                    return None
                return FilingEntity(
                    id=int(row.id),
                    registry=str(row.registry),
                    number=str(row.number),
                    status=str(row.status),
                    company_id=int(row.company_id),
                )
        except SQLAlchemyError as e:
            logger.error(
                "Error getting filing_entity_id=%s for company_id=%s: %s",
                filing_entity_id,
                company_id,
                e,
            )
            return None
