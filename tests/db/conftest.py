"""Pytest configuration for database tests."""

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from db import CompanyCreate, Database, FilingCreate, FinancialFactCreate


@pytest.fixture(scope="session")
def test_db_url() -> str:
    """Test database URL."""
    return "postgresql://rag_user:rag_password@localhost:5432/rag_db"


@pytest.fixture(scope="session")
def test_engine(test_db_url: str) -> Engine:
    """Test database engine."""
    engine = create_engine(test_db_url)

    # Run migrations to create tables
    from alembic import command
    from alembic.config import Config

    # Create Alembic config
    alembic_cfg = Config("alembic.ini")

    # Run migrations to create all tables
    command.upgrade(alembic_cfg, "head")

    yield engine

    # Cleanup - truncate all tables (don't drop them since they're managed by migrations)
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE financial_facts CASCADE"))
        conn.execute(text("TRUNCATE TABLE filings CASCADE"))
        conn.execute(text("TRUNCATE TABLE companies CASCADE"))
        conn.commit()

    engine.dispose()


@pytest.fixture(autouse=True)
def clean_tables(test_engine: Engine):
    """Automatically clean tables before each test."""
    # Clean tables before each test
    with test_engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE financial_facts CASCADE"))
        conn.execute(text("TRUNCATE TABLE filings CASCADE"))
        conn.execute(text("TRUNCATE TABLE companies CASCADE"))
        conn.commit()


@pytest.fixture(scope="function")
def db(test_engine: Engine) -> Database:
    """Test database instance."""
    return Database("postgresql://rag_user:rag_password@localhost:5432/rag_db")


@pytest.fixture(scope="function")
def sample_company() -> CompanyCreate:
    """Sample company data for testing."""
    return CompanyCreate(ticker="AAPL", exchange="NASDAQ", name="Apple Inc.")


@pytest.fixture(scope="function")
def sample_filing(sample_company: CompanyCreate) -> FilingCreate:
    """Sample filing data for testing."""
    return FilingCreate(
        company_id=1,  # Will be set in tests
        source="SEC",
        filing_number="0000320193-25-000073",
        form_type="10-Q",
        filing_date=date(2024, 12, 19),
        fiscal_period_end=date(2024, 9, 28),
        fiscal_year=2024,
        fiscal_quarter=4,
        public_url="https://www.sec.gov/Archives/edgar/data/320193/000032019325000073/aapl-20240928.htm",
    )


@pytest.fixture(scope="function")
def sample_financial_fact() -> FinancialFactCreate:
    """Sample financial fact data for testing."""
    return FinancialFactCreate(
        filing_id=1,  # Will be set in tests
        metric="Revenue",
        value=Decimal("89498.0"),
        unit="USD",
        statement="Income Statement",
        period_end=date(2024, 9, 28),
        period_start=date(2024, 6, 30),
    )
