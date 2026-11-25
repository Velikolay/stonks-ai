"""Pytest configuration for database tests."""

from datetime import date
from decimal import Decimal
from typing import Callable, Optional, Sequence

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from filings import CompanyCreate, FilingCreate, FinancialFact, PeriodType
from filings.db import AsyncFilingsDatabase
from filings.refresh import refresh_companies_sync


def recreate_test_database() -> None:
    """Drop and recreate the test database so migrations run from the base.

    ``alembic upgrade head`` skips a revision inserted below the stamped
    version, which leaves tables such as ``financial_rewrites`` missing.
    """
    default_db_url = "postgresql://rag_user:rag_password@localhost:5432/postgres"
    test_db_name = "rag_db_test"
    default_engine = create_engine(default_db_url, isolation_level="AUTOCOMMIT")
    try:
        with default_engine.connect() as conn:
            conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :name AND pid <> pg_backend_pid()"
                ),
                {"name": test_db_name},
            )
            conn.execute(text(f"DROP DATABASE IF EXISTS {test_db_name}"))
            conn.execute(text(f"CREATE DATABASE {test_db_name}"))
    except Exception as e:
        print(f"❌ Failed to recreate test database: {e}")
        print("Please recreate the test database manually:")
        print(
            "  docker compose exec postgres psql -U rag_user -d postgres "
            f'-c "DROP DATABASE IF EXISTS {test_db_name};"'
        )
        raise
    finally:
        default_engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """Rebuild the test database before running tests."""
    recreate_test_database()


@pytest.fixture(scope="session")
def test_db_url() -> str:
    """Test database URL."""
    # Use a separate test database to avoid conflicts with main database
    return "postgresql://rag_user:rag_password@localhost:5432/rag_db_test"


@pytest.fixture(scope="session")
def test_engine(test_db_url: str) -> Engine:
    """Test database engine."""
    import os
    from pathlib import Path

    engine = create_engine(test_db_url)

    # Run migrations to create tables
    from alembic import command
    from alembic.config import Config

    # Get the project root directory (where alembic.ini is located)
    project_root = Path(__file__).parent.parent.parent.parent
    alembic_ini_path = project_root / "alembic.ini"

    # Create Alembic config and override the database URL
    alembic_cfg = Config(str(alembic_ini_path))
    alembic_cfg.set_main_option("sqlalchemy.url", test_db_url)

    # Save original state
    original_cwd = os.getcwd()
    original_database_url = os.environ.get("DATABASE_URL")

    # CRITICAL: Override DATABASE_URL so Alembic's env.py uses test database
    os.environ["DATABASE_URL"] = test_db_url
    os.chdir(str(project_root))

    try:
        # Run migrations to create all tables
        command.upgrade(alembic_cfg, "head")
    finally:
        # Restore original state
        os.chdir(original_cwd)
        if original_database_url is not None:
            os.environ["DATABASE_URL"] = original_database_url
        else:
            os.environ.pop("DATABASE_URL", None)

    yield engine

    # Cleanup - truncate all tables (don't drop views/tables since they're managed by migrations)
    try:
        with engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE yearly_financials CASCADE"))
            conn.execute(text("TRUNCATE TABLE quarterly_financials CASCADE"))
            conn.execute(text("TRUNCATE TABLE financial_facts_normalized CASCADE"))
            conn.execute(text("TRUNCATE TABLE dimension_breakdowns CASCADE"))
            conn.execute(text("TRUNCATE TABLE dimension_normalization CASCADE"))
            conn.execute(text("TRUNCATE TABLE hierarchy_normalization CASCADE"))
            conn.execute(text("TRUNCATE TABLE concept_normalization CASCADE"))
            conn.execute(text("TRUNCATE TABLE financial_facts_overridden CASCADE"))
            conn.execute(text("TRUNCATE TABLE financial_facts CASCADE"))
            conn.execute(text("TRUNCATE TABLE filings CASCADE"))
            conn.execute(text("TRUNCATE TABLE filing_entities CASCADE"))
            conn.execute(text("TRUNCATE TABLE tickers CASCADE"))
            conn.execute(text("TRUNCATE TABLE companies RESTART IDENTITY CASCADE"))
            conn.execute(text("TRUNCATE TABLE documents CASCADE"))
            conn.execute(text("TRUNCATE TABLE concept_normalization_overrides CASCADE"))
            conn.commit()
    except Exception as e:
        # Tables might not exist yet, which is fine
        print(f"Note: Could not truncate tables during cleanup: {e}")

    engine.dispose()


@pytest.fixture(autouse=True)
def clean_tables(test_engine: Engine):
    """Automatically clean tables before each test."""
    # Clean tables before each test
    try:
        with test_engine.connect() as conn:
            conn.execute(text("TRUNCATE TABLE yearly_financials CASCADE"))
            conn.execute(text("TRUNCATE TABLE quarterly_financials CASCADE"))
            conn.execute(text("TRUNCATE TABLE financial_facts_normalized CASCADE"))
            conn.execute(text("TRUNCATE TABLE dimension_breakdowns CASCADE"))
            conn.execute(text("TRUNCATE TABLE dimension_normalization CASCADE"))
            conn.execute(text("TRUNCATE TABLE hierarchy_normalization CASCADE"))
            conn.execute(text("TRUNCATE TABLE concept_normalization CASCADE"))
            conn.execute(text("TRUNCATE TABLE financial_facts_overridden CASCADE"))
            conn.execute(text("TRUNCATE TABLE financial_facts CASCADE"))
            conn.execute(text("TRUNCATE TABLE filings CASCADE"))
            conn.execute(text("TRUNCATE TABLE filing_entities CASCADE"))
            conn.execute(text("TRUNCATE TABLE tickers CASCADE"))
            conn.execute(text("TRUNCATE TABLE companies RESTART IDENTITY CASCADE"))
            conn.execute(text("TRUNCATE TABLE documents CASCADE"))
            conn.execute(text("TRUNCATE TABLE concept_normalization_overrides CASCADE"))
            conn.execute(text("TRUNCATE TABLE financial_facts_overrides CASCADE"))
            # Recreate company rows that migrations and override CSVs reference.
            # company_id=0: global overrides
            for cid, name in [(0, "0")]:
                conn.execute(
                    text(
                        "INSERT INTO companies (id, name, industry) "
                        f"VALUES ({cid}, '{name}', NULL) "
                        "ON CONFLICT (id) DO NOTHING"
                    )
                )
            conn.commit()
    except Exception:
        # Tables might not exist yet, which is fine for the first test
        pass


@pytest.fixture
def refresh(test_db_url: str) -> Callable[..., None]:
    """Run the refresh for company 1 against the test database.

    It writes through its own connection, so seed rows must be committed
    first: seed in one ``test_engine.begin()`` block, read in another.
    """

    def run(stages: Optional[Sequence[str]] = None) -> None:
        refresh_companies_sync([1], database_url=test_db_url, stages=stages, jobs=1)

    return run


@pytest.fixture
async def db(test_db_url: str) -> AsyncFilingsDatabase:
    """Async test database instance."""
    database = AsyncFilingsDatabase(test_db_url)
    await database.initialize()
    try:
        yield database
    finally:
        await database.aclose()


@pytest.fixture(scope="function")
def sample_company() -> CompanyCreate:
    """Sample company data for testing."""
    return CompanyCreate(name="Apple Inc.")


@pytest.fixture(scope="function")
def sample_filing(sample_company: CompanyCreate) -> FilingCreate:
    """Sample filing data for testing."""
    return FilingCreate(
        company_id=1,  # Will be set in tests
        filing_entity_id=0,  # Will be set in tests
        registry="SEC",
        number="0000320193-25-000073",
        form_type="10-Q",
        filing_date=date(2024, 12, 19),
        fiscal_period_end=date(2024, 9, 28),
        fiscal_year=2024,
        fiscal_quarter=4,
        public_url="https://www.sec.gov/Archives/edgar/data/320193/000032019325000073/aapl-20240928.htm",
    )


@pytest.fixture(scope="function")
def sample_financial_fact() -> FinancialFact:
    """Sample financial fact data for testing."""
    return FinancialFact(
        id=0,
        company_id=1,  # Will be set in tests
        filing_id=1,  # Will be set in tests
        form_type="10-Q",
        concept="us-gaap:Revenues",
        label="Revenues",
        is_abstract=False,
        value=Decimal("89498.0"),
        unit="USD",
        statement="Income Statement",
        period_end=date(2024, 9, 28),
        period=PeriodType.Q,
    )
