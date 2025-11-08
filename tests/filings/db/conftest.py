"""Pytest configuration for database tests."""

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from filings import (
    CompanyCreate,
    FilingCreate,
    FilingsDatabase,
    FinancialFact,
    PeriodType,
)


def create_test_database_if_not_exists():
    """Create test database if it doesn't exist."""
    # Connect to default postgres database to create test database
    default_db_url = "postgresql://rag_user:rag_password@localhost:5432/postgres"
    test_db_name = "rag_db_test"

    try:
        # Try to connect to the test database first
        test_db_url = (
            f"postgresql://rag_user:rag_password@localhost:5432/{test_db_name}"
        )
        test_engine = create_engine(test_db_url)
        test_engine.connect()
        test_engine.dispose()
        print(f"✅ Test database '{test_db_name}' already exists")
        return
    except OperationalError:
        # Test database doesn't exist, create it
        try:
            # Use autocommit mode to avoid transaction block issues
            default_engine = create_engine(default_db_url, isolation_level="AUTOCOMMIT")
            with default_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE {test_db_name}"))
            default_engine.dispose()
            print(f"✅ Created test database '{test_db_name}'")
        except Exception as e:
            print(f"❌ Failed to create test database: {e}")
            print("Please create the test database manually:")
            print(
                f'  docker compose exec postgres psql -U rag_user -d postgres -c "CREATE DATABASE {test_db_name};"'
            )
            raise


@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """Setup test database before running tests."""
    create_test_database_if_not_exists()


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
            conn.execute(text("TRUNCATE TABLE financial_facts CASCADE"))
            conn.execute(text("TRUNCATE TABLE filings CASCADE"))
            conn.execute(text("TRUNCATE TABLE companies CASCADE"))
            conn.execute(text("TRUNCATE TABLE documents CASCADE"))
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
            # Views that depend on the data need to be dropped/recreated or just left as-is
            # since they're just queries over the data
            conn.execute(text("TRUNCATE TABLE financial_facts CASCADE"))
            conn.execute(text("TRUNCATE TABLE filings CASCADE"))
            conn.execute(text("TRUNCATE TABLE companies CASCADE"))
            conn.execute(text("TRUNCATE TABLE documents CASCADE"))
            conn.commit()
    except Exception:
        # Tables might not exist yet, which is fine for the first test
        pass


@pytest.fixture(scope="function")
def db(test_engine: Engine) -> FilingsDatabase:
    """Test database instance."""
    return FilingsDatabase(
        "postgresql://rag_user:rag_password@localhost:5432/rag_db_test"
    )


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
def sample_financial_fact() -> FinancialFact:
    """Sample financial fact data for testing."""
    return FinancialFact(
        id=0,
        filing_id=1,  # Will be set in tests
        concept="us-gaap:Revenues",
        label="Revenues",
        is_abstract=False,
        value=Decimal("89498.0"),
        unit="USD",
        statement="Income Statement",
        period_end=date(2024, 9, 28),
        period=PeriodType.Q,
    )
