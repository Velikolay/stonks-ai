"""Tests for company database operations."""

import pytest

from filings import Company, CompanyCreate


@pytest.mark.asyncio
class TestCompanyOperations:
    """Test company database operations."""

    async def _get_or_create_company_by_ticker(
        self, db, *, ticker: str, exchange: str, name: str
    ) -> Company:
        existing = await db.companies.get_company_by_ticker(ticker, exchange)
        if existing:
            return existing
        company_id = await db.companies.insert_company(CompanyCreate(name=name))
        assert company_id is not None
        company = await db.companies.get_company_by_id(company_id)
        assert company is not None
        ok = await db.companies.upsert_ticker(
            company_id=company.id,
            ticker=ticker,
            exchange=exchange,
            status="active",
        )
        assert ok
        return company

    async def test_insert_company(self, db, sample_company):
        """Test inserting a new company."""
        company_id = await db.companies.insert_company(sample_company)
        assert company_id is not None
        assert isinstance(company_id, int)
        assert company_id > 0

        company = await db.companies.get_company_by_id(company_id)
        assert company is not None
        assert company.id == company_id
        assert company.name == sample_company.name

    async def test_get_company_by_id(self, db, sample_company):
        """Test retrieving company by ID."""
        company_id = await db.companies.insert_company(sample_company)
        assert company_id is not None, "insert_company should return an ID"
        company = await db.companies.get_company_by_id(company_id)
        assert company is not None
        assert company.id == company_id
        assert company.name == sample_company.name

    async def test_get_company_by_id_not_found(self, db):
        """Test retrieving non-existent company by ID."""
        company = await db.companies.get_company_by_id(99999)
        assert company is None

    async def test_get_companies_by_ids(self, db):
        """Test retrieving multiple companies by IDs."""
        c1_id = await db.companies.insert_company(CompanyCreate(name="Company One"))
        c2_id = await db.companies.insert_company(CompanyCreate(name="Company Two"))
        assert c1_id is not None
        assert c2_id is not None

        companies = await db.companies.get_companies_by_ids(
            company_ids=[c2_id, c1_id, 99999, c2_id]
        )
        ids = {c.id for c in companies}
        assert c1_id in ids
        assert c2_id in ids

    async def test_get_company_by_ticker(self, db, sample_company):
        """Test retrieving company by ticker."""
        company = await self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name=sample_company.name
        )
        assert (
            company is not None
        ), "_get_or_create_company_by_ticker should return a company"
        company_id = company.id

        company = await db.companies.get_company_by_ticker("AAPL")
        assert company is not None
        assert company.id == company_id
        assert company.name == sample_company.name

    async def test_get_company_by_ticker_and_exchange(self, db, sample_company):
        """Test retrieving company by ticker and exchange."""
        created = await self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name=sample_company.name
        )
        assert created is not None
        company_id = created.id

        company = await db.companies.get_company_by_ticker("AAPL", "NASDAQ")
        assert company is not None
        assert company.id == company_id
        assert company.name == sample_company.name

    async def test_get_company_by_ticker_not_found(self, db):
        """Test retrieving non-existent company by ticker."""
        company = await db.companies.get_company_by_ticker("INVALID")
        assert company is None

    async def test_get_all_companies(self, db, sample_company):
        """Test retrieving all companies."""
        company1 = await self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name="Apple Inc."
        )
        company2 = await self._get_or_create_company_by_ticker(
            db, ticker="MSFT", exchange="NASDAQ", name="Microsoft Corp."
        )
        company3 = await self._get_or_create_company_by_ticker(
            db, ticker="GOOGL", exchange="NASDAQ", name="Alphabet Inc."
        )
        assert company1 is not None
        assert company2 is not None
        assert company3 is not None

        companies = await db.companies.get_all_companies()
        assert len(companies) >= 3
        assert any(c.name == "Apple Inc." for c in companies)
        assert any(c.name == "Microsoft Corp." for c in companies)
        assert any(c.name == "Alphabet Inc." for c in companies)

    async def test_get_or_create_company_new(self, db, sample_company):
        """Test get_or_create_company with new company."""
        company = await db.companies.get_or_create_company(sample_company)
        assert company is not None
        assert company.name == sample_company.name
        assert company.id > 0

    async def test_get_or_create_company_existing(self, db, sample_company):
        """Test get_or_create_company with existing company."""
        original_company = await self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name=sample_company.name
        )
        retrieved_company = await self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name=sample_company.name
        )
        assert retrieved_company is not None
        assert retrieved_company.id == original_company.id
        assert retrieved_company.name == original_company.name

    async def test_insert_company_minimal_fields(self, db):
        """Test inserting company with minimal fields."""
        company_data = CompanyCreate(name="Test Company")
        company_id = await db.companies.insert_company(company_data)
        assert company_id is not None

        company = await db.companies.get_company_by_id(company_id)
        assert company is not None
        assert company.name == "Test Company"


class TestCompanyModelValidation:
    """Sync tests for company model validation (no asyncio)."""

    def test_company_model_validation(self):
        """Test company model validation."""
        company = CompanyCreate(name="Apple Inc.")
        assert company.name == "Apple Inc."

        complete_company = Company(id=1, name="Apple Inc.")
        assert complete_company.id == 1
        assert complete_company.name == "Apple Inc."
