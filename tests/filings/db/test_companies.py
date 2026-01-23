"""Tests for company database operations."""

from filings import Company, CompanyCreate


class TestCompanyOperations:
    """Test company database operations."""

    def _get_or_create_company_by_ticker(
        self, db, *, ticker: str, exchange: str, name: str
    ) -> Company:
        existing = db.companies.get_company_by_ticker(ticker, exchange)
        if existing:
            return existing
        company_id = db.companies.insert_company(CompanyCreate(name=name))
        assert company_id is not None
        company = db.companies.get_company_by_id(company_id)
        assert company is not None
        assert db.companies.upsert_ticker(
            company_id=company.id,
            ticker=ticker,
            exchange=exchange,
            status="active",
        )
        return company

    def test_insert_company(self, db, sample_company):
        """Test inserting a new company."""
        # Insert company
        company_id = db.companies.insert_company(sample_company)

        # Verify company was inserted
        assert company_id is not None
        assert isinstance(company_id, int)
        assert company_id > 0

        # Retrieve and verify company data
        company = db.companies.get_company_by_id(company_id)
        assert company is not None
        assert company.id == company_id
        assert company.name == sample_company.name

    def test_get_company_by_id(self, db, sample_company):
        """Test retrieving company by ID."""
        # Insert company first
        company_id = db.companies.insert_company(sample_company)

        # Retrieve company
        company = db.companies.get_company_by_id(company_id)

        # Verify company data
        assert company is not None
        assert company.id == company_id
        assert company.name == sample_company.name

    def test_get_company_by_id_not_found(self, db):
        """Test retrieving non-existent company by ID."""
        company = db.companies.get_company_by_id(99999)
        assert company is None

    def test_get_companies_by_ids(self, db):
        """Test retrieving multiple companies by IDs."""
        c1_id = db.companies.insert_company(CompanyCreate(name="Company One"))
        c2_id = db.companies.insert_company(CompanyCreate(name="Company Two"))
        assert c1_id is not None
        assert c2_id is not None

        companies = db.companies.get_companies_by_ids(
            company_ids=[c2_id, c1_id, 99999, c2_id]
        )
        ids = {c.id for c in companies}
        assert c1_id in ids
        assert c2_id in ids

    def test_get_company_by_ticker(self, db, sample_company):
        """Test retrieving company by ticker."""
        # Create company + ticker mapping first
        company = self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name=sample_company.name
        )
        assert company is not None
        company_id = company.id

        # Retrieve company by ticker
        company = db.companies.get_company_by_ticker("AAPL")

        # Verify company data
        assert company is not None
        assert company.id == company_id
        assert company.name == sample_company.name

    def test_get_company_by_ticker_and_exchange(self, db, sample_company):
        """Test retrieving company by ticker and exchange."""
        created = self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name=sample_company.name
        )
        assert created is not None
        company_id = created.id

        # Retrieve company by ticker and exchange
        company = db.companies.get_company_by_ticker("AAPL", "NASDAQ")

        # Verify company data
        assert company is not None
        assert company.id == company_id
        assert company.name == sample_company.name

    def test_get_company_by_ticker_not_found(self, db):
        """Test retrieving non-existent company by ticker."""
        company = db.companies.get_company_by_ticker("INVALID")
        assert company is None

    def test_get_all_companies(self, db, sample_company):
        """Test retrieving all companies."""
        # Insert multiple companies
        company1 = self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name="Apple Inc."
        )
        company2 = self._get_or_create_company_by_ticker(
            db, ticker="MSFT", exchange="NASDAQ", name="Microsoft Corp."
        )
        company3 = self._get_or_create_company_by_ticker(
            db, ticker="GOOGL", exchange="NASDAQ", name="Alphabet Inc."
        )
        assert company1 is not None
        assert company2 is not None
        assert company3 is not None

        # Retrieve all companies
        companies = db.companies.get_all_companies()

        # Verify results
        assert len(companies) >= 3
        assert any(c.name == "Apple Inc." for c in companies)
        assert any(c.name == "Microsoft Corp." for c in companies)
        assert any(c.name == "Alphabet Inc." for c in companies)

    def test_get_or_create_company_new(self, db, sample_company):
        """Test get_or_create_company with new company."""
        # Get or create company
        company = db.companies.get_or_create_company(sample_company)

        # Verify company was created
        assert company is not None
        assert company.name == sample_company.name
        assert company.id > 0

    def test_get_or_create_company_existing(self, db, sample_company):
        """Test get_or_create_company with existing company."""
        # Create company + ticker mapping first
        original_company = self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name=sample_company.name
        )

        # Try to get or create the same company again
        retrieved_company = self._get_or_create_company_by_ticker(
            db, ticker="AAPL", exchange="NASDAQ", name=sample_company.name
        )

        # Verify same company was returned
        assert retrieved_company is not None
        assert retrieved_company.id == original_company.id
        assert retrieved_company.name == original_company.name

    def test_insert_company_minimal_fields(self, db):
        """Test inserting company with minimal fields."""
        company_data = CompanyCreate(name="Test Company")

        company_id = db.companies.insert_company(company_data)
        assert company_id is not None

        company = db.companies.get_company_by_id(company_id)
        assert company is not None
        assert company.name == "Test Company"

    def test_company_model_validation(self):
        """Test company model validation."""
        # Valid company
        company = CompanyCreate(name="Apple Inc.")
        assert company.name == "Apple Inc."

        # Company with ID (complete model)
        complete_company = Company(id=1, name="Apple Inc.")
        assert complete_company.id == 1
        assert complete_company.name == "Apple Inc."
