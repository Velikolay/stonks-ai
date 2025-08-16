"""Tests for company database operations."""

from filings import Company, CompanyCreate


class TestCompanyOperations:
    """Test company database operations."""

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
        assert company.ticker == sample_company.ticker
        assert company.exchange == sample_company.exchange
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
        assert company.ticker == sample_company.ticker
        assert company.exchange == sample_company.exchange
        assert company.name == sample_company.name

    def test_get_company_by_id_not_found(self, db):
        """Test retrieving non-existent company by ID."""
        company = db.companies.get_company_by_id(99999)
        assert company is None

    def test_get_company_by_ticker(self, db, sample_company):
        """Test retrieving company by ticker."""
        # Insert company first
        company_id = db.companies.insert_company(sample_company)

        # Retrieve company by ticker
        company = db.companies.get_company_by_ticker(sample_company.ticker)

        # Verify company data
        assert company is not None
        assert company.id == company_id
        assert company.ticker == sample_company.ticker
        assert company.exchange == sample_company.exchange
        assert company.name == sample_company.name

    def test_get_company_by_ticker_and_exchange(self, db, sample_company):
        """Test retrieving company by ticker and exchange."""
        # Insert company first
        company_id = db.companies.insert_company(sample_company)

        # Retrieve company by ticker and exchange
        company = db.companies.get_company_by_ticker(
            sample_company.ticker, sample_company.exchange
        )

        # Verify company data
        assert company is not None
        assert company.id == company_id
        assert company.ticker == sample_company.ticker
        assert company.exchange == sample_company.exchange
        assert company.name == sample_company.name

    def test_get_company_by_ticker_not_found(self, db):
        """Test retrieving non-existent company by ticker."""
        company = db.companies.get_company_by_ticker("INVALID")
        assert company is None

    def test_get_all_companies(self, db, sample_company):
        """Test retrieving all companies."""
        # Insert multiple companies
        company1 = CompanyCreate(ticker="AAPL", exchange="NASDAQ", name="Apple Inc.")
        company2 = CompanyCreate(
            ticker="MSFT", exchange="NASDAQ", name="Microsoft Corp."
        )
        company3 = CompanyCreate(
            ticker="GOOGL", exchange="NASDAQ", name="Alphabet Inc."
        )

        db.companies.insert_company(company1)
        db.companies.insert_company(company2)
        db.companies.insert_company(company3)

        # Retrieve all companies
        companies = db.companies.get_all_companies()

        # Verify results
        assert len(companies) >= 3
        assert any(c.ticker == "AAPL" for c in companies)
        assert any(c.ticker == "MSFT" for c in companies)
        assert any(c.ticker == "GOOGL" for c in companies)

    def test_get_or_create_company_new(self, db, sample_company):
        """Test get_or_create_company with new company."""
        # Get or create company
        company = db.companies.get_or_create_company(sample_company)

        # Verify company was created
        assert company is not None
        assert company.ticker == sample_company.ticker
        assert company.exchange == sample_company.exchange
        assert company.name == sample_company.name
        assert company.id > 0

    def test_get_or_create_company_existing(self, db, sample_company):
        """Test get_or_create_company with existing company."""
        # Create company first
        original_company = db.companies.get_or_create_company(sample_company)

        # Try to get or create the same company again
        retrieved_company = db.companies.get_or_create_company(sample_company)

        # Verify same company was returned
        assert retrieved_company is not None
        assert retrieved_company.id == original_company.id
        assert retrieved_company.ticker == original_company.ticker
        assert retrieved_company.exchange == original_company.exchange
        assert retrieved_company.name == original_company.name

    def test_insert_company_without_ticker(self, db):
        """Test inserting company without ticker."""
        company_data = CompanyCreate(ticker=None, exchange=None, name="Test Company")

        company_id = db.companies.insert_company(company_data)
        assert company_id is not None

        company = db.companies.get_company_by_id(company_id)
        assert company is not None
        assert company.ticker is None
        assert company.exchange is None
        assert company.name == "Test Company"

    def test_company_model_validation(self):
        """Test company model validation."""
        # Valid company
        company = CompanyCreate(ticker="AAPL", exchange="NASDAQ", name="Apple Inc.")
        assert company.ticker == "AAPL"
        assert company.exchange == "NASDAQ"
        assert company.name == "Apple Inc."

        # Company with ID (complete model)
        complete_company = Company(
            id=1, ticker="AAPL", exchange="NASDAQ", name="Apple Inc."
        )
        assert complete_company.id == 1
        assert complete_company.ticker == "AAPL"
        assert complete_company.exchange == "NASDAQ"
        assert complete_company.name == "Apple Inc."
