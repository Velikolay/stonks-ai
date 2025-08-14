"""Tests for filing database operations."""

from datetime import date

from db import Filing, FilingCreate


class TestFilingOperations:
    """Test filing database operations."""

    def test_insert_filing(self, db, sample_company, sample_filing):
        """Test inserting a new filing."""
        # Create company first
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id

        # Insert filing
        filing_id = db.filings.insert_filing(sample_filing)

        # Verify filing was inserted
        assert filing_id is not None
        assert isinstance(filing_id, int)
        assert filing_id > 0

        # Retrieve and verify filing data
        filing = db.filings.get_filing_by_id(filing_id)
        assert filing is not None
        assert filing.id == filing_id
        assert filing.company_id == company.id
        assert filing.source == sample_filing.source
        assert filing.filing_number == sample_filing.filing_number
        assert filing.form_type == sample_filing.form_type
        assert filing.filing_date == sample_filing.filing_date
        assert filing.fiscal_period_end == sample_filing.fiscal_period_end
        assert filing.fiscal_year == sample_filing.fiscal_year
        assert filing.fiscal_quarter == sample_filing.fiscal_quarter
        assert filing.public_url == sample_filing.public_url

    def test_get_filing_by_id(self, db, sample_company, sample_filing):
        """Test retrieving filing by ID."""
        # Create company and filing first
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing_id = db.filings.insert_filing(sample_filing)

        # Retrieve filing
        filing = db.filings.get_filing_by_id(filing_id)

        # Verify filing data
        assert filing is not None
        assert filing.id == filing_id
        assert filing.company_id == company.id
        assert filing.source == sample_filing.source
        assert filing.filing_number == sample_filing.filing_number

    def test_get_filing_by_id_not_found(self, db):
        """Test retrieving non-existent filing by ID."""
        filing = db.filings.get_filing_by_id(99999)
        assert filing is None

    def test_get_filings_by_company(self, db, sample_company):
        """Test retrieving filings by company."""
        # Create company
        company = db.companies.get_or_create_company(sample_company)

        # Create multiple filings
        filing1 = FilingCreate(
            company_id=company.id,
            source="SEC",
            filing_number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
            public_url="https://example.com/1",
        )

        filing2 = FilingCreate(
            company_id=company.id,
            source="SEC",
            filing_number="0000320193-25-000074",
            form_type="10-K",
            filing_date=date(2024, 11, 15),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=0,
            public_url="https://example.com/2",
        )

        db.filings.insert_filing(filing1)
        db.filings.insert_filing(filing2)

        # Retrieve all filings for company
        filings = db.filings.get_filings_by_company(company.id)

        # Verify results
        assert len(filings) >= 2
        assert any(f.form_type == "10-Q" for f in filings)
        assert any(f.form_type == "10-K" for f in filings)

    def test_get_filings_by_company_and_form_type(self, db, sample_company):
        """Test retrieving filings by company and form type."""
        # Create company
        company = db.companies.get_or_create_company(sample_company)

        # Create filings with different form types
        filing1 = FilingCreate(
            company_id=company.id,
            source="SEC",
            filing_number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
        )

        filing2 = FilingCreate(
            company_id=company.id,
            source="SEC",
            filing_number="0000320193-25-000074",
            form_type="10-K",
            filing_date=date(2024, 11, 15),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=0,
        )

        db.filings.insert_filing(filing1)
        db.filings.insert_filing(filing2)

        # Retrieve only 10-Q filings
        filings = db.filings.get_filings_by_company(company.id, "10-Q")

        # Verify results
        assert len(filings) >= 1
        assert all(f.form_type == "10-Q" for f in filings)

    def test_get_latest_filing(self, db, sample_company):
        """Test retrieving latest filing for company and form type."""
        # Create company
        company = db.companies.get_or_create_company(sample_company)

        # Create multiple filings with different dates
        filing1 = FilingCreate(
            company_id=company.id,
            source="SEC",
            filing_number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 6, 15),
            fiscal_period_end=date(2024, 3, 30),
            fiscal_year=2024,
            fiscal_quarter=2,
        )

        filing2 = FilingCreate(
            company_id=company.id,
            source="SEC",
            filing_number="0000320193-25-000074",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
        )

        db.filings.insert_filing(filing1)
        latest_filing_id = db.filings.insert_filing(filing2)

        # Retrieve latest filing
        latest_filing = db.filings.get_latest_filing(company.id, "10-Q")

        # Verify results
        assert latest_filing is not None
        assert latest_filing.id == latest_filing_id
        assert latest_filing.filing_date == date(2024, 12, 19)

    def test_get_filing_by_number(self, db, sample_company, sample_filing):
        """Test retrieving filing by source and filing number."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing_id = db.filings.insert_filing(sample_filing)

        # Retrieve filing by number
        filing = db.filings.get_filing_by_number(
            sample_filing.source, sample_filing.filing_number
        )

        # Verify results
        assert filing is not None
        assert filing.id == filing_id
        assert filing.source == sample_filing.source
        assert filing.filing_number == sample_filing.filing_number

    def test_get_filing_by_number_not_found(self, db):
        """Test retrieving non-existent filing by number."""
        filing = db.filings.get_filing_by_number("SEC", "INVALID")
        assert filing is None

    def test_get_or_create_filing_new(self, db, sample_company, sample_filing):
        """Test get_or_create_filing with new filing."""
        # Create company
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id

        # Get or create filing
        filing = db.filings.get_or_create_filing(sample_filing)

        # Verify filing was created
        assert filing is not None
        assert filing.company_id == company.id
        assert filing.source == sample_filing.source
        assert filing.filing_number == sample_filing.filing_number
        assert filing.id > 0

    def test_get_or_create_filing_existing(self, db, sample_company, sample_filing):
        """Test get_or_create_filing with existing filing."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        original_filing = db.filings.get_or_create_filing(sample_filing)

        # Try to get or create the same filing again
        retrieved_filing = db.filings.get_or_create_filing(sample_filing)

        # Verify same filing was returned
        assert retrieved_filing is not None
        assert retrieved_filing.id == original_filing.id
        assert retrieved_filing.source == original_filing.source
        assert retrieved_filing.filing_number == original_filing.filing_number

    def test_filing_model_validation(self):
        """Test filing model validation."""
        # Valid filing
        filing = FilingCreate(
            company_id=1,
            source="SEC",
            filing_number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
            public_url="https://example.com",
        )
        assert filing.company_id == 1
        assert filing.source == "SEC"
        assert filing.filing_number == "0000320193-25-000073"
        assert filing.form_type == "10-Q"
        assert filing.fiscal_year == 2024
        assert filing.fiscal_quarter == 4

        # Filing with ID (complete model)
        complete_filing = Filing(
            id=1,
            company_id=1,
            source="SEC",
            filing_number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
            public_url="https://example.com",
        )
        assert complete_filing.id == 1
        assert complete_filing.company_id == 1
        assert complete_filing.source == "SEC"
        assert complete_filing.filing_number == "0000320193-25-000073"
