"""Integration tests for database operations."""

from datetime import date
from decimal import Decimal

from filings import CompanyCreate, FilingCreate, FinancialFactCreate, PeriodType


class TestDatabaseIntegration:
    """Integration tests for database operations."""

    def test_full_workflow(self, db):
        """Test complete workflow: company -> filing -> financial facts."""
        # 1. Create company
        company_data = CompanyCreate(
            ticker="AAPL", exchange="NASDAQ", name="Apple Inc."
        )
        company = db.companies.get_or_create_company(company_data)
        assert company is not None
        assert company.ticker == "AAPL"

        # 2. Create filing
        filing_data = FilingCreate(
            company_id=company.id,
            source="SEC",
            filing_number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
            public_url="https://example.com",
        )
        filing = db.filings.get_or_create_filing(filing_data)
        assert filing is not None
        assert filing.company_id == company.id
        assert filing.form_type == "10-Q"

        # 3. Create financial facts
        facts_data = [
            FinancialFactCreate(
                filing_id=filing.id,
                concept="us-gaap:Revenues",
                label="Revenues",
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period=PeriodType.Q,
            ),
            FinancialFactCreate(
                filing_id=filing.id,
                concept="us-gaap:NetIncomeLoss",
                label="Net Income (Loss)",
                value=Decimal("22956.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period=PeriodType.Q,
            ),
        ]
        fact_ids = db.financial_facts.insert_financial_facts_batch(facts_data)
        assert len(fact_ids) == 2

        # 4. Verify relationships
        # Get company filings
        filings = db.filings.get_filings_by_company(company.id)
        assert len(filings) >= 1
        assert any(f.id == filing.id for f in filings)

        # Get filing facts
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)
        assert len(facts) >= 2
        assert any(f.concept == "us-gaap:Revenues" for f in facts)
        assert any(f.concept == "us-gaap:NetIncomeLoss" for f in facts)

        # Get facts by concept
        revenue_facts = db.financial_facts.get_financial_facts_by_concept(
            company.id, "us-gaap:Revenues"
        )
        assert len(revenue_facts) >= 1
        assert all(f.concept == "us-gaap:Revenues" for f in revenue_facts)

    def test_multiple_companies_and_filings(self, db):
        """Test working with multiple companies and filings."""
        # Create multiple companies
        companies_data = [
            CompanyCreate(ticker="AAPL", exchange="NASDAQ", name="Apple Inc."),
            CompanyCreate(ticker="MSFT", exchange="NASDAQ", name="Microsoft Corp."),
            CompanyCreate(ticker="GOOGL", exchange="NASDAQ", name="Alphabet Inc."),
        ]

        companies = []
        for company_data in companies_data:
            company = db.companies.get_or_create_company(company_data)
            companies.append(company)

        # Create filings for each company
        filings = []
        for i, company in enumerate(companies):
            filing_data = FilingCreate(
                company_id=company.id,
                source="SEC",
                filing_number=f"0000320193-25-00007{i}",
                form_type="10-Q",
                filing_date=date(2024, 12, 19),
                fiscal_period_end=date(2024, 9, 28),
                fiscal_year=2024,
                fiscal_quarter=4,
            )
            filing = db.filings.get_or_create_filing(filing_data)
            filings.append(filing)

        # Create financial facts for each filing
        for filing in filings:
            fact_data = FinancialFactCreate(
                filing_id=filing.id,
                concept="us-gaap:Revenues",
                label="Revenues",
                value=Decimal("50000.0"),
                unit="USD",
                statement="Income Statement",
                period=PeriodType.Q,
            )
            db.financial_facts.insert_financial_fact(fact_data)

        # Verify all companies exist
        all_companies = db.companies.get_all_companies()
        assert len(all_companies) >= 3
        assert any(c.ticker == "AAPL" for c in all_companies)
        assert any(c.ticker == "MSFT" for c in all_companies)
        assert any(c.ticker == "GOOGL" for c in all_companies)

        # Verify filings for each company
        for company in companies:
            company_filings = db.filings.get_filings_by_company(company.id)
            assert len(company_filings) >= 1
            assert all(f.company_id == company.id for f in company_filings)

    def test_get_or_create_operations(self, db):
        """Test get_or_create operations work correctly."""
        # Test company get_or_create
        company_data = CompanyCreate(
            ticker="TSLA", exchange="NASDAQ", name="Tesla Inc."
        )

        # First call should create
        company1 = db.companies.get_or_create_company(company_data)
        assert company1 is not None
        assert company1.ticker == "TSLA"

        # Second call should retrieve existing
        company2 = db.companies.get_or_create_company(company_data)
        assert company2 is not None
        assert company2.id == company1.id

        # Test filing get_or_create
        filing_data = FilingCreate(
            company_id=company1.id,
            source="SEC",
            filing_number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
        )

        # First call should create
        filing1 = db.filings.get_or_create_filing(filing_data)
        assert filing1 is not None
        assert filing1.filing_number == "0000320193-25-000073"

        # Second call should retrieve existing
        filing2 = db.filings.get_or_create_filing(filing_data)
        assert filing2 is not None
        assert filing2.id == filing1.id

    def test_data_consistency(self, db):
        """Test data consistency across operations."""
        # Create company
        company = db.companies.get_or_create_company(
            CompanyCreate(ticker="NVDA", exchange="NASDAQ", name="NVIDIA Corp.")
        )

        # Create filing
        filing = db.filings.get_or_create_filing(
            FilingCreate(
                company_id=company.id,
                source="SEC",
                filing_number="0000320193-25-000073",
                form_type="10-Q",
                filing_date=date(2024, 12, 19),
                fiscal_period_end=date(2024, 9, 28),
                fiscal_year=2024,
                fiscal_quarter=4,
            )
        )

        # Create financial fact
        fact_id = db.financial_facts.insert_financial_fact(
            FinancialFactCreate(
                filing_id=filing.id,
                concept="us-gaap:Revenues",
                label="Revenues",
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period=PeriodType.Q,
            )
        )

        # Verify data consistency
        retrieved_company = db.companies.get_company_by_id(company.id)
        assert retrieved_company.ticker == "NVDA"

        retrieved_filing = db.filings.get_filing_by_id(filing.id)
        assert retrieved_filing.company_id == company.id
        assert retrieved_filing.form_type == "10-Q"

        retrieved_facts = db.financial_facts.get_financial_facts_by_filing(filing.id)
        assert len(retrieved_facts) >= 1
        assert any(f.id == fact_id for f in retrieved_facts)

    def test_error_handling(self, db):
        """Test error handling for invalid operations."""
        # Test inserting filing with non-existent company
        filing_data = FilingCreate(
            company_id=99999,  # Non-existent company
            source="SEC",
            filing_number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
        )

        # This should fail due to foreign key constraint
        filing_id = db.filings.insert_filing(filing_data)
        assert filing_id is None

        # Test inserting financial fact with non-existent filing
        fact_data = FinancialFactCreate(
            filing_id=99999,  # Non-existent filing
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period=PeriodType.Q,
        )

        # This should fail due to foreign key constraint
        fact_id = db.financial_facts.insert_financial_fact(fact_data)
        assert fact_id is None
