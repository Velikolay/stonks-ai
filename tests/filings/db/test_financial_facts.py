"""Tests for financial facts database operations."""

from datetime import date
from decimal import Decimal

from filings import FilingCreate, FinancialFact, FinancialFactCreate


class TestFinancialFactOperations:
    """Test financial facts database operations."""

    def test_insert_financial_fact(
        self, db, sample_company, sample_filing, sample_financial_fact
    ):
        """Test inserting a new financial fact."""
        # Create company and filing first
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing = db.filings.get_or_create_filing(sample_filing)
        sample_financial_fact.filing_id = filing.id

        # Insert financial fact
        fact_id = db.financial_facts.insert_financial_fact(sample_financial_fact)

        # Verify fact was inserted
        assert fact_id is not None
        assert isinstance(fact_id, int)
        assert fact_id > 0

        # Retrieve and verify fact data
        fact = db.financial_facts.get_financial_facts_by_filing(filing.id)[0]
        assert fact is not None
        assert fact.id == fact_id
        assert fact.filing_id == filing.id
        assert fact.metric == sample_financial_fact.metric
        assert fact.value == sample_financial_fact.value
        assert fact.unit == sample_financial_fact.unit
        assert fact.statement == sample_financial_fact.statement

    def test_insert_financial_facts_batch(self, db, sample_company, sample_filing):
        """Test inserting multiple financial facts."""
        # Create company and filing first
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing = db.filings.get_or_create_filing(sample_filing)

        # Create multiple facts
        facts_data = [
            FinancialFactCreate(
                filing_id=filing.id,
                metric="Revenue",
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period_start=date(2024, 6, 30),
            ),
            FinancialFactCreate(
                filing_id=filing.id,
                metric="Net Income",
                value=Decimal("22956.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period_start=date(2024, 6, 30),
            ),
            FinancialFactCreate(
                filing_id=filing.id,
                metric="Total Assets",
                value=Decimal("352755.0"),
                unit="USD",
                statement="Balance Sheet",
                period_end=date(2024, 9, 28),
            ),
        ]

        # Insert facts
        fact_ids = db.financial_facts.insert_financial_facts_batch(facts_data)

        # Verify facts were inserted
        assert len(fact_ids) == 3
        assert all(isinstance(fid, int) for fid in fact_ids)
        assert all(fid > 0 for fid in fact_ids)

        # Retrieve and verify facts
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)
        assert len(facts) >= 3
        assert any(f.metric == "Revenue" for f in facts)
        assert any(f.metric == "Net Income" for f in facts)
        assert any(f.metric == "Total Assets" for f in facts)

    def test_get_financial_facts_by_filing(self, db, sample_company, sample_filing):
        """Test retrieving financial facts by filing."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing = db.filings.get_or_create_filing(sample_filing)

        # Create multiple facts
        facts_data = [
            FinancialFactCreate(
                filing_id=filing.id,
                metric="Revenue",
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
            ),
            FinancialFactCreate(
                filing_id=filing.id,
                metric="Net Income",
                value=Decimal("22956.0"),
                unit="USD",
                statement="Income Statement",
            ),
        ]

        db.financial_facts.insert_financial_facts_batch(facts_data)

        # Retrieve facts
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)

        # Verify results
        assert len(facts) >= 2
        assert any(f.metric == "Revenue" for f in facts)
        assert any(f.metric == "Net Income" for f in facts)
        assert all(f.filing_id == filing.id for f in facts)

    def test_get_financial_facts_by_filing_empty(self, db):
        """Test retrieving financial facts for filing with no facts."""
        facts = db.financial_facts.get_financial_facts_by_filing(99999)
        assert facts == []

    def test_get_financial_facts_by_metric(self, db, sample_company):
        """Test retrieving financial facts by company and metric."""
        # Create company
        company = db.companies.get_or_create_company(sample_company)

        # Create multiple filings with revenue facts
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
            form_type="10-Q",
            filing_date=date(2024, 6, 15),
            fiscal_period_end=date(2024, 3, 30),
            fiscal_year=2024,
            fiscal_quarter=2,
        )

        filing1_obj = db.filings.get_or_create_filing(filing1)
        filing2_obj = db.filings.get_or_create_filing(filing2)

        # Create revenue facts for both filings
        fact1 = FinancialFactCreate(
            filing_id=filing1_obj.id,
            metric="Revenue",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
        )

        fact2 = FinancialFactCreate(
            filing_id=filing2_obj.id,
            metric="Revenue",
            value=Decimal("81797.0"),
            unit="USD",
            statement="Income Statement",
        )

        db.financial_facts.insert_financial_fact(fact1)
        db.financial_facts.insert_financial_fact(fact2)

        # Retrieve revenue facts
        facts = db.financial_facts.get_financial_facts_by_metric(company.id, "Revenue")

        # Verify results
        assert len(facts) >= 2
        assert all(f.metric == "Revenue" for f in facts)
        assert all(f.filing_id in [filing1_obj.id, filing2_obj.id] for f in facts)

    def test_get_financial_facts_by_metric_limit(self, db, sample_company):
        """Test retrieving financial facts by metric with limit."""
        # Create company
        company = db.companies.get_or_create_company(sample_company)

        # Create multiple filings with revenue facts
        for i in range(5):
            filing = FilingCreate(
                company_id=company.id,
                source="SEC",
                filing_number=f"0000320193-25-00007{i}",
                form_type="10-Q",
                filing_date=date(2024, 12, 19),
                fiscal_period_end=date(2024, 9, 28),
                fiscal_year=2024,
                fiscal_quarter=4,
            )

            filing_obj = db.filings.get_or_create_filing(filing)

            fact = FinancialFactCreate(
                filing_id=filing_obj.id,
                metric="Revenue",
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
            )

            db.financial_facts.insert_financial_fact(fact)

        # Retrieve revenue facts with limit
        facts = db.financial_facts.get_financial_facts_by_metric(
            company.id, "Revenue", limit=3
        )

        # Verify results
        assert len(facts) <= 3
        assert all(f.metric == "Revenue" for f in facts)

    def test_financial_fact_model_validation(self):
        """Test financial fact model validation."""
        # Valid financial fact
        fact = FinancialFactCreate(
            filing_id=1,
            metric="Revenue",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
        )
        assert fact.filing_id == 1
        assert fact.metric == "Revenue"
        assert fact.value == Decimal("89498.0")
        assert fact.unit == "USD"
        assert fact.statement == "Income Statement"
        assert fact.period_end == date(2024, 9, 28)
        assert fact.period_start == date(2024, 6, 30)

        # Financial fact with ID (complete model)
        complete_fact = FinancialFact(
            id=1,
            filing_id=1,
            metric="Revenue",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
        )
        assert complete_fact.id == 1
        assert complete_fact.filing_id == 1
        assert complete_fact.metric == "Revenue"
        assert complete_fact.value == Decimal("89498.0")

    def test_financial_fact_with_optional_fields(
        self, db, sample_company, sample_filing
    ):
        """Test financial fact with optional fields."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing = db.filings.get_or_create_filing(sample_filing)

        # Create fact with optional fields
        fact_data = FinancialFactCreate(
            filing_id=filing.id,
            metric="Revenue",
            value=Decimal("89498.0"),
            unit="USD",
            axis="Segment",
            member="iPhone",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
        )

        fact_id = db.financial_facts.insert_financial_fact(fact_data)

        # Retrieve and verify
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)
        fact = facts[0]

        assert fact.id == fact_id
        assert fact.axis == "Segment"
        assert fact.member == "iPhone"
        assert fact.statement == "Income Statement"
