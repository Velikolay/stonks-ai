"""Tests for financial facts database operations."""

from datetime import date
from decimal import Decimal

from filings import (
    FilingCreate,
    FinancialFact,
    FinancialFactAbstract,
    FinancialFactCreate,
    PeriodType,
)


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
        assert fact.concept == sample_financial_fact.concept
        assert fact.label == sample_financial_fact.label
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
                concept="us-gaap:Revenues",
                label="Revenues",
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                abstracts=[
                    FinancialFactAbstract(concept="us-gaap:Revenues", label="Revenues"),
                ],
                period_end=date(2024, 9, 28),
                period_start=date(2024, 6, 30),
                period=PeriodType.Q,
            ),
            FinancialFactCreate(
                filing_id=filing.id,
                concept="us-gaap:NetIncomeLoss",
                label="Net Income (Loss)",
                value=Decimal("22956.0"),
                unit="USD",
                statement="Income Statement",
                abstracts=[
                    FinancialFactAbstract(
                        concept="us-gaap:NetIncomeLoss", label="Net Income (Loss)"
                    ),
                ],
                period_end=date(2024, 9, 28),
                period_start=date(2024, 6, 30),
                period=PeriodType.Q,
            ),
            FinancialFactCreate(
                filing_id=filing.id,
                concept="us-gaap:Assets",
                label="Total Assets",
                value=Decimal("352755.0"),
                unit="USD",
                statement="Balance Sheet",
                period_end=date(2024, 9, 28),
                period=PeriodType.Q,
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
        assert any(f.concept == "us-gaap:Revenues" for f in facts)
        assert any(f.concept == "us-gaap:NetIncomeLoss" for f in facts)
        assert any(f.concept == "us-gaap:Assets" for f in facts)

        # Verify abstracts for facts that have them
        revenue_fact = next(f for f in facts if f.concept == "us-gaap:Revenues")
        net_income_fact = next(f for f in facts if f.concept == "us-gaap:NetIncomeLoss")

        assert revenue_fact.abstracts is not None
        assert len(revenue_fact.abstracts) == 1
        assert revenue_fact.abstracts[0].concept == "us-gaap:Revenues"
        assert revenue_fact.abstracts[0].label == "Revenues"

        assert net_income_fact.abstracts is not None
        assert len(net_income_fact.abstracts) == 1
        assert net_income_fact.abstracts[0].concept == "us-gaap:NetIncomeLoss"
        assert net_income_fact.abstracts[0].label == "Net Income (Loss)"

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
                concept="us-gaap:Revenues",
                label="Revenues",
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period=PeriodType.Q,
            ),
            FinancialFactCreate(
                filing_id=filing.id,
                concept="us-gaap:NetIncomeLoss",
                label="Net Income (Loss)",
                value=Decimal("22956.0"),
                unit="USD",
                statement="Income Statement",
                period=PeriodType.Q,
            ),
        ]

        db.financial_facts.insert_financial_facts_batch(facts_data)

        # Retrieve facts
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)

        # Verify results
        assert len(facts) >= 2
        assert any(f.concept == "us-gaap:Revenues" for f in facts)
        assert any(f.concept == "us-gaap:NetIncomeLoss" for f in facts)
        assert all(f.filing_id == filing.id for f in facts)

    def test_get_financial_facts_by_filing_empty(self, db):
        """Test retrieving financial facts for filing with no facts."""
        facts = db.financial_facts.get_financial_facts_by_filing(99999)
        assert facts == []

    def test_get_financial_facts_by_concept(self, db, sample_company):
        """Test retrieving financial facts by company and concept."""
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
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period=PeriodType.Q,
        )

        fact2 = FinancialFactCreate(
            filing_id=filing2_obj.id,
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("81797.0"),
            unit="USD",
            statement="Income Statement",
            period=PeriodType.Q,
        )

        db.financial_facts.insert_financial_fact(fact1)
        db.financial_facts.insert_financial_fact(fact2)

        # Retrieve revenue facts
        facts = db.financial_facts.get_financial_facts_by_concept(
            company.id, "us-gaap:Revenues"
        )

        # Verify results
        assert len(facts) >= 2
        assert all(f.concept == "us-gaap:Revenues" for f in facts)
        assert all(f.filing_id in [filing1_obj.id, filing2_obj.id] for f in facts)

    def test_get_financial_facts_by_concept_limit(self, db, sample_company):
        """Test retrieving financial facts by concept with limit."""
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
                concept="us-gaap:Revenues",
                label="Revenues",
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period=PeriodType.Q,
            )

            db.financial_facts.insert_financial_fact(fact)

        # Retrieve revenue facts with limit
        facts = db.financial_facts.get_financial_facts_by_concept(
            company.id, "us-gaap:Revenues", limit=3
        )

        # Verify results
        assert len(facts) <= 3
        assert all(f.concept == "us-gaap:Revenues" for f in facts)

    def test_financial_fact_model_validation(self):
        """Test financial fact model validation."""
        # Valid financial fact
        fact = FinancialFactCreate(
            filing_id=1,
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
            period=PeriodType.Q,
        )
        assert fact.filing_id == 1
        assert fact.concept == "us-gaap:Revenues"
        assert fact.label == "Revenues"
        assert fact.value == Decimal("89498.0")
        assert fact.unit == "USD"
        assert fact.statement == "Income Statement"
        assert fact.period_end == date(2024, 9, 28)
        assert fact.period_start == date(2024, 6, 30)

        # Financial fact with ID (complete model)
        complete_fact = FinancialFact(
            id=1,
            filing_id=1,
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
            period=PeriodType.Q,
        )
        assert complete_fact.id == 1
        assert complete_fact.filing_id == 1
        assert complete_fact.concept == "us-gaap:Revenues"
        assert complete_fact.label == "Revenues"
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
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("89498.0"),
            unit="USD",
            axis="Segment",
            member="iPhone",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
            period=PeriodType.Q,
        )

        fact_id = db.financial_facts.insert_financial_fact(fact_data)

        # Retrieve and verify
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)
        fact = facts[0]

        assert fact.id == fact_id
        assert fact.axis == "Segment"
        assert fact.member == "iPhone"
        assert fact.statement == "Income Statement"

    def test_financial_fact_with_abstracts(
        self, db, sample_company, sample_filing, sample_financial_fact_with_abstracts
    ):
        """Test financial fact with abstracts."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing = db.filings.get_or_create_filing(sample_filing)
        sample_financial_fact_with_abstracts.filing_id = filing.id

        # Insert financial fact with abstracts
        fact_id = db.financial_facts.insert_financial_fact(
            sample_financial_fact_with_abstracts
        )

        # Verify fact was inserted
        assert fact_id is not None
        assert isinstance(fact_id, int)
        assert fact_id > 0

        # Retrieve and verify fact data
        fact = db.financial_facts.get_financial_facts_by_filing(filing.id)[0]
        assert fact is not None
        assert fact.id == fact_id
        assert fact.filing_id == filing.id
        assert fact.concept == sample_financial_fact_with_abstracts.concept
        assert fact.label == sample_financial_fact_with_abstracts.label
        assert fact.value == sample_financial_fact_with_abstracts.value
        assert fact.abstracts is not None
        assert len(fact.abstracts) == 2

        # Verify abstracts content
        assert fact.abstracts[0].concept == "us-gaap:Revenues"
        assert fact.abstracts[0].label == "Revenues"
        assert fact.abstracts[1].concept == "us-gaap:NetIncomeLoss"
        assert fact.abstracts[1].label == "Net Income (Loss)"

    def test_financial_fact_abstracts_model_validation(self):
        """Test financial fact abstract model validation."""
        # Valid abstract
        abstract = FinancialFactAbstract(concept="us-gaap:Revenues", label="Revenues")
        assert abstract.concept == "us-gaap:Revenues"
        assert abstract.label == "Revenues"

        # Test with different concepts
        abstract2 = FinancialFactAbstract(
            concept="us-gaap:NetIncomeLoss", label="Net Income (Loss)"
        )
        assert abstract2.concept == "us-gaap:NetIncomeLoss"
        assert abstract2.label == "Net Income (Loss)"

    def test_financial_fact_with_empty_abstracts(
        self, db, sample_company, sample_filing
    ):
        """Test financial fact with empty abstracts list."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing = db.filings.get_or_create_filing(sample_filing)

        # Create fact with empty abstracts
        fact_data = FinancialFactCreate(
            filing_id=filing.id,
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
            period=PeriodType.Q,
            abstracts=[],
        )

        fact_id = db.financial_facts.insert_financial_fact(fact_data)

        # Retrieve and verify
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)
        fact = facts[0]

        assert fact.id == fact_id
        assert fact.abstracts == []

    def test_financial_fact_with_none_abstracts(
        self, db, sample_company, sample_filing
    ):
        """Test financial fact with None abstracts."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing = db.filings.get_or_create_filing(sample_filing)

        # Create fact with None abstracts
        fact_data = FinancialFactCreate(
            filing_id=filing.id,
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
            period=PeriodType.Q,
            abstracts=None,
        )

        fact_id = db.financial_facts.insert_financial_fact(fact_data)

        # Retrieve and verify
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)
        fact = facts[0]

        assert fact.id == fact_id
        assert fact.abstracts is None

    def test_financial_fact_with_period_field(self, db, sample_company, sample_filing):
        """Test financial fact with period field."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        filing = db.filings.get_or_create_filing(sample_filing)

        # Test with YTD period
        fact_data_ytd = FinancialFactCreate(
            filing_id=filing.id,
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 1, 1),
            period=PeriodType.YTD,
        )

        fact_id_ytd = db.financial_facts.insert_financial_fact(fact_data_ytd)

        # Test with Q period
        fact_data_q = FinancialFactCreate(
            filing_id=filing.id,
            concept="us-gaap:NetIncomeLoss",
            label="Net Income (Loss)",
            value=Decimal("22956.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
            period=PeriodType.Q,
        )

        fact_id_q = db.financial_facts.insert_financial_fact(fact_data_q)

        # Test with Q period for assets
        fact_data_q_assets = FinancialFactCreate(
            filing_id=filing.id,
            concept="us-gaap:Assets",
            label="Total Assets",
            value=Decimal("352755.0"),
            unit="USD",
            statement="Balance Sheet",
            period_end=date(2024, 9, 28),
            period=PeriodType.Q,
        )

        fact_id_q_assets = db.financial_facts.insert_financial_fact(fact_data_q_assets)

        # Retrieve and verify facts
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)

        # Find the facts by concept
        revenue_fact = next(f for f in facts if f.concept == "us-gaap:Revenues")
        net_income_fact = next(f for f in facts if f.concept == "us-gaap:NetIncomeLoss")
        assets_fact = next(f for f in facts if f.concept == "us-gaap:Assets")

        # Verify period values
        assert revenue_fact.id == fact_id_ytd
        assert revenue_fact.period == PeriodType.YTD

        assert net_income_fact.id == fact_id_q
        assert net_income_fact.period == PeriodType.Q

        assert assets_fact.id == fact_id_q_assets
        assert assets_fact.period == PeriodType.Q

    def test_period_type_enum_values(self):
        """Test PeriodType enum values."""
        assert PeriodType.YTD == "YTD"
        assert PeriodType.Q == "Q"

        # Test enum creation from string
        assert PeriodType("YTD") == PeriodType.YTD
        assert PeriodType("Q") == PeriodType.Q

    def test_financial_fact_optional_period_field(self):
        """Test that FinancialFact period field is optional."""
        # Test FinancialFactCreate without period field should work (for balance sheet items)
        fact_create = FinancialFactCreate(
            filing_id=1,
            concept="us-gaap:Assets",
            label="Total Assets",
            value=Decimal("352755.0"),
            unit="USD",
            statement="Balance Sheet",
            period_end=date(2024, 9, 28),
            # period field intentionally omitted (balance sheet items don't have periods)
        )
        assert fact_create.period is None

        # Test FinancialFact without period field should work
        fact = FinancialFact(
            id=1,
            filing_id=1,
            concept="us-gaap:Assets",
            label="Total Assets",
            value=Decimal("352755.0"),
            unit="USD",
            statement="Balance Sheet",
            period_end=date(2024, 9, 28),
            # period field intentionally omitted
        )
        assert fact.period is None

        # Test FinancialFact with period field should work
        fact_with_period = FinancialFact(
            id=1,
            filing_id=1,
            concept="us-gaap:Revenues",
            label="Revenues",
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period_start=date(2024, 6, 30),
            period=PeriodType.Q,
        )
        assert fact_with_period.period == PeriodType.Q
