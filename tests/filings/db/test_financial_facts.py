"""Tests for financial facts database operations."""

import uuid
from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from filings import FilingCreate, FinancialFact, FinancialFactCreate, PeriodType


class TestFinancialFactOperations:
    """Test financial facts database operations."""

    def _ensure_registry_id(self, db, *, company_id: int) -> int:
        registry_id = db.companies.get_or_create_filing_registry_id(
            company_id=company_id,
            registry="SEC",
            number=str(company_id).zfill(10),  # CIK (unique per company for tests)
            status="active",
        )
        assert registry_id is not None
        return int(registry_id)

    def test_insert_financial_fact(
        self, db, sample_company, sample_filing, sample_financial_fact
    ):
        """Test inserting a new financial fact."""
        # Create company and filing first
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        sample_filing.registry_id = self._ensure_registry_id(db, company_id=company.id)
        filing = db.filings.get_or_create_filing(sample_filing)
        sample_financial_fact.company_id = company.id
        sample_financial_fact.filing_id = filing.id
        sample_financial_fact.form_type = filing.form_type

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
        sample_filing.registry_id = self._ensure_registry_id(db, company_id=company.id)
        filing = db.filings.get_or_create_filing(sample_filing)

        # Create multiple facts
        facts_data = [
            FinancialFactCreate(
                key=str(uuid.uuid4()),
                company_id=company.id,
                filing_id=filing.id,
                form_type=filing.form_type,
                concept="us-gaap:Revenues",
                label="Revenues",
                is_abstract=False,
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period=PeriodType.Q,
            ),
            FinancialFactCreate(
                key=str(uuid.uuid4()),
                company_id=company.id,
                filing_id=filing.id,
                form_type=filing.form_type,
                concept="us-gaap:NetIncomeLoss",
                label="Net Income (Loss)",
                is_abstract=False,
                value=Decimal("22956.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period=PeriodType.Q,
            ),
            FinancialFactCreate(
                key=str(uuid.uuid4()),
                company_id=company.id,
                filing_id=filing.id,
                form_type=filing.form_type,
                concept="us-gaap:Assets",
                label="Total Assets",
                is_abstract=False,
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

    def test_insert_financial_facts_with_abstract(
        self, db, sample_company, sample_filing
    ):
        """Test inserting multiple financial facts with abstract."""
        # Create company and filing first
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        sample_filing.registry_id = self._ensure_registry_id(db, company_id=company.id)
        filing = db.filings.get_or_create_filing(sample_filing)

        abstract_key = str(uuid.uuid4())

        # Create multiple facts
        facts_data = [
            FinancialFactCreate(
                key=str(uuid.uuid4()),
                abstract_key=abstract_key,
                company_id=company.id,
                filing_id=filing.id,
                form_type=filing.form_type,
                concept="us-gaap:Revenues",
                label="Revenues",
                is_abstract=False,
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period=PeriodType.Q,
            ),
            FinancialFactCreate(
                key=str(uuid.uuid4()),
                abstract_key=abstract_key,
                company_id=company.id,
                filing_id=filing.id,
                form_type=filing.form_type,
                concept="us-gaap:NetIncomeLoss",
                label="Net Income (Loss)",
                is_abstract=False,
                value=Decimal("22956.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period=PeriodType.Q,
            ),
            FinancialFactCreate(
                key=abstract_key,
                company_id=company.id,
                filing_id=filing.id,
                form_type=filing.form_type,
                concept="us-gaap:IncomeStatementAbstract",
                label="Income Statement",
                is_abstract=True,
                statement="Income Statement",
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
        assert any(f.concept == "us-gaap:IncomeStatementAbstract" for f in facts)
        assert any(f.concept == "us-gaap:Revenues" for f in facts)
        assert any(f.concept == "us-gaap:NetIncomeLoss" for f in facts)

        abstract_fact = next(
            f for f in facts if f.concept == "us-gaap:IncomeStatementAbstract"
        )
        revenues_fact = next(f for f in facts if f.concept == "us-gaap:Revenues")
        net_income_fact = next(f for f in facts if f.concept == "us-gaap:NetIncomeLoss")

        assert revenues_fact.abstract_id == abstract_fact.id
        assert net_income_fact.abstract_id == abstract_fact.id

    def test_get_financial_facts_by_filing(self, db, sample_company, sample_filing):
        """Test retrieving financial facts by filing."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        sample_filing.registry_id = self._ensure_registry_id(db, company_id=company.id)
        filing = db.filings.get_or_create_filing(sample_filing)

        # Create multiple facts
        facts_data = [
            FinancialFactCreate(
                key=str(uuid.uuid4()),
                company_id=company.id,
                filing_id=filing.id,
                form_type=filing.form_type,
                concept="us-gaap:Revenues",
                label="Revenues",
                is_abstract=False,
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period=PeriodType.Q,
            ),
            FinancialFactCreate(
                key=str(uuid.uuid4()),
                company_id=company.id,
                filing_id=filing.id,
                form_type=filing.form_type,
                concept="us-gaap:NetIncomeLoss",
                label="Net Income (Loss)",
                is_abstract=False,
                value=Decimal("22956.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
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
        registry_id = self._ensure_registry_id(db, company_id=company.id)
        filing1 = FilingCreate(
            company_id=company.id,
            registry_id=registry_id,
            registry="SEC",
            number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
        )

        filing2 = FilingCreate(
            company_id=company.id,
            registry_id=registry_id,
            registry="SEC",
            number="0000320193-25-000074",
            form_type="10-Q",
            filing_date=date(2024, 6, 15),
            fiscal_period_end=date(2024, 3, 30),
            fiscal_year=2024,
            fiscal_quarter=2,
        )

        filing1_obj = db.filings.get_or_create_filing(filing1)
        filing2_obj = db.filings.get_or_create_filing(filing2)

        # Create revenue facts for both filings
        fact1 = FinancialFact(
            id=1,
            company_id=company.id,
            filing_id=filing1_obj.id,
            form_type=filing1_obj.form_type,
            concept="us-gaap:Revenues",
            label="Revenues",
            is_abstract=False,
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period=PeriodType.Q,
        )

        fact2 = FinancialFact(
            id=2,
            company_id=company.id,
            filing_id=filing2_obj.id,
            form_type=filing2_obj.form_type,
            concept="us-gaap:Revenues",
            label="Revenues",
            is_abstract=False,
            value=Decimal("81797.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
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
        registry_id = self._ensure_registry_id(db, company_id=company.id)

        # Create multiple filings with revenue facts
        for i in range(5):
            filing = FilingCreate(
                company_id=company.id,
                registry_id=registry_id,
                registry="SEC",
                number=f"0000320193-25-00007{i}",
                form_type="10-Q",
                filing_date=date(2024, 12, 19),
                fiscal_period_end=date(2024, 9, 28),
                fiscal_year=2024,
                fiscal_quarter=4,
            )

            filing_obj = db.filings.get_or_create_filing(filing)

            fact = FinancialFact(
                id=1,
                company_id=company.id,
                filing_id=filing_obj.id,
                form_type=filing_obj.form_type,
                concept="us-gaap:Revenues",
                label="Revenues",
                is_abstract=False,
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
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
            key=str(uuid.uuid4()),
            company_id=1,
            filing_id=1,
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
        assert fact.filing_id == 1
        assert fact.concept == "us-gaap:Revenues"
        assert fact.label == "Revenues"
        assert fact.is_abstract is False
        assert fact.value == Decimal("89498.0")
        assert fact.unit == "USD"
        assert fact.statement == "Income Statement"
        assert fact.period_end == date(2024, 9, 28)

        # Valid financial fact abstract
        fact_abstract = FinancialFactCreate(
            key=str(uuid.uuid4()),
            company_id=1,
            filing_id=1,
            form_type="10-Q",
            concept="us-gaap:Revenues",
            label="Revenues",
            is_abstract=True,
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period=PeriodType.Q,
        )
        assert fact_abstract.value is None
        assert fact_abstract.unit is None

        # Financial fact with ID (complete model)
        complete_fact = FinancialFact(
            id=1,
            company_id=1,
            filing_id=1,
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
        assert complete_fact.id == 1
        assert complete_fact.filing_id == 1
        assert complete_fact.concept == "us-gaap:Revenues"
        assert complete_fact.label == "Revenues"
        assert complete_fact.is_abstract is False
        assert complete_fact.value == Decimal("89498.0")

        # Financial fact abstract with ID (complete model)
        complete_fact_abstract = FinancialFact(
            id=1,
            company_id=1,
            filing_id=1,
            form_type="10-Q",
            concept="us-gaap:Revenues",
            label="Revenues",
            is_abstract=True,
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period=PeriodType.Q,
        )
        assert complete_fact_abstract.value is None
        assert complete_fact_abstract.unit is None

    def test_financial_fact_create_requires_value_when_not_abstract(self):
        """Ensure FinancialFactCreate enforces value for non-abstract facts."""
        with pytest.raises(ValidationError) as exc:
            FinancialFactCreate(
                key=str(uuid.uuid4()),
                company_id=1,
                filing_id=1,
                form_type="10-Q",
                concept="us-gaap:Revenues",
                label="Revenues",
                is_abstract=False,
                value=None,
                statement="Income Statement",
                period_end=date(2024, 9, 28),
            )

        assert "value cannot be None when is_abstract is False" in str(exc.value)

    def test_financial_fact_requires_value_when_not_abstract(self):
        """Ensure FinancialFact enforces value for non-abstract facts."""
        with pytest.raises(ValidationError) as exc:
            FinancialFact(
                id=1,
                parent_id=None,
                abstract_id=None,
                company_id=1,
                filing_id=1,
                form_type="10-Q",
                concept="us-gaap:Revenues",
                label="Revenues",
                is_abstract=False,
                value=None,
                statement="Income Statement",
                period_end=date(2024, 9, 28),
            )

        assert "value cannot be None when is_abstract is False" in str(exc.value)

    def test_financial_fact_with_optional_fields(
        self, db, sample_company, sample_filing
    ):
        """Test financial fact with optional fields."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        sample_filing.registry_id = self._ensure_registry_id(db, company_id=company.id)
        filing = db.filings.get_or_create_filing(sample_filing)

        # Create fact with optional fields
        fact_data = FinancialFact(
            id=0,
            company_id=company.id,
            filing_id=filing.id,
            form_type=filing.form_type,
            concept="us-gaap:Revenues",
            label="Revenues",
            is_abstract=False,
            value=Decimal("89498.0"),
            unit="USD",
            axis="Segment",
            member="iPhone",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
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

    def test_financial_fact_with_period_field(self, db, sample_company, sample_filing):
        """Test financial fact with period field."""
        # Create company and filing
        company = db.companies.get_or_create_company(sample_company)
        sample_filing.company_id = company.id
        sample_filing.registry_id = self._ensure_registry_id(db, company_id=company.id)
        filing = db.filings.get_or_create_filing(sample_filing)

        # Test with YTD period
        fact_data_ytd = FinancialFact(
            id=0,
            company_id=company.id,
            filing_id=filing.id,
            form_type=filing.form_type,
            concept="us-gaap:Revenues",
            label="Revenues",
            is_abstract=False,
            value=Decimal("89498.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period=PeriodType.YTD,
        )

        fact_id_ytd = db.financial_facts.insert_financial_fact(fact_data_ytd)

        # Test with Q period
        fact_data_q = FinancialFact(
            id=0,
            company_id=company.id,
            filing_id=filing.id,
            form_type=filing.form_type,
            concept="us-gaap:NetIncomeLoss",
            label="Net Income (Loss)",
            is_abstract=False,
            value=Decimal("22956.0"),
            unit="USD",
            statement="Income Statement",
            period_end=date(2024, 9, 28),
            period=PeriodType.Q,
        )

        fact_id_q = db.financial_facts.insert_financial_fact(fact_data_q)

        # Test with Q period for assets
        fact_data_q_assets = FinancialFact(
            id=0,
            company_id=company.id,
            filing_id=filing.id,
            form_type=filing.form_type,
            concept="us-gaap:Assets",
            label="Total Assets",
            is_abstract=False,
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
            key=str(uuid.uuid4()),
            company_id=1,
            filing_id=1,
            form_type="10-Q",
            concept="us-gaap:Assets",
            label="Total Assets",
            is_abstract=False,
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
            company_id=1,
            filing_id=1,
            form_type="10-Q",
            concept="us-gaap:Assets",
            label="Total Assets",
            is_abstract=False,
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
            company_id=1,
            filing_id=1,
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
        assert fact_with_period.period == PeriodType.Q
