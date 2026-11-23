"""Tests for SEC 10-Q parser."""

from datetime import date
from decimal import Decimal
from unittest.mock import Mock, patch

import pandas as pd

from filings.models import PeriodType
from filings.parsers.sec_xbrl import (
    SECXBRLParser,
    _filed_unit,
    fiscal_focus,
    reporting_currency_amounts,
)


class TestSECXBRLParser:
    """Test the SEC XBRL parser."""

    def test_fiscal_focus_reads_the_cover_facts(self):
        """The cover year and period are the fiscal year and quarter."""
        assert fiscal_focus({"fiscal_year": "2025", "fiscal_period": "Q1"}) == (2025, 1)
        assert fiscal_focus({"fiscal_year": "2025", "fiscal_period": "FY"}) == (2025, 4)
        assert fiscal_focus({"fiscal_year": "2024", "fiscal_period": "q3"}) == (2024, 3)

    def test_fiscal_focus_missing_cover_is_unset(self):
        """A filing without the cover facts leaves both values unset."""
        assert fiscal_focus(None) == (None, None)
        assert fiscal_focus({}) == (None, None)
        assert fiscal_focus({"fiscal_year": "2025"}) == (2025, None)
        assert fiscal_focus({"fiscal_period": "H1"}) == (None, None)

    def test_parse_disaggregated_revenues_product(self):
        """Test parsing disaggregated revenues by product."""
        parser = SECXBRLParser()

        # Mock XBRL query result for product revenue
        mock_revenue_df = pd.DataFrame(
            {
                "concept": ["Revenue", "Revenue", "Revenue"],
                "label": ["Contract Revenue", "Contract Revenue", "Contract Revenue"],
                "value": [1000000, 2000000, 3000000],
                "period_start": ["2024-01-01", "2024-01-01", "2024-01-01"],
                "period_end": ["2024-03-31", "2024-03-31", "2024-03-31"],
                "dim_srt_ProductOrServiceAxis": ["iPhone", "Mac", "iPad"],
            }
        )

        # Mock XBRL object with simpler setup
        mock_xbrl = Mock()
        mock_query = Mock()
        mock_query.by_concept.return_value = mock_query
        mock_query.by_dimension.return_value = mock_query
        mock_query.to_dataframe.return_value = mock_revenue_df
        mock_xbrl.query.return_value = mock_query

        # Mock the _create_disaggregated_metric_fact method to return expected facts
        with patch.object(parser, "_create_disaggregated_metric_fact") as mock_create:
            mock_facts = []
            for i, product in enumerate(
                [
                    ("aapl:IPhoneMember", "iPhone"),
                    ("aapl:MacMember", "Mac"),
                    ("aapl:IPadMember", "iPad"),
                ]
            ):
                mock_fact = Mock()
                mock_fact.concept = "Revenue"
                mock_fact.member = product[0]
                mock_fact.member_label = product[1]
                mock_fact.value = Decimal(str(1000000 + i * 1000000))
                mock_fact.axis = "srt:ProductOrServiceAxis"
                mock_fact.statement = "Income Statement"
                mock_fact.label = "Contract Revenue"
                mock_facts.append(mock_fact)

            mock_create.side_effect = mock_facts

            facts = parser._parse_disaggregated_revenues(mock_xbrl)

            assert len(facts) == 3
            assert facts[0].concept == "Revenue"
            assert facts[0].member == "aapl:IPhoneMember"
            assert facts[0].value == Decimal("1000000")
            assert facts[0].axis == "srt:ProductOrServiceAxis"
            assert facts[0].member_label == "iPhone"
            assert facts[0].statement == "Income Statement"
            assert facts[0].label == "Contract Revenue"

    def test_parse_disaggregated_revenues_geographic(self):
        """Test parsing disaggregated revenues by geographic region."""
        parser = SECXBRLParser()

        # Mock XBRL query result for geographic revenue
        mock_revenue_df = pd.DataFrame(
            {
                "concept": ["Revenue", "Revenue"],
                "label": ["Contract Revenue", "Contract Revenue"],
                "numeric_value": [5000000, 3000000],
                "period_start": ["2024-01-01", "2024-01-01"],
                "period_end": ["2024-03-31", "2024-03-31"],
                "dim_srt_StatementGeographicalAxis": [
                    "aapl:AmericasMember",
                    "aapl:EuropeMember",
                ],
                "dimension_member_label": ["Americas", "Europe"],
            }
        )

        # Mock XBRL object
        mock_xbrl = Mock()
        mock_query = Mock()
        mock_query.by_concept.return_value = mock_query
        mock_query.by_dimension.return_value = mock_query
        mock_query.to_dataframe.return_value = mock_revenue_df
        mock_xbrl.query.return_value = mock_query

        # Mock empty results for other dimensions
        empty_df = pd.DataFrame()
        mock_query.by_concept.side_effect = lambda concept: mock_query
        mock_query.by_dimension.side_effect = lambda dimension: mock_query
        mock_query.to_dataframe.side_effect = lambda: (
            empty_df
            if "StatementGeographicalAxis" not in str(mock_query.by_dimension.call_args)
            else mock_revenue_df
        )

        facts = parser._parse_disaggregated_revenues(mock_xbrl)

        assert len(facts) == 2
        assert facts[0].concept == "Revenue"
        assert facts[0].member == "aapl:AmericasMember"
        assert facts[0].member_label == "Americas"
        assert facts[0].value == Decimal("5000000")
        assert facts[0].axis == "srt:StatementGeographicalAxis"
        assert facts[0].statement == "Income Statement"

    def test_create_disaggregated_revenue_fact(self):
        """Test creating disaggregated revenue fact."""
        parser = SECXBRLParser()

        # Mock row data
        row = {
            "concept": "Revenue",
            "label": "Contract Revenue",
            "numeric_value": 1000000,
            "period_start": "2024-01-01",
            "period_end": "2024-03-31",
            "dim_srt_ProductOrServiceAxis": "aapl:IPhoneMember",
            "dimension_member_label": "iPhone",
        }

        fact = parser._create_disaggregated_metric_fact(
            row,
            comparative_row=None,
            metric="Revenue",
            dimension="srt:ProductOrServiceAxis",
            form_type="10-Q",
            position=0,
        )

        assert fact.concept == "Revenue"
        assert fact.value == Decimal("1000000")
        assert fact.member == "aapl:IPhoneMember"
        assert fact.axis == "srt:ProductOrServiceAxis"
        assert fact.member_label == "iPhone"
        assert fact.statement == "Income Statement"
        assert fact.label == "Contract Revenue"
        # Regression test: ensure period field is set
        assert fact.period is not None
        assert fact.period in ["YTD", "Q"]  # PeriodType values

    def test_create_disaggregated_revenue_fact_invalid_value(self):
        """Test creating disaggregated revenue fact with invalid value."""
        parser = SECXBRLParser()

        # Mock row data with invalid value
        row = {
            "concept": "Revenue",
            "label": "Contract Revenue",
            "value": "invalid",
            "period_start": "2024-01-01",
            "period_end": "2024-03-31",
            "dim_srt_ProductOrServiceAxis": "iPhone",
        }

        fact = parser._create_disaggregated_metric_fact(
            row,
            comparative_row=None,
            metric="Revenue",
            dimension="srt:ProductOrServiceAxis",
            form_type="10-Q",
            position=0,
        )

        assert fact is None

    def test_parser_requires_period_field_regression(self):
        """Regression test: ensure parser creates FinancialFact with period field."""
        parser = SECXBRLParser()

        # Mock row data
        row = {
            "concept": "Revenue",
            "label": "Contract Revenue",
            "numeric_value": 1000000,
            "period_start": "2024-01-01",
            "period_end": "2024-03-31",
            "dim_srt_ProductOrServiceAxis": "iPhone",
        }

        fact = parser._create_disaggregated_metric_fact(
            row,
            comparative_row=None,
            metric="Revenue",
            dimension="srt:ProductOrServiceAxis",
            form_type="10-Q",
            position=0,
        )

        # This test ensures that if someone accidentally removes the period field
        # from the parser's FinancialFact creation, this test will fail
        assert fact is not None, "Parser should create a valid FinancialFact"
        assert hasattr(fact, "period"), "FinancialFact should have period field"
        assert fact.period is not None, "Period field should not be None"
        assert fact.period in [
            "YTD",
            "Q",
        ], f"Period should be YTD or Q, got {fact.period}"

    def test_create_financial_fact_with_hierarchy_requires_period(self):
        """Test that _create_financial_fact_with_hierarchy includes period field."""
        parser = SECXBRLParser()

        # Mock row data
        row = {
            "concept": "us-gaap:Revenues",
            "label": "Revenues",
            "2024-03-31 (Q1)": 1000000,  # Period column with value
            "2023-03-31 (Q1)": 500000,  # Period column with value
            "abstract": False,
        }

        fact = parser._create_financial_fact_with_hierarchy(
            row=row,
            statement_type="Income Statement",
            period_col="2024-03-31 (Q1)",
            comparative_period_col="2023-03-31 (Q1)",
            position=0,
        )

        # Verify the fact was created successfully
        assert fact is not None, "Parser should create a valid FinancialFact"
        assert fact.concept == "us-gaap:Revenues"
        assert fact.value == Decimal("1000000")
        assert fact.comparative_value == Decimal("500000")
        assert fact.statement == "Income Statement"

        # Regression test: ensure period field is set (for income statement items)
        assert hasattr(fact, "period"), "FinancialFact should have period field"
        assert (
            fact.period is not None
        ), "Period field should not be None for income statement items"
        assert fact.period in [
            "YTD",
            "Q",
        ], f"Period should be YTD or Q, got {fact.period}"

    def test_reporting_currency_keeps_the_majority_currency(self):
        """A convenience translation does not replace the reporting currency."""
        facts = pd.DataFrame(
            [
                {
                    "concept": "ifrs-full:Revenue",
                    "numeric_value": 1_339_255_000_000,
                    "currency": "TWD",
                    "unit_ref": "Unit_TWD",
                    "period_end": "2020-12-31",
                    "is_dimensioned": False,
                },
                {
                    "concept": "ifrs-full:Revenue",
                    "numeric_value": 47_694_300_000,
                    "currency": "USD",
                    "unit_ref": "Unit_USD",
                    "period_end": "2020-12-31",
                    "is_dimensioned": False,
                },
                {
                    "concept": "ifrs-full:Equity",
                    "numeric_value": 1_614_387_000_000,
                    "currency": "TWD",
                    "unit_ref": "Unit_TWD",
                    "period_end": None,
                    "period_instant": "2019-12-31",
                    "is_dimensioned": False,
                },
                {
                    "concept": "ifrs-full:Equity",
                    "numeric_value": 53_974_800_000,
                    "currency": "USD",
                    "unit_ref": "Unit_USD",
                    "period_end": None,
                    "period_instant": "2019-12-31",
                    "is_dimensioned": False,
                },
                {
                    "concept": "ifrs-full:BasicEarningsLossPerShare",
                    "numeric_value": 13.65,
                    "currency": "TWD",
                    "unit_ref": "Unit_TWD_per_Share",
                    "period_end": "2019-12-31",
                    "is_dimensioned": False,
                },
                {
                    "concept": "ifrs-full:Revenue",
                    "numeric_value": 9,
                    "currency": "TWD",
                    "unit_ref": "Unit_TWD",
                    "period_end": "2020-12-31",
                    "is_dimensioned": True,
                },
            ]
        )

        amounts = reporting_currency_amounts(facts)

        assert amounts[("ifrs-full:Revenue", date(2020, 12, 31))] == (
            Decimal("1339255000000"),
            "twd",
        )
        assert amounts[("ifrs-full:Equity", date(2019, 12, 31))][0] == Decimal(
            "1614387000000"
        )
        assert (
            amounts[("ifrs-full:BasicEarningsLossPerShare", date(2019, 12, 31))][1]
            == "twdPerShare"
        )
        assert ("ifrs-full:Revenue", date(2020, 12, 31)) in amounts

    def test_reporting_currency_is_unused_for_one_currency(self):
        """A filing that reports one currency needs no replacement."""
        facts = pd.DataFrame(
            [
                {
                    "concept": "us-gaap:Revenues",
                    "numeric_value": 10,
                    "currency": "USD",
                    "unit_ref": "USD",
                    "period_end": date(2024, 9, 28),
                    "is_dimensioned": False,
                }
            ]
        )
        assert reporting_currency_amounts(facts) == {}
        assert reporting_currency_amounts(None) == {}

    def test_filed_unit_is_stored_unchanged(self):
        """The raw unit ref is what gets stored. A blank ref uses the default."""
        assert _filed_unit("iso4217_usd") == "iso4217_usd"
        assert _filed_unit("unit_twd") == "unit_twd"
        assert _filed_unit("unit12") == "unit12"
        assert _filed_unit(None, "usd") == "usd"
        assert _filed_unit("  ", "usd") == "usd"
        assert _filed_unit(float("nan")) is None

    def test_face_fact_uses_reporting_currency_when_the_frame_does_not(self):
        """The current-year convenience translation is replaced on the face row."""
        parser = SECXBRLParser()
        parser._reporting_amounts = {
            ("ifrs-full:Revenue", date(2020, 12, 31)): (
                Decimal("1339255000000"),
                "twd",
            )
        }
        row = pd.Series(
            {
                "concept": "ifrs-full_Revenue",
                "label": "NET REVENUE",
                "2020-12-31 (FY)": 47694300000,
                "2019-12-31 (FY)": 1069985000000,
                "unit": "unit_usd",
                "abstract": False,
                "weight": 1,
                "parent_concept": float("nan"),
                "parent_abstract_concept": float("nan"),
            }
        )

        fact = parser._create_financial_fact_with_hierarchy(
            row=row,
            statement_type="Income Statement",
            period_col="2020-12-31 (FY)",
            comparative_period_col="2019-12-31 (FY)",
            position=0,
        )

        assert fact is not None
        assert fact.value == Decimal("1339255000000")
        assert fact.unit == "twd"
        assert fact.comparative_value == Decimal("1069985000000")

    def test_face_fact_keeps_the_filed_unit(self):
        """A unit ref is stored as written when the amount is not replaced."""
        parser = SECXBRLParser()
        row = pd.Series(
            {
                "concept": "us-gaap_Revenues",
                "label": "Revenue",
                "2024-09-28 (FY)": 100,
                "unit": "iso4217_usd",
                "abstract": False,
                "weight": 1,
                "parent_concept": float("nan"),
                "parent_abstract_concept": float("nan"),
            }
        )

        fact = parser._create_financial_fact_with_hierarchy(
            row=row,
            statement_type="Income Statement",
            period_col="2024-09-28 (FY)",
            comparative_period_col=None,
            position=0,
        )

        assert fact is not None
        assert fact.unit == "iso4217_usd"

    def test_determine_period_type_treats_20f_as_annual(self):
        """A 20-F is an annual report, so its duration facts are year-to-date."""
        parser = SECXBRLParser()
        period_end = date(2024, 12, 31)
        period_start = date(2024, 1, 1)
        assert (
            parser._determine_period_type(period_start, period_end, "20-F")
            == PeriodType.YTD
        )
        assert (
            parser._determine_period_type(period_start, period_end, "20-F/A")
            == PeriodType.YTD
        )

    def test_determine_period_type_from_column_validation(self):
        """Test that _determine_period_type_from_column handles various inputs correctly."""
        parser = SECXBRLParser()

        # Test with empty period column (should return None for balance sheet items)
        result = parser._determine_period_type_from_column("", "Income Statement")
        assert result is None

        # Test with period column missing ISO date (should return None for balance sheet items)
        result = parser._determine_period_type_from_column(
            "Some Random Text", "Income Statement"
        )
        assert result is None

        # Test with period column missing ISO date (only quarter indicator)
        result = parser._determine_period_type_from_column("Q1", "Income Statement")
        assert result is None

        # Test with invalid date format (should return None for balance sheet items)
        result = parser._determine_period_type_from_column(
            "2024/03/31 (Q1)", "Income Statement"
        )
        assert result is None

        # Test with invalid date format (MM-DD-YYYY)
        result = parser._determine_period_type_from_column(
            "03-31-2024 (Q1)", "Income Statement"
        )
        assert result is None

    def test_determine_period_type_from_column_valid_inputs(self):
        """Test that _determine_period_type_from_column works with valid inputs."""
        parser = SECXBRLParser()

        # Test with valid quarter period column
        result = parser._determine_period_type_from_column(
            "2024-03-31 (Q1)", "Income Statement"
        )
        assert result == "Q"

        # Test with valid quarter period column (different format)
        result = parser._determine_period_type_from_column(
            "2024-06-30 Q2", "Income Statement"
        )
        assert result == "Q"

        # Duration CFS / IS facts use the column heuristic
        result = parser._determine_period_type_from_column(
            "2024-12-31", "Cash Flow Statement", point_in_time=False
        )
        assert result == "YTD"

        result = parser._determine_period_type_from_column(
            "2024-12-31 (YTD)", "Cash Flow Statement", point_in_time=False
        )
        assert result == "YTD"

        result = parser._determine_period_type_from_column(
            "Q3 2024-09-30", "Cash Flow Statement", point_in_time=False
        )
        assert result == "Q"

        result = parser._determine_period_type_from_column(
            "2024-03-31 (Q1)", "Balance Sheet"
        )
        assert result is None

        result = parser._determine_period_type_from_column(
            "2024-03-31", "Balance Sheet"
        )
        assert result is None

        # Instant facts from edgartools point_in_time must not inherit CFS YTD/Q
        result = parser._determine_period_type_from_column(
            "2024-06-30", "Cash Flow Statement", point_in_time=True
        )
        assert result is None

        result = parser._determine_period_type_from_column(
            "2024-06-30", "Statement of Equity", point_in_time=True
        )
        assert result is None

        result = parser._determine_period_type_from_column(
            "2024-06-30", "Cash Flow Statement", point_in_time=False
        )
        assert result == "YTD"

    def test_is_column_mostly_empty(self):
        """Test that _is_column_mostly_empty correctly identifies empty columns."""
        parser = SECXBRLParser()

        # Create a test DataFrame
        test_data = {
            "concept": ["us-gaap:Revenues", "us-gaap:Assets", "us-gaap:Liabilities"],
            "label": ["Revenues", "Assets", "Liabilities"],
            "good_column": [1000000, 2000000, 3000000],  # 100% non-null
            "mostly_empty": [1000000, None, None],  # 33% non-null
            "completely_empty": [None, None, None],  # 0% non-null
            "empty_strings": [
                1000000,
                "",
                "",
            ],  # 33% non-empty (empty strings count as empty)
        }

        df = pd.DataFrame(test_data)

        # Test good column (should not be mostly empty)
        assert not parser._is_column_mostly_empty(df, "good_column", threshold=20.0)
        assert not parser._is_column_mostly_empty(df, "good_column", threshold=50.0)

        # Test mostly empty column (33% non-null)
        assert not parser._is_column_mostly_empty(
            df, "mostly_empty", threshold=20.0
        )  # 33% > 20%, so not mostly empty
        assert parser._is_column_mostly_empty(
            df, "mostly_empty", threshold=40.0
        )  # 33% < 40%, so mostly empty

        # Test completely empty column
        assert parser._is_column_mostly_empty(df, "completely_empty", threshold=20.0)
        assert not parser._is_column_mostly_empty(
            df, "completely_empty", threshold=0.0
        )  # 0% is not < 0%

        # Test column with empty strings (should be treated same as None)
        assert not parser._is_column_mostly_empty(
            df, "empty_strings", threshold=20.0
        )  # 33% > 20%, so not mostly empty
        assert parser._is_column_mostly_empty(
            df, "empty_strings", threshold=40.0
        )  # 33% < 40%, so mostly empty

        # Test non-existent column
        assert parser._is_column_mostly_empty(df, "non_existent_column", threshold=20.0)

        # Test with empty DataFrame
        empty_df = pd.DataFrame()
        assert parser._is_column_mostly_empty(empty_df, "any_column", threshold=20.0)

        # Test with None DataFrame
        assert parser._is_column_mostly_empty(None, "any_column", threshold=20.0)

    def test_parse_disaggregated_revenues_empty_data(self):
        """Test parsing disaggregated revenues with empty data."""
        parser = SECXBRLParser()

        # Mock XBRL object with empty results
        mock_xbrl = Mock()
        mock_query = Mock()
        mock_query.by_concept.return_value = mock_query
        mock_query.by_dimension.return_value = mock_query
        mock_query.to_dataframe.return_value = pd.DataFrame()
        mock_xbrl.query.return_value = mock_query

        facts = parser._parse_disaggregated_revenues(mock_xbrl)

        assert len(facts) == 0

    def test_parse_disaggregated_revenues_business_segment(self):
        """Test parsing disaggregated revenues by business segments (geographic regions)."""
        parser = SECXBRLParser()

        # Mock XBRL query result for business segment revenue
        mock_revenue_df = pd.DataFrame(
            {
                "concept": ["Revenue", "Revenue", "Revenue"],
                "label": ["Contract Revenue", "Contract Revenue", "Contract Revenue"],
                "numeric_value": [4000000, 2000000, 1000000],
                "period_start": ["2024-01-01", "2024-01-01", "2024-01-01"],
                "period_end": ["2024-03-31", "2024-03-31", "2024-03-31"],
                "dim_us-gaap_StatementBusinessSegmentsAxis": [
                    "Americas",
                    "Europe",
                    "AsiaPacific",
                ],
            }
        )

        # Mock XBRL object
        mock_xbrl = Mock()
        mock_query = Mock()
        mock_query.by_concept.return_value = mock_query
        mock_query.by_dimension.return_value = mock_query
        mock_query.to_dataframe.return_value = mock_revenue_df
        mock_xbrl.query.return_value = mock_query

        # Mock empty results for other dimensions
        empty_df = pd.DataFrame()
        mock_query.by_concept.side_effect = lambda concept: mock_query
        mock_query.by_dimension.side_effect = lambda dimension: mock_query
        mock_query.to_dataframe.side_effect = lambda: (
            empty_df
            if "StatementBusinessSegmentsAxis"
            not in str(mock_query.by_dimension.call_args)
            else mock_revenue_df
        )

        facts = parser._parse_disaggregated_revenues(mock_xbrl)

        assert len(facts) == 3

        # Check that all expected regions are present (order doesn't matter)
        members = [fact.member for fact in facts]
        assert "Americas" in members
        assert "Europe" in members
        assert "AsiaPacific" in members

        # Check that all facts have the correct structure
        for fact in facts:
            assert fact.concept == "Revenue"
            assert fact.axis == "us-gaap:StatementBusinessSegmentsAxis"
            assert fact.statement == "Income Statement"
            assert isinstance(fact.value, Decimal)
            assert fact.value > 0

    def test_parse_disaggregated_metrics_operating_income(self):
        """Test parsing disaggregated operating income by product."""
        parser = SECXBRLParser()

        # Mock XBRL query result for operating income by product
        mock_operating_income_df = pd.DataFrame(
            {
                "concept": ["OperatingIncome", "OperatingIncome", "OperatingIncome"],
                "label": ["Operating Income", "Operating Income", "Operating Income"],
                "value": [500000, 300000, 200000],
                "period_start": ["2024-01-01", "2024-01-01", "2024-01-01"],
                "period_end": ["2024-03-31", "2024-03-31", "2024-03-31"],
                "dim_srt_ProductOrServiceAxis": ["iPhone", "Mac", "iPad"],
            }
        )

        # Mock XBRL object
        mock_xbrl = Mock()
        mock_query = Mock()
        mock_query.by_concept.return_value = mock_query
        mock_query.by_dimension.return_value = mock_query
        mock_query.to_dataframe.return_value = mock_operating_income_df
        mock_xbrl.query.return_value = mock_query

        # Mock the _create_disaggregated_metric_fact method to return expected facts
        with patch.object(parser, "_create_disaggregated_metric_fact") as mock_create:
            mock_facts = []
            for i, product in enumerate(["iPhone", "Mac", "iPad"]):
                mock_fact = Mock()
                mock_fact.concept = "OperatingIncome"
                mock_fact.member = product
                mock_fact.value = Decimal(str(500000 - i * 100000))
                mock_fact.axis = "srt:ProductOrServiceAxis"
                mock_fact.statement = "Disaggregated OperatingIncome (Product)"
                mock_fact.label = f"Operating Income - {product}"
                mock_facts.append(mock_fact)

            mock_create.side_effect = mock_facts

            facts = parser._parse_disaggregated_metrics(mock_xbrl, "OperatingIncome")

            assert len(facts) == 3
            assert facts[0].concept == "OperatingIncome"
            assert facts[0].member == "iPhone"
            assert facts[0].value == Decimal("500000")
            assert facts[0].axis == "srt:ProductOrServiceAxis"
            assert facts[0].statement == "Disaggregated OperatingIncome (Product)"
            assert facts[0].label == "Operating Income - iPhone"

    def test_statement_fact_keeps_instance_value_and_print_sign(self):
        """Instance amount stays in value; preferred_sign is print_sign."""
        parser = SECXBRLParser()
        row = pd.Series(
            {
                "concept": "us-gaap_InterestExpenseNonoperating",
                "label": "Interest expense",
                "abstract": False,
                "unit": "usd",
                "2026-09-30 (YTD)": 622463000.0,
                "2025-09-30 (YTD)": 542115000.0,
                "weight": -1.0,
                "preferred_sign": -1.0,
                "point_in_time": False,
            }
        )

        fact = parser._create_financial_fact_with_hierarchy(
            row,
            "Income Statement",
            "2026-09-30 (YTD)",
            "2025-09-30 (YTD)",
            1,
        )

        assert fact is not None
        assert fact.value == Decimal("622463000.0")
        assert fact.comparative_value == Decimal("542115000.0")
        assert fact.weight == Decimal("-1.0")
        assert fact.print_sign == Decimal("-1.0")

    def test_statement_fact_rounds_binary_float_noise(self):
        """Float artifacts longer than five decimals still become a fact."""
        parser = SECXBRLParser()
        row = pd.Series(
            {
                "concept": "us-gaap_EarningsPerShareDiluted",
                "label": "Diluted earnings per share",
                "abstract": False,
                "unit": "usdPerShare",
                "2023-09-30 (FY)": 54.72,
                "2022-09-30 (FY)": 54.650000000000006,
                "weight": 1.0,
                "preferred_sign": 1.0,
                "point_in_time": False,
            }
        )

        fact = parser._create_financial_fact_with_hierarchy(
            row,
            "Income Statement",
            "2023-09-30 (FY)",
            "2022-09-30 (FY)",
            1,
        )

        assert fact is not None
        assert fact.value == Decimal("54.72")
        assert fact.comparative_value == Decimal("54.65000")

    def test_supplement_segment_dimensions_fills_note_only_members(self):
        """Segment-note members are added when the face statement lacks them."""
        parser = SECXBRLParser()
        face = Mock()
        face.concept = "us-gaap:Revenues"
        face.axis = ""
        face.member = ""
        face.label = "Revenue"
        face.value = Decimal("100")
        face.is_abstract = False
        face.period = PeriodType.YTD
        face.period_end = date(2022, 1, 30)

        already = Mock()
        already.concept = "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"
        already.axis = "srt:ProductOrServiceAxis"
        already.member = "nvda:GamingMember"
        already.label = "Revenue"
        already.period_end = date(2022, 1, 30)

        frame = pd.DataFrame(
            {
                "concept": [
                    "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax",
                    "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax",
                    "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax",
                    "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax",
                ],
                "label": ["Revenue", "Gaming", "Data Center", "Customer A"],
                "abstract": [False, False, False, False],
                "dimension": [False, True, True, True],
                "dimension_axis": [
                    None,
                    "srt:ProductOrServiceAxis",
                    "srt:ProductOrServiceAxis",
                    "srt:MajorCustomersAxis",
                ],
                "dimension_member": [
                    None,
                    "nvda_GamingMember",
                    "nvda_DataCenterMember",
                    "nvda_CustomerAMember",
                ],
                "dimension_member_label": [None, "Gaming", "Data Center", "Customer A"],
                "unit": ["usd", "usd", "usd", "usd"],
                "weight": [1.0, 1.0, 1.0, 1.0],
                "preferred_sign": [1.0, 1.0, 1.0, 1.0],
                "point_in_time": [False, False, False, False],
                "2022-01-30 (FY)": [100.0, 40.0, 60.0, 25.0],
            }
        )
        statement = Mock()
        statement.to_dataframe.return_value = frame
        available = pd.DataFrame(
            {
                "category": ["disclosure", "disclosure"],
                "name": [
                    "Segment Information - Schedule of Revenue by Market (Details)",
                    "Debt - Schedule of Debt (Details)",
                ],
                "role_name": ["SegmentRevenue", "DebtSchedule"],
            }
        )
        xbrl = Mock()
        xbrl.statements.list_available.return_value = available
        xbrl.statements.get.return_value = statement

        added = parser._supplement_segment_dimensions(xbrl, [face, already])

        assert [fact.member for fact in added] == ["nvda:DataCenterMember"]
        assert added[0].axis == "srt:ProductOrServiceAxis"
        assert added[0].concept == "us-gaap:Revenues"
        assert added[0].label == "Revenue"
        assert added[0].value == Decimal("60.0")
        xbrl.statements.get.assert_called_once_with("SegmentRevenue")

    def test_supplement_accepts_revenue_concept_with_different_capitalization(self):
        """A company extension of the revenue concept joins the face line."""
        parser = SECXBRLParser()
        face = Mock()
        face.concept = "us-gaap:Revenues"
        face.axis = ""
        face.member = ""
        face.label = "Revenue"
        face.value = Decimal("100")
        face.is_abstract = False
        face.period = PeriodType.Q
        face.period_end = date(2018, 3, 31)

        frame = pd.DataFrame(
            {
                "concept": [
                    "spgi_RevenuefromContractwithCustomerExcludingAssessedTax",
                    "spgi_RevenuefromContractwithCustomerExcludingAssessedTax",
                    "spgi_SomeOtherRevenueMember",
                ],
                "label": ["Subscription", "Non-transaction", "Consulting"],
                "abstract": [False, False, False],
                "dimension": [True, True, True],
                "dimension_axis": [
                    "us-gaap:ProductOrServiceAxis",
                    "us-gaap:ProductOrServiceAxis",
                    "us-gaap:ProductOrServiceAxis",
                ],
                "dimension_member": [
                    "spgi_SubscriptionMember",
                    "spgi_NonTransactionMember",
                    "spgi_ConsultingMember",
                ],
                "dimension_member_label": [
                    "Subscription",
                    "Non-transaction",
                    "Consulting",
                ],
                "unit": ["usd", "usd", "usd"],
                "weight": [1.0, 1.0, 1.0],
                "preferred_sign": [1.0, 1.0, 1.0],
                "point_in_time": [False, False, False],
                "2018-03-31 (Q1)": [60.0, 40.0, 15.0],
            }
        )
        statement = Mock()
        statement.to_dataframe.return_value = frame
        available = pd.DataFrame(
            {
                "category": ["disclosure"],
                "name": ["Revenue Disaggregated by Revenue Type (Details)"],
                "role_name": ["RevenueType"],
            }
        )
        xbrl = Mock()
        xbrl.statements.list_available.return_value = available
        xbrl.statements.get.return_value = statement

        added = parser._supplement_segment_dimensions(xbrl, [face])
        by_member = {fact.member: fact for fact in added}

        assert set(by_member) == {
            "spgi:SubscriptionMember",
            "spgi:NonTransactionMember",
        }
        assert by_member["spgi:SubscriptionMember"].concept == "us-gaap:Revenues"
        assert by_member["spgi:SubscriptionMember"].label == "Revenue"
        assert by_member["spgi:SubscriptionMember"].value == Decimal("60.0")
        assert by_member["spgi:NonTransactionMember"].value == Decimal("40.0")

    def test_supplement_keeps_revenue_components_as_their_own_members(self):
        """Advertising lines are kept; other sales on the segment total is split out."""
        parser = SECXBRLParser()
        face = Mock()
        face.concept = "us-gaap:Revenues"
        face.axis = ""
        face.member = ""
        face.label = "Revenues"
        face.value = Decimal("100")
        face.period_end = date(2016, 12, 31)

        segment_total = Mock()
        segment_total.concept = "us-gaap:Revenues"
        segment_total.axis = "us-gaap:StatementBusinessSegmentsAxis"
        segment_total.member = "goog:GoogleInc.Member"
        segment_total.label = "Revenues"
        segment_total.period_end = date(2016, 12, 31)

        frame = pd.DataFrame(
            {
                "concept": [
                    "us-gaap_AdvertisingRevenue",
                    "us-gaap_AdvertisingRevenue",
                    "us-gaap_OtherSalesRevenueNet",
                ],
                "label": [
                    "Google properties",
                    "Google",
                    "Google",
                ],
                "abstract": [False, False, False],
                "dimension": [True, True, True],
                "dimension_axis": [
                    "us-gaap:ProductOrServiceAxis",
                    "us-gaap:StatementBusinessSegmentsAxis",
                    "us-gaap:StatementBusinessSegmentsAxis",
                ],
                "dimension_member": [
                    "goog_GooglePropertiesMember",
                    "goog_GoogleInc.Member",
                    "goog_GoogleInc.Member",
                ],
                "dimension_member_label": [
                    "Google properties",
                    "Google",
                    "Google",
                ],
                "unit": ["usd", "usd", "usd"],
                "weight": [1.0, 1.0, 1.0],
                "preferred_sign": [1.0, 1.0, 1.0],
                "point_in_time": [False, False, False],
                "2016-12-31 (FY)": [63.0, 79.0, 10.0],
            }
        )
        statement = Mock()
        statement.to_dataframe.return_value = frame
        available = pd.DataFrame(
            {
                "category": ["disclosure"],
                "name": ["Revenue by Revenue Source (Details)"],
                "role_name": ["RevenueSource"],
            }
        )
        xbrl = Mock()
        xbrl.statements.list_available.return_value = available
        xbrl.statements.get.return_value = statement

        added = parser._supplement_segment_dimensions(xbrl, [face, segment_total])
        by_member = {fact.member: fact for fact in added}

        assert set(by_member) == {"goog:GooglePropertiesMember", "Other sales"}
        assert by_member["goog:GooglePropertiesMember"].concept == "us-gaap:Revenues"
        assert by_member["goog:GooglePropertiesMember"].label == "Revenues"
        assert by_member["goog:GooglePropertiesMember"].value == Decimal("63.0")
        assert by_member["Other sales"].value == Decimal("10.0")
        assert by_member["Other sales"].member_label == "Other sales"

    def test_supplement_keeps_only_the_undimensioned_plug(self):
        """The row that closes the members to revenue is kept; other note lines are not."""
        parser = SECXBRLParser()
        face = Mock()
        face.concept = "us-gaap:Revenues"
        face.axis = ""
        face.member = ""
        face.label = "Revenues"
        face.value = Decimal("100")
        face.is_abstract = False
        face.period = PeriodType.YTD
        face.period_end = date(2024, 12, 31)

        frame = pd.DataFrame(
            {
                "concept": [
                    "us-gaap_Revenues",
                    "us-gaap_Revenues",
                    "us-gaap_Revenues",
                    "us-gaap_RevenueNotFromContractWithCustomer",
                    "us-gaap_PaymentsToAcquirePropertyPlantAndEquipment",
                    "us-gaap_RevenuesAbstract",
                ],
                "label": [
                    "Revenues",
                    "United States",
                    "EMEA",
                    "Hedging gains (losses) included in consolidated revenue",
                    "Capital expenditures",
                    "Revenue categories",
                ],
                "abstract": [False, False, False, False, False, True],
                "dimension": [False, True, True, False, False, False],
                "dimension_axis": [
                    None,
                    "srt:StatementGeographicalAxis",
                    "srt:StatementGeographicalAxis",
                    None,
                    None,
                    None,
                ],
                "dimension_member": [
                    None,
                    "country:US",
                    "us-gaap:EMEAMember",
                    None,
                    None,
                    None,
                ],
                "dimension_member_label": [
                    None,
                    "United States",
                    "EMEA",
                    None,
                    None,
                    None,
                ],
                "unit": ["usd", "usd", "usd", "usd", "usd", None],
                "weight": [1.0, 1.0, 1.0, 1.0, 1.0, None],
                "preferred_sign": [1.0, 1.0, 1.0, 1.0, 1.0, None],
                "point_in_time": [False, False, False, False, False, False],
                "2024-12-31 (FY)": [100.0, 60.0, 43.0, -3.0, 999.0, None],
                "parent_concept": [None, None, None, None, None, None],
                "parent_abstract_concept": [None, None, None, None, None, None],
            }
        )
        statement = Mock()
        statement.to_dataframe.return_value = frame
        available = pd.DataFrame(
            {
                "category": ["disclosure"],
                "name": ["Revenue by Geographic Area (Details)"],
                "role_name": ["GeographicRevenue"],
            }
        )
        xbrl = Mock()
        xbrl.statements.list_available.return_value = available
        xbrl.statements.get.return_value = statement

        added = parser._supplement_segment_dimensions(xbrl, [face])
        undimensioned = [fact for fact in added if not fact.axis]

        assert len(undimensioned) == 1
        assert undimensioned[0].concept == "us-gaap:RevenueNotFromContractWithCustomer"
        assert undimensioned[0].value == Decimal("-3.0")
        assert (
            undimensioned[0].label
            == "Hedging gains (losses) included in consolidated revenue"
        )

    def test_supplement_keeps_extension_axis_that_sums_to_revenue(self):
        """A company axis is kept when its revenue members equal face revenue."""
        parser = SECXBRLParser()
        face = Mock()
        face.concept = "us-gaap:Revenues"
        face.axis = ""
        face.member = ""
        face.label = "Revenues"
        face.value = Decimal("100")
        face.is_abstract = False
        face.period = PeriodType.Q
        face.period_end = date(2012, 3, 31)

        axis = (
            "goog:RevenueFromExternalCustomersAndLongLivedAssets"
            "AttributedToGeographicAreasAxis"
        )
        frame = pd.DataFrame(
            {
                "concept": [
                    "us-gaap_SalesRevenueNet",
                    "us-gaap_SalesRevenueNet",
                    "us-gaap_SalesRevenueNet",
                ],
                "label": ["United States", "United Kingdom", "Rest of the world"],
                "abstract": [False, False, False],
                "dimension": [True, True, True],
                "dimension_axis": [axis, axis, axis],
                "dimension_member": [
                    "us-gaap_SegmentGeographicalGroupsOfCountriesGroupOneMember",
                    "us-gaap_SegmentGeographicalGroupsOfCountriesGroupTwoMember",
                    "goog_SegmentGeographicalGroupsOfCountriesGroupThreeMember",
                ],
                "dimension_member_label": [
                    "United States",
                    "United Kingdom",
                    "Rest of the world",
                ],
                "unit": ["usd", "usd", "usd"],
                "weight": [1.0, 1.0, 1.0],
                "preferred_sign": [1.0, 1.0, 1.0],
                "point_in_time": [False, False, False],
                "2012-03-31 (Q1)": [50.0, 10.0, 40.0],
                "parent_concept": [None, None, None],
                "parent_abstract_concept": [None, None, None],
            }
        )
        statement = Mock()
        statement.to_dataframe.return_value = frame
        available = pd.DataFrame(
            {
                "category": ["disclosure"],
                "name": ["Revenues by Geographic Area (Detail)"],
                "role_name": ["GeographicRevenue"],
            }
        )
        xbrl = Mock()
        xbrl.statements.list_available.return_value = available
        xbrl.statements.get.return_value = statement

        added = parser._supplement_segment_dimensions(xbrl, [face])
        by_member = {fact.member_label: fact for fact in added}

        assert set(by_member) == {
            "United States",
            "United Kingdom",
            "Rest of the world",
        }
        assert by_member["United States"].concept == "us-gaap:Revenues"
        assert by_member["United States"].label == "Revenues"
        assert by_member["United States"].axis == axis
        assert by_member["United States"].value == Decimal("50")
        assert by_member["Rest of the world"].value == Decimal("40")

    def test_supplement_drops_extension_axis_that_misses_revenue(self):
        """An extension axis that does not add up to face revenue is left out."""
        parser = SECXBRLParser()
        face = Mock()
        face.concept = "us-gaap:Revenues"
        face.axis = ""
        face.member = ""
        face.label = "Revenues"
        face.value = Decimal("100")
        face.is_abstract = False
        face.period = PeriodType.Q
        face.period_end = date(2012, 3, 31)

        frame = pd.DataFrame(
            {
                "concept": ["us-gaap_SalesRevenueNet", "us-gaap_SalesRevenueNet"],
                "label": ["United States", "United Kingdom"],
                "abstract": [False, False],
                "dimension": [True, True],
                "dimension_axis": ["dei:LegalEntityAxis", "dei:LegalEntityAxis"],
                "dimension_member": ["country_US", "country_GB"],
                "dimension_member_label": ["United States", "United Kingdom"],
                "unit": ["usd", "usd"],
                "weight": [1.0, 1.0],
                "preferred_sign": [1.0, 1.0],
                "point_in_time": [False, False],
                "2012-03-31 (Q1)": [60.0, 30.0],
                "parent_concept": [None, None],
                "parent_abstract_concept": [None, None],
            }
        )
        statement = Mock()
        statement.to_dataframe.return_value = frame
        available = pd.DataFrame(
            {
                "category": ["disclosure"],
                "name": ["Revenues by Geographic Area (Detail)"],
                "role_name": ["GeographicRevenue"],
            }
        )
        xbrl = Mock()
        xbrl.statements.list_available.return_value = available
        xbrl.statements.get.return_value = statement

        assert parser._supplement_segment_dimensions(xbrl, [face]) == []

    def test_supplement_keeps_undimensioned_revenue_components(self):
        """Advertising and other fees that sum to revenue are kept as filed."""
        parser = SECXBRLParser()
        face = Mock()
        face.concept = "us-gaap:Revenues"
        face.axis = ""
        face.member = ""
        face.label = "Revenue"
        face.value = Decimal("100")
        face.is_abstract = False
        face.period = PeriodType.YTD
        face.period_end = date(2017, 12, 31)

        frame = pd.DataFrame(
            {
                "concept": [
                    "us-gaap_AdvertisingRevenue",
                    "us-gaap_OtherSalesRevenueNet",
                    "us-gaap_Revenues",
                    "us-gaap_Revenues",
                ],
                "label": [
                    "Advertising",
                    "Payments and other fees",
                    "United States",
                    "Rest of the world",
                ],
                "abstract": [False, False, False, False],
                "dimension": [False, False, True, True],
                "dimension_axis": [
                    None,
                    None,
                    "us-gaap:StatementGeographicalAxis",
                    "us-gaap:StatementGeographicalAxis",
                ],
                "dimension_member": [
                    None,
                    None,
                    "country_US",
                    "fb_RestOfWorldMember",
                ],
                "dimension_member_label": [
                    None,
                    None,
                    "United States",
                    "Rest of the world",
                ],
                "unit": ["usd", "usd", "usd", "usd"],
                "weight": [1.0, 1.0, 1.0, 1.0],
                "preferred_sign": [1.0, 1.0, 1.0, 1.0],
                "point_in_time": [False, False, False, False],
                "2017-12-31 (FY)": [90.0, 10.0, 40.0, 60.0],
                "parent_concept": [None, None, None, None],
                "parent_abstract_concept": [None, None, None, None],
            }
        )
        statement = Mock()
        statement.to_dataframe.return_value = frame
        available = pd.DataFrame(
            {
                "category": ["disclosure"],
                "name": ["Revenue Recognition (Details)"],
                "role_name": ["RevenueRecognition"],
            }
        )
        xbrl = Mock()
        xbrl.statements.list_available.return_value = available
        xbrl.statements.get.return_value = statement

        added = parser._supplement_segment_dimensions(xbrl, [face])
        components = {fact.concept: fact for fact in added if not fact.axis}

        assert set(components) == {
            "us-gaap:AdvertisingRevenue",
            "us-gaap:OtherSalesRevenueNet",
        }
        assert components["us-gaap:AdvertisingRevenue"].label == "Advertising"
        assert components["us-gaap:AdvertisingRevenue"].value == Decimal("90.0")
        assert components["us-gaap:AdvertisingRevenue"].member in (None, "")
        assert (
            components["us-gaap:OtherSalesRevenueNet"].label
            == "Payments and other fees"
        )
        assert components["us-gaap:OtherSalesRevenueNet"].value == Decimal("10.0")

    def test_supplement_leaves_components_when_product_split_exists(self):
        """A product split already on the face is not repeated from the note."""
        parser = SECXBRLParser()
        face = Mock()
        face.concept = "us-gaap:Revenues"
        face.axis = ""
        face.member = ""
        face.label = "Revenue"
        face.value = Decimal("100")
        face.is_abstract = False
        face.period = PeriodType.YTD
        face.period_end = date(2018, 12, 31)

        advertising = Mock()
        advertising.concept = "us-gaap:Revenues"
        advertising.axis = "srt:ProductOrServiceAxis"
        advertising.member = "us-gaap:AdvertisingMember"
        advertising.member_label = "Advertising"
        advertising.label = "Revenue"
        advertising.value = Decimal("90")
        advertising.is_abstract = False
        advertising.period = PeriodType.YTD
        advertising.period_end = date(2018, 12, 31)

        other = Mock()
        other.concept = "us-gaap:Revenues"
        other.axis = "srt:ProductOrServiceAxis"
        other.member = "us-gaap:ServiceOtherMember"
        other.member_label = "Other revenue"
        other.label = "Revenue"
        other.value = Decimal("10")
        other.is_abstract = False
        other.period = PeriodType.YTD
        other.period_end = date(2018, 12, 31)

        frame = pd.DataFrame(
            {
                "concept": [
                    "us-gaap_AdvertisingRevenue",
                    "us-gaap_OtherSalesRevenueNet",
                ],
                "label": ["Advertising", "Payments and other fees"],
                "abstract": [False, False],
                "dimension": [False, False],
                "dimension_axis": [None, None],
                "dimension_member": [None, None],
                "dimension_member_label": [None, None],
                "unit": ["usd", "usd"],
                "weight": [1.0, 1.0],
                "preferred_sign": [1.0, 1.0],
                "point_in_time": [False, False],
                "2018-12-31 (FY)": [90.0, 10.0],
                "parent_concept": [None, None],
                "parent_abstract_concept": [None, None],
            }
        )
        statement = Mock()
        statement.to_dataframe.return_value = frame
        available = pd.DataFrame(
            {
                "category": ["disclosure"],
                "name": ["Revenue Recognition (Details)"],
                "role_name": ["RevenueRecognition"],
            }
        )
        xbrl = Mock()
        xbrl.statements.list_available.return_value = available
        xbrl.statements.get.return_value = statement

        added = parser._supplement_segment_dimensions(xbrl, [face, advertising, other])

        assert added == []

    def test_face_revenue_keeps_the_published_quarter(self):
        """A printed quarter is stored beside the year-to-date revenue total."""
        parser = SECXBRLParser()
        frame = pd.DataFrame(
            {
                "concept": ["us-gaap_Revenues"],
                "label": ["Revenues"],
                "abstract": [False],
                "dimension": [False],
                "parent_concept": [None],
                "parent_abstract_concept": [None],
                "unit": ["usd"],
                "weight": [1.0],
                "preferred_sign": [1.0],
                "point_in_time": [False],
                "2025-09-30 (Q3)": [102346.0],
                "2025-09-30 (YTD)": [289007.0],
            }
        )

        facts = parser._parse_statement(frame, "Income Statement", False)
        by_period = {fact.period: fact for fact in facts}

        assert by_period[PeriodType.Q].value == Decimal("102346.0")
        assert by_period[PeriodType.YTD].value == Decimal("289007.0")
        assert by_period[PeriodType.Q].key != by_period[PeriodType.YTD].key

    def test_face_eps_keeps_the_published_quarter(self):
        """A printed per-share quarter is stored beside the year-to-date total."""
        parser = SECXBRLParser()
        frame = pd.DataFrame(
            {
                "concept": ["us-gaap_EarningsPerShareDiluted"],
                "label": ["Diluted earnings per share"],
                "abstract": [False],
                "dimension": [False],
                "parent_concept": [None],
                "parent_abstract_concept": [None],
                "unit": ["usdPerShare"],
                "weight": [1.0],
                "preferred_sign": [1.0],
                "point_in_time": [False],
                "2024-08-04 (Q3)": [-0.40],
                "2024-08-04 (YTD)": [0.33],
            }
        )

        facts = parser._parse_statement(frame, "Income Statement", False)
        by_period = {fact.period: fact for fact in facts}

        assert by_period[PeriodType.Q].value == Decimal("-0.40")
        assert by_period[PeriodType.YTD].value == Decimal("0.33")
        assert by_period[PeriodType.Q].key != by_period[PeriodType.YTD].key

    def test_dimension_suffix_rewrites_only_reused_members(self):
        """A shared parent member takes the label suffix; a distinct member does not."""
        parser = SECXBRLParser()
        period = "2025-03-29 (Q1)"
        frame = pd.DataFrame(
            {
                "concept": [
                    "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax"
                ]
                * 4,
                "label": [
                    "Operating Segments - Data Center",
                    "Operating Segments - Embedded",
                    "Client - Client and Gaming",
                    "Gaming - Client and Gaming",
                ],
                "abstract": [False] * 4,
                "dimension": [True] * 4,
                "dimension_axis": [
                    "srt:ConsolidationItemsAxis",
                    "srt:ConsolidationItemsAxis",
                    "srt:ProductOrServiceAxis",
                    "srt:ProductOrServiceAxis",
                ],
                "dimension_member": [
                    "us-gaap_OperatingSegmentsMember",
                    "us-gaap_OperatingSegmentsMember",
                    "amd_ClientMember",
                    "amd_GamingMember",
                ],
                "dimension_member_label": [
                    "Operating Segments",
                    "Operating Segments",
                    "Client",
                    "Gaming",
                ],
                "unit": ["usd"] * 4,
                "weight": [1.0] * 4,
                "preferred_sign": [1.0] * 4,
                "point_in_time": [False] * 4,
                period: [3674.0, 823.0, 2294.0, 647.0],
            }
        )

        facts = parser._parse_statement(frame, "Income Statement", True)
        by_member = {fact.member: fact.member_label for fact in facts}

        assert by_member["Data Center"] == "Data Center"
        assert by_member["Embedded"] == "Embedded"
        assert by_member["amd:ClientMember"] == "Client"
        assert by_member["amd:GamingMember"] == "Gaming"

    def test_dimension_metadata_is_kept_beside_the_display_member(self):
        """Raw coordinates stay on the fact after the display member collapses."""
        parser = SECXBRLParser()
        period = "2025-03-29 (Q1)"
        metadata = [
            {
                "dimension": "srt:ConsolidationItemsAxis",
                "member": "us-gaap_OperatingSegmentsMember",
                "member_label": "Operating Segments",
            },
            {
                "dimension": "srt:ProductOrServiceAxis",
                "member": "nvda_DataCenterMember",
                "member_label": "Data Center",
            },
        ]
        frame = pd.DataFrame(
            {
                "concept": [
                    "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax"
                ]
                * 2,
                "label": [
                    "Operating Segments - Data Center",
                    "Operating Segments - Embedded",
                ],
                "abstract": [False, False],
                "dimension": [True, True],
                "dimension_axis": ["srt:ConsolidationItemsAxis"] * 2,
                "dimension_member": ["us-gaap_OperatingSegmentsMember"] * 2,
                "dimension_member_label": ["Operating Segments"] * 2,
                "dimension_metadata": [metadata, metadata[:1]],
                "unit": ["usd", "usd"],
                "weight": [1.0, 1.0],
                "preferred_sign": [1.0, 1.0],
                "point_in_time": [False, False],
                period: [3674.0, 823.0],
            }
        )

        facts = parser._parse_statement(frame, "Income Statement", True)
        data_center = next(fact for fact in facts if fact.member == "Data Center")

        assert [(item.axis, item.member) for item in data_center.dimensions] == [
            ("srt:ConsolidationItemsAxis", "us-gaap:OperatingSegmentsMember"),
            ("srt:ProductOrServiceAxis", "nvda:DataCenterMember"),
        ]

    def test_dimension_suffix_keeps_parent_when_suffixes_collide(self):
        """Two reused parents that share a child name stay on different members."""
        parser = SECXBRLParser()
        period = "2025-12-31 (FY)"
        frame = pd.DataFrame(
            {
                "concept": [
                    "us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax"
                ]
                * 4,
                "label": [
                    "Operating segments - Ratings",
                    "Operating segments - Indices",
                    "Intersegment Elimination - Ratings",
                    "Intersegment Elimination - Indices",
                ],
                "abstract": [False] * 4,
                "dimension": [True] * 4,
                "dimension_axis": ["srt:ConsolidationItemsAxis"] * 4,
                "dimension_member": [
                    "us-gaap_OperatingSegmentsMember",
                    "us-gaap_OperatingSegmentsMember",
                    "us-gaap_IntersegmentEliminationMember",
                    "us-gaap_IntersegmentEliminationMember",
                ],
                "dimension_member_label": [
                    "Operating segments",
                    "Operating segments",
                    "Intersegment Elimination",
                    "Intersegment Elimination",
                ],
                "unit": ["usd"] * 4,
                "weight": [1.0] * 4,
                "preferred_sign": [1.0] * 4,
                "point_in_time": [False] * 4,
                period: [4549.0, 1839.0, -175.0, -11.0],
            }
        )

        facts = parser._parse_statement(frame, "Income Statement", True)
        by_member = {fact.member: fact.value for fact in facts}

        assert by_member["Operating segments - Ratings"] == Decimal("4549.0")
        assert by_member["Operating segments - Indices"] == Decimal("1839.0")
        assert by_member["Intersegment Elimination - Ratings"] == Decimal("-175.0")
        assert by_member["Intersegment Elimination - Indices"] == Decimal("-11.0")

    def test_dimension_suffix_keeps_parent_when_suffix_is_its_own_member(self):
        """An elimination of a named member does not replace that member."""
        parser = SECXBRLParser()
        period = "2018-03-31 (Q1)"
        frame = pd.DataFrame(
            {
                "concept": ["us-gaap_Revenues"] * 6,
                "label": [
                    "Subscription",
                    "Non-transaction",
                    "Intersegment Elimination - Subscription",
                    "Intersegment Elimination - Non-transaction",
                    "Operating Segments - Indices",
                    "Operating Segments - Ratings",
                ],
                "abstract": [False] * 6,
                "dimension": [True] * 6,
                "dimension_axis": [
                    "us-gaap:ProductOrServiceAxis",
                    "us-gaap:ProductOrServiceAxis",
                    "us-gaap:ConsolidationItemsAxis",
                    "us-gaap:ConsolidationItemsAxis",
                    "us-gaap:ConsolidationItemsAxis",
                    "us-gaap:ConsolidationItemsAxis",
                ],
                "dimension_member": [
                    "spgi_SubscriptionMember",
                    "spgi_NonTransactionMember",
                    "us-gaap_IntersegmentEliminationMember",
                    "us-gaap_IntersegmentEliminationMember",
                    "us-gaap_OperatingSegmentsMember",
                    "us-gaap_OperatingSegmentsMember",
                ],
                "dimension_member_label": [
                    "Subscription",
                    "Non-transaction",
                    "Intersegment Elimination",
                    "Intersegment Elimination",
                    "Operating Segments",
                    "Operating Segments",
                ],
                "unit": ["usd"] * 6,
                "weight": [1.0] * 6,
                "preferred_sign": [1.0] * 6,
                "point_in_time": [False] * 6,
                period: [638.0, 352.0, 0.0, -28.0, 214.0, 748.0],
            }
        )

        facts = parser._parse_statement(frame, "Income Statement", True)
        by_member = {fact.member: fact.value for fact in facts}

        assert by_member["spgi:SubscriptionMember"] == Decimal("638.0")
        assert by_member["spgi:NonTransactionMember"] == Decimal("352.0")
        assert by_member["Intersegment Elimination - Subscription"] == Decimal("0.0")
        assert by_member["Intersegment Elimination - Non-transaction"] == Decimal(
            "-28.0"
        )
        assert by_member["Indices"] == Decimal("214.0")
        assert by_member["Ratings"] == Decimal("748.0")

    def test_dimension_uses_quarter_column_and_keeps_ytd(self):
        """A same-date year-to-date column does not replace the published quarter."""
        parser = SECXBRLParser()
        frame = pd.DataFrame(
            {
                "concept": ["us-gaap_Revenues", "us-gaap_Revenues"],
                "label": ["Revenue", "Revenue - United States"],
                "abstract": [False, False],
                "dimension": [False, True],
                "dimension_axis": [None, "srt:StatementGeographicalAxis"],
                "dimension_member": [None, "country_US"],
                "dimension_member_label": [None, "United States"],
                "unit": ["usd", "usd"],
                "weight": [1.0, 1.0],
                "preferred_sign": [1.0, 1.0],
                "point_in_time": [False, False],
                "2024-10-27 (Q3)": [35082.0, 19620.0],
                "2024-10-27 (YTD)": [91166.0, 53873.0],
                "2025-10-26 (Q3)": [57006.0, 39177.0],
                "2025-10-26 (YTD)": [147811.0, 97759.0],
            }
        )

        facts = parser._parse_statement(frame, "Income Statement", True)
        face = {fact.period: fact for fact in facts if not fact.axis}
        geo = {
            (fact.period, fact.period_end): fact
            for fact in facts
            if fact.member == "country:US"
        }

        assert face[PeriodType.Q].value == Decimal("57006")
        assert face[PeriodType.YTD].value == Decimal("147811")
        current = geo[(PeriodType.Q, date(2025, 10, 26))]
        prior = geo[(PeriodType.Q, date(2024, 10, 27))]
        assert current.value == Decimal("39177")
        assert current.comparative_value == Decimal("19620")
        assert prior.value == Decimal("19620")
        assert prior.comparative_value is None
        current_ytd = geo[(PeriodType.YTD, date(2025, 10, 26))]
        assert current_ytd.value == Decimal("97759")
        assert current_ytd.comparative_value == Decimal("53873")
        assert current_ytd.key.endswith("|ytd")
        assert geo[(PeriodType.YTD, date(2024, 10, 27))].value == Decimal("53873")

    def test_dimension_keeps_prior_quarter_numbered_differently(self):
        """A fiscal Q1 labeled Q2 still pairs with the prior-year Q1 column."""
        parser = SECXBRLParser()
        frame = pd.DataFrame(
            {
                "concept": ["us-gaap_Revenues", "us-gaap_Revenues"],
                "label": ["Revenue", "Revenue - Gaming"],
                "abstract": [False, False],
                "dimension": [False, True],
                "dimension_axis": [None, "srt:ProductOrServiceAxis"],
                "dimension_member": [None, "nvda_GamingMember"],
                "dimension_member_label": [None, "Gaming"],
                "unit": ["usd", "usd"],
                "weight": [1.0, 1.0],
                "preferred_sign": [1.0, 1.0],
                "point_in_time": [False, False],
                "2015-04-26 (Q1)": [1151.0, 499.0],
                "2016-05-01 (Q2)": [1305.0, 687.0],
            }
        )

        facts = parser._parse_statement(frame, "Income Statement", True)
        face = [fact for fact in facts if not fact.axis]
        gaming = {fact.period_end: fact for fact in facts if fact.axis}

        assert face[0].comparative_value == Decimal("1151")
        assert gaming[date(2016, 5, 1)].comparative_value == Decimal("499")
        assert gaming[date(2015, 4, 26)].value == Decimal("499")

    def test_dimension_keeps_every_annual_column(self):
        """A three-year 10-K column is a fact for each year, not only the latest."""
        parser = SECXBRLParser()
        frame = pd.DataFrame(
            {
                "concept": ["us-gaap_SalesRevenueNet", "us-gaap_SalesRevenueNet"],
                "label": ["Net sales", "Net sales - United States"],
                "abstract": [False, False],
                "dimension": [False, True],
                "dimension_axis": [None, "us-gaap:StatementGeographicalAxis"],
                "dimension_member": [None, "country_US"],
                "dimension_member_label": [None, "United States"],
                "unit": ["usd", "usd"],
                "weight": [1.0, 1.0],
                "preferred_sign": [1.0, 1.0],
                "point_in_time": [False, False],
                "2013-12-31 (FY)": [74452000000.0, 43959000000.0],
                "2014-12-31 (FY)": [88988000000.0, 54717000000.0],
                "2015-12-31 (FY)": [107006000000.0, 70537000000.0],
            }
        )

        facts = parser._parse_statement(frame, "Income Statement", True)
        face = [fact for fact in facts if not fact.axis]
        geo = {fact.period_end: fact for fact in facts if fact.member == "country:US"}

        assert len(face) == 1
        assert face[0].period_end == date(2015, 12, 31)
        assert face[0].value == Decimal("107006000000")
        assert geo[date(2015, 12, 31)].value == Decimal("70537000000")
        assert geo[date(2015, 12, 31)].comparative_value == Decimal("54717000000")
        assert geo[date(2014, 12, 31)].value == Decimal("54717000000")
        assert geo[date(2014, 12, 31)].comparative_value == Decimal("43959000000")
        assert geo[date(2013, 12, 31)].value == Decimal("43959000000")
        assert geo[date(2013, 12, 31)].comparative_value is None
