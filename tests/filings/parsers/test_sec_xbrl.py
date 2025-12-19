"""Tests for SEC 10-Q parser."""

from decimal import Decimal
from unittest.mock import Mock, patch

import pandas as pd

from filings.parsers.geography import GeographyParser
from filings.parsers.product import ProductParser
from filings.parsers.sec_xbrl import SECXBRLParser


class TestSECXBRLParser:
    """Test the SEC XBRL parser."""

    def test_parse_disaggregated_revenues_product(self):
        """Test parsing disaggregated revenues by product."""
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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
            for i, product in enumerate(["iPhone", "Mac", "iPad"]):
                mock_fact = Mock()
                mock_fact.concept = "Revenue"
                mock_fact.member = product
                mock_fact.value = Decimal(str(1000000 + i * 1000000))
                mock_fact.axis = "srt:ProductOrServiceAxis"
                mock_fact.parsed_axis = "Product"
                mock_fact.statement = "Income Statement"
                mock_fact.label = "Contract Revenue"
                mock_facts.append(mock_fact)

            mock_create.side_effect = mock_facts

            facts = parser._parse_disaggregated_revenues(mock_xbrl)

            assert len(facts) == 3
            assert facts[0].concept == "Revenue"
            assert facts[0].member == "iPhone"
            assert facts[0].value == Decimal("1000000")
            assert facts[0].axis == "srt:ProductOrServiceAxis"
            assert facts[0].parsed_axis == "Product"
            assert facts[0].statement == "Income Statement"
            assert facts[0].label == "Contract Revenue"

    def test_parse_disaggregated_revenues_geographic(self):
        """Test parsing disaggregated revenues by geographic region."""
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

        # Mock XBRL query result for geographic revenue
        mock_revenue_df = pd.DataFrame(
            {
                "concept": ["Revenue", "Revenue"],
                "label": ["Contract Revenue", "Contract Revenue"],
                "numeric_value": [5000000, 3000000],
                "period_start": ["2024-01-01", "2024-01-01"],
                "period_end": ["2024-03-31", "2024-03-31"],
                "dim_srt_StatementGeographicAxis": ["Americas", "Europe"],
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
            if "StatementGeographicAxis" not in str(mock_query.by_dimension.call_args)
            else mock_revenue_df
        )

        facts = parser._parse_disaggregated_revenues(mock_xbrl)

        assert len(facts) == 2
        assert facts[0].concept == "Revenue"
        assert facts[0].member == "Americas"
        assert facts[0].value == Decimal("5000000")
        assert facts[0].axis == "srt:StatementGeographicAxis"
        assert facts[0].parsed_axis == "Geographic"
        assert facts[0].statement == "Income Statement"

    def test_create_disaggregated_revenue_fact(self):
        """Test creating disaggregated revenue fact."""
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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
            metric="Revenue",
            dimension="srt:ProductOrServiceAxis",
            dimension_parsed="Product",
            dimension_value_parsed=None,
            position=0,
        )

        assert fact.concept == "Revenue"
        assert fact.value == Decimal("1000000")
        assert fact.member == "iPhone"
        assert fact.axis == "srt:ProductOrServiceAxis"
        assert fact.parsed_axis == "Product"
        assert fact.statement == "Income Statement"
        assert fact.label == "Contract Revenue"
        # Regression test: ensure period field is set
        assert fact.period is not None
        assert fact.period in ["YTD", "Q"]  # PeriodType values

    def test_create_disaggregated_revenue_fact_invalid_value(self):
        """Test creating disaggregated revenue fact with invalid value."""
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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
            metric="Revenue",
            dimension="srt:ProductOrServiceAxis",
            dimension_parsed="Product",
            dimension_value_parsed=None,
            position=0,
        )

        assert fact is None

    def test_parser_requires_period_field_regression(self):
        """Regression test: ensure parser creates FinancialFact with period field."""
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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
            metric="Revenue",
            dimension="srt:ProductOrServiceAxis",
            dimension_parsed="Product",
            dimension_value_parsed=None,
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
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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

    def test_determine_period_type_from_column_validation(self):
        """Test that _determine_period_type_from_column handles various inputs correctly."""
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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

        # Test with valid YTD period column (no quarter indicator)
        result = parser._determine_period_type_from_column(
            "2024-12-31", "Cash Flow Statement"
        )
        assert result == "YTD"

        # Test with valid YTD period column (explicit YTD)
        result = parser._determine_period_type_from_column(
            "2024-12-31 (YTD)", "Cash Flow Statement"
        )
        assert result == "YTD"

        # Test with valid period column (quarter in middle)
        result = parser._determine_period_type_from_column(
            "Q3 2024-09-30", "Cash Flow Statement"
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

    def test_is_column_mostly_empty(self):
        """Test that _is_column_mostly_empty correctly identifies empty columns."""
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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
            assert fact.parsed_axis == "Geographic"
            assert fact.statement == "Income Statement"
            assert isinstance(fact.value, Decimal)
            assert fact.value > 0

    def test_parse_disaggregated_revenues_business_segment_non_region(self):
        """Test that business segments without region information are ignored."""
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

        # Mock XBRL query result for business segment revenue with non-region segments
        mock_revenue_df = pd.DataFrame(
            {
                "concept": ["Revenue", "Revenue", "Revenue"],
                "label": ["Contract Revenue", "Contract Revenue", "Contract Revenue"],
                "value": [4000000, 2000000, 1000000],
                "period_start": ["2024-01-01", "2024-01-01", "2024-01-01"],
                "period_end": ["2024-03-31", "2024-03-31", "2024-03-31"],
                "dim_us-gaap_StatementBusinessSegmentsAxis": [
                    "ProductRevenue",
                    "CustomerSegment",
                    "RandomText",
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

        # Should not extract any facts since none contain region information
        assert len(facts) == 0

    def test_constructor_with_custom_geography_parser(self):
        """Test that SECXBRLParser can accept a custom geography parser."""
        # Create a custom geography parser
        custom_parser = GeographyParser()

        # Create SECXBRLParser with custom geography parser
        parser = SECXBRLParser(
            geography_parser=custom_parser,
            product_parser=ProductParser(),
        )

        # Verify that the custom parser is used
        assert parser.geography_parser is custom_parser

    def test_parse_disaggregated_metrics_operating_income(self):
        """Test parsing disaggregated operating income by product."""
        geography_parser = GeographyParser()
        product_parser = ProductParser()
        parser = SECXBRLParser(
            geography_parser=geography_parser,
            product_parser=product_parser,
        )

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
