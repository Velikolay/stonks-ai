"""Tests for SEC 10-Q parser."""

from decimal import Decimal
from unittest.mock import Mock, patch

import pandas as pd

from filings.parsers.geography_parser import GeographyParser
from filings.parsers.sec_10q import SEC10QParser


class TestSEC10QParser:
    """Test the SEC 10-Q parser."""

    def test_parse_disaggregated_revenues_product(self):
        """Test parsing disaggregated revenues by product."""
        geography_parser = GeographyParser()
        parser = SEC10QParser(geography_parser=geography_parser)

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
                mock_fact.statement = "Disaggregated Revenue (Product)"
                mock_fact.label = f"Contract Revenue - {product}"
                mock_facts.append(mock_fact)

            mock_create.side_effect = mock_facts

            facts = parser._parse_disaggregated_revenues(mock_xbrl)

            assert len(facts) == 3
            assert facts[0].concept == "Revenue"
            assert facts[0].member == "iPhone"
            assert facts[0].value == Decimal("1000000")
            assert facts[0].axis == "srt:ProductOrServiceAxis"
            assert facts[0].statement == "Disaggregated Revenue (Product)"
            assert facts[0].label == "Contract Revenue - iPhone"

    def test_parse_disaggregated_revenues_geographic(self):
        """Test parsing disaggregated revenues by geographic region."""
        geography_parser = GeographyParser()
        parser = SEC10QParser(geography_parser=geography_parser)

        # Mock XBRL query result for geographic revenue
        mock_revenue_df = pd.DataFrame(
            {
                "concept": ["Revenue", "Revenue"],
                "label": ["Contract Revenue", "Contract Revenue"],
                "value": [5000000, 3000000],
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
        assert facts[0].statement == "Disaggregated Revenue (Geographic)"

    def test_create_disaggregated_revenue_fact(self):
        """Test creating disaggregated revenue fact."""
        geography_parser = GeographyParser()
        parser = SEC10QParser(geography_parser=geography_parser)

        # Mock row data
        row = {
            "concept": "Revenue",
            "label": "Contract Revenue",
            "value": 1000000,
            "period_start": "2024-01-01",
            "period_end": "2024-03-31",
            "dim_srt_ProductOrServiceAxis": "iPhone",
        }

        fact = parser._create_disaggregated_metric_fact(
            row,
            metric="Revenue",
            dimension="srt:ProductOrServiceAxis",
            dimension_type="Product",
        )

        assert fact.concept == "Revenue"
        assert fact.value == Decimal("1000000")
        assert fact.member == "iPhone"
        assert fact.axis == "srt:ProductOrServiceAxis"
        assert fact.statement == "Disaggregated Revenue (Product)"
        assert fact.label == "Contract Revenue - iPhone"

    def test_create_disaggregated_revenue_fact_invalid_value(self):
        """Test creating disaggregated revenue fact with invalid value."""
        geography_parser = GeographyParser()
        parser = SEC10QParser(geography_parser=geography_parser)

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
            dimension_type="Product",
        )

        assert fact is None

    def test_parse_disaggregated_revenues_empty_data(self):
        """Test parsing disaggregated revenues with empty data."""
        geography_parser = GeographyParser()
        parser = SEC10QParser(geography_parser=geography_parser)

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
        parser = SEC10QParser(geography_parser=geography_parser)

        # Mock XBRL query result for business segment revenue
        mock_revenue_df = pd.DataFrame(
            {
                "concept": ["Revenue", "Revenue", "Revenue"],
                "label": ["Contract Revenue", "Contract Revenue", "Contract Revenue"],
                "value": [4000000, 2000000, 1000000],
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
            assert fact.statement == "Disaggregated Revenue (Geographic)"
            assert isinstance(fact.value, Decimal)
            assert fact.value > 0

    def test_parse_disaggregated_revenues_business_segment_non_region(self):
        """Test that business segments without region information are ignored."""
        geography_parser = GeographyParser()
        parser = SEC10QParser(geography_parser=geography_parser)

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
        """Test that SEC10QParser can accept a custom geography parser."""
        # Create a custom geography parser
        custom_parser = GeographyParser()

        # Create SEC10QParser with custom geography parser
        parser = SEC10QParser(geography_parser=custom_parser)

        # Verify that the custom parser is used
        assert parser.geography_parser is custom_parser

    def test_parse_disaggregated_metrics_operating_income(self):
        """Test parsing disaggregated operating income by product."""
        geography_parser = GeographyParser()
        parser = SEC10QParser(geography_parser=geography_parser)

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
