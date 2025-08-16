"""Tests for SEC 10-Q XBRL parser."""

from decimal import Decimal
from unittest.mock import Mock, patch

from filings.parsers.sec_10q import SEC10QParser


class TestSEC10QParser:
    """Test SEC 10-Q XBRL parser."""

    def test_parser_initialization(self):
        """Test parser initialization."""
        parser = SEC10QParser()
        assert parser is not None

    @patch("filings.parsers.sec_10q.Company")
    def test_parse_company_filings(self, mock_company_class):
        """Test parsing multiple company filings."""
        # Mock company and filings
        mock_company = Mock()
        mock_filing1 = Mock()
        mock_filing1.accession_number = "0000320193-25-000073"

        mock_xbrl = Mock()
        mock_xbrl.statements.income_statement.return_value = None
        mock_xbrl.statements.balance_sheet.return_value = None
        mock_xbrl.statements.cashflow_statement.return_value = None

        mock_filing1.xbrl.return_value = mock_xbrl
        mock_company.get_filings.return_value = [mock_filing1]
        mock_company_class.return_value = mock_company

        # Test parsing
        parser = SEC10QParser()
        facts = parser.parse_company_filings("AAPL", form="10-Q", limit=1)

        assert isinstance(facts, list)
        mock_company_class.assert_called_once_with("AAPL")
        mock_company.get_filings.assert_called_once_with(form="10-Q")

    def test_parse_date_valid(self):
        """Test parsing valid date strings."""
        parser = SEC10QParser()

        # Test ISO date format
        result = parser._parse_date("2024-09-28")
        assert result.year == 2024
        assert result.month == 9
        assert result.day == 28

        # Test ISO date with time
        result = parser._parse_date("2024-09-28T00:00:00")
        assert result.year == 2024
        assert result.month == 9
        assert result.day == 28

    def test_parse_date_invalid(self):
        """Test parsing invalid date strings."""
        parser = SEC10QParser()

        # Test None
        result = parser._parse_date(None)
        assert result is None

        # Test empty string
        result = parser._parse_date("")
        assert result is None

        # Test invalid format
        result = parser._parse_date("invalid-date")
        assert result is None

    @patch("pandas.DataFrame")
    def test_parse_statement_with_data(self, mock_dataframe):
        """Test parsing statement with valid data."""
        parser = SEC10QParser()

        # Mock DataFrame with period columns and hierarchy
        mock_df = Mock()
        mock_df.empty = False
        mock_df.columns.tolist.return_value = [
            "concept",
            "label",
            "2025-06-28 (Q2)",
            "2024-06-29 (Q2)",
            "level",
            "abstract",
            "dimension",
        ]
        mock_df.iterrows.return_value = [
            (
                0,
                {
                    "concept": "us-gaap_Revenues",
                    "label": "Revenue",
                    "2025-06-28 (Q2)": 96428000000.0,
                    "level": 1,
                    "abstract": False,
                    "dimension": False,
                },
            ),
            (
                1,
                {
                    "concept": "us-gaap_CostsAndExpensesAbstract",
                    "label": "Costs and expenses:",
                    "2025-06-28 (Q2)": None,
                    "level": 1,
                    "abstract": True,
                    "dimension": False,
                },
            ),
            (
                2,
                {
                    "concept": "us-gaap_CostOfRevenue",
                    "label": "Total Cost of Revenue",
                    "2025-06-28 (Q2)": 39039000000.0,
                    "level": 2,
                    "abstract": False,
                    "dimension": False,
                },
            ),
        ]

        facts = parser._parse_statement(mock_df, "Income Statement")

        assert len(facts) == 2
        assert facts[0].concept == "us-gaap_Revenues"
        assert facts[0].label == "Revenue"
        assert facts[0].value == Decimal("96428000000.0")
        assert facts[1].concept == "us-gaap_CostOfRevenue"
        assert facts[1].label == "Total Cost of Revenue"
        assert facts[1].value == Decimal("39039000000.0")

        # Check that the second fact has the abstract in its hierarchy
        assert facts[1].abstracts is not None
        assert (
            len(facts[1].abstracts) == 1
        )  # Only parent abstract, not the element itself
        assert facts[1].abstracts[0].concept == "us-gaap_CostsAndExpensesAbstract"

    def test_parse_statement_empty(self):
        """Test parsing empty statement."""
        parser = SEC10QParser()

        # Mock empty DataFrame
        mock_df = Mock()
        mock_df.empty = True

        facts = parser._parse_statement(mock_df, "Income Statement")
        assert facts == []

    def test_parse_statement_none(self):
        """Test parsing None statement."""
        parser = SEC10QParser()

        facts = parser._parse_statement(None, "Income Statement")
        assert facts == []

    def test_create_financial_fact_with_hierarchy(self):
        """Test creating financial fact with hierarchical abstracts."""
        parser = SEC10QParser()

        # Mock row data
        row = {
            "concept": "us-gaap_CostOfRevenue",
            "label": "Total Cost of Revenue",
            "2025-06-28 (Q2)": 39039000000.0,
            "level": 2,
            "abstract": False,
            "dimension": False,
        }

        # Mock abstract hierarchy
        abstract_hierarchy = [
            {
                "level": 1,
                "concept": "us-gaap_CostsAndExpensesAbstract",
                "label": "Costs and expenses:",
            }
        ]

        fact = parser._create_financial_fact_with_hierarchy(
            row, "Income Statement", "2025-06-28 (Q2)", "2025-06-28", abstract_hierarchy
        )

        assert fact is not None
        assert fact.concept == "us-gaap_CostOfRevenue"
        assert fact.label == "Total Cost of Revenue"
        assert fact.value == Decimal("39039000000.0")
        assert fact.unit == "USD"
        assert fact.statement == "Income Statement"
        assert fact.period_end.year == 2025
        assert fact.period_end.month == 6
        assert fact.period_end.day == 28

        # Check abstracts hierarchy
        assert fact.abstracts is not None
        assert len(fact.abstracts) == 1  # Only parent abstract, not the element itself
        assert fact.abstracts[0].concept == "us-gaap_CostsAndExpensesAbstract"
        assert fact.abstracts[0].label == "Costs and expenses:"

    def test_abstract_detection_by_value(self):
        """Test that abstracts are detected by lack of numeric values."""
        parser = SEC10QParser()

        # Mock DataFrame with different abstract scenarios
        mock_df = Mock()
        mock_df.empty = False
        mock_df.columns.tolist.return_value = [
            "concept",
            "label",
            "2025-06-28 (Q2)",
            "level",
            "abstract",
            "dimension",
        ]
        mock_df.iterrows.return_value = [
            (
                0,
                {
                    "concept": "us-gaap_Revenues",
                    "label": "Revenue",
                    "2025-06-28 (Q2)": 96428000000.0,
                    "level": 1,
                    "abstract": False,  # Should be ignored
                    "dimension": False,
                },
            ),
            (
                1,
                {
                    "concept": "us-gaap_CostsAndExpensesAbstract",
                    "label": "Costs and expenses:",
                    "2025-06-28 (Q2)": None,  # No value = abstract
                    "level": 1,
                    "abstract": False,  # Should be ignored
                    "dimension": False,
                },
            ),
            (
                2,
                {
                    "concept": "us-gaap_CostOfRevenue",
                    "label": "Total Cost of Revenue",
                    "2025-06-28 (Q2)": 39039000000.0,
                    "level": 2,
                    "abstract": True,  # Should be ignored
                    "dimension": False,
                },
            ),
            (
                3,
                {
                    "concept": "us-gaap_AnotherAbstract",
                    "label": "Another Abstract",
                    "2025-06-28 (Q2)": 0,  # Zero value = abstract
                    "level": 2,
                    "abstract": False,  # Should be ignored
                    "dimension": False,
                },
            ),
        ]

        facts = parser._parse_statement(mock_df, "Income Statement")

        # Should only get 2 facts (rows 0 and 2), rows 1 and 3 are abstracts
        assert len(facts) == 2
        assert facts[0].concept == "us-gaap_Revenues"
        assert facts[1].concept == "us-gaap_CostOfRevenue"

        # Second fact should have the abstract in its hierarchy
        assert facts[1].abstracts is not None
        assert len(facts[1].abstracts) == 1
        assert facts[1].abstracts[0].concept == "us-gaap_CostsAndExpensesAbstract"
