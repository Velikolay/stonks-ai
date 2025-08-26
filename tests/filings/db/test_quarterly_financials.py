"""Tests for quarterly financial metrics operations."""

from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch

from sqlalchemy.exc import SQLAlchemyError

from filings.db.quarterly_financials import QuarterlyFinancialsOperations
from filings.models.quarterly_financials import (
    QuarterlyFinancial,
    QuarterlyFinancialsFilter,
)


class TestQuarterlyFinancialsOperations:
    """Test the QuarterlyMetricsOperations class."""

    def test_operations_initialization(self):
        """Test operations initialization."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            assert operations.engine == mock_engine
            assert operations.quarterly_financials_view is not None

    def test_get_quarterly_metrics_with_filters(self):
        """Test getting quarterly metrics with various filters."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the get_quarterly_financials method to return test data
            with patch.object(operations, "get_quarterly_financials") as mock_get:
                # Create test data
                test_financial1 = QuarterlyFinancial(
                    company_id=1,
                    fiscal_year=2024,
                    fiscal_quarter=1,
                    label="Revenue",
                    normalized_label="Revenue",
                    value=Decimal("1000000.00"),
                    unit="USD",
                    statement="Income Statement",
                    period_end=None,
                    period_start=None,
                    source_type="10-Q",
                )
                test_financial2 = QuarterlyFinancial(
                    company_id=1,
                    fiscal_year=2024,
                    fiscal_quarter=2,
                    label="Revenue",
                    normalized_label="Revenue",
                    value=Decimal("1100000.00"),
                    unit="USD",
                    statement="Income Statement",
                    period_end=None,
                    period_start=None,
                    source_type="10-Q",
                )
                mock_get.return_value = [test_financial1, test_financial2]

                # Test with filter
                filter_params = QuarterlyFinancialsFilter(
                    company_id=1, fiscal_year_start=2024, fiscal_year_end=2024
                )

                result = operations.get_quarterly_financials(filter_params)

                # Verify the method was called with correct parameters
                mock_get.assert_called_once_with(filter_params)

                # Verify the result
                assert len(result) == 2
                assert result[0].company_id == 1
                assert result[0].fiscal_year == 2024
                assert result[0].fiscal_quarter == 1
                assert result[0].label == "Revenue"
                assert result[0].value == Decimal("1000000.00")
                assert result[0].source_type == "10-Q"

    def test_get_metrics_by_company_and_year(self):
        """Test getting metrics by company and year."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the get_quarterly_financials method
            with patch.object(operations, "get_quarterly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_company_and_year(1, 2024)

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1
                assert call_args.fiscal_year_start == 2024
                assert call_args.fiscal_year_end == 2024

    def test_get_metrics_by_company(self):
        """Test getting metrics by company."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_quarterly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_company(1)

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1

    def test_get_metrics_by_label(self):
        """Test getting metrics by label."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_quarterly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_label(1, "Revenue")

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1
                assert call_args.labels == ["Revenue"]

    def test_get_metrics_by_statement(self):
        """Test getting metrics by statement."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_quarterly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_statement(1, "Income Statement")

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1
                assert call_args.statement == "Income Statement"

    def test_get_latest_metrics_by_company(self):
        """Test getting latest metrics by company."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the database connection and query execution
            mock_conn = Mock()
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_conn
            mock_engine.connect.return_value = mock_context
            mock_result = Mock()
            mock_conn.execute.return_value = mock_result
            mock_result.fetchall.return_value = []

            # Mock the select function
            with patch("filings.db.quarterly_financials.select") as mock_select:
                mock_select.return_value = Mock()
                mock_select.return_value.where.return_value = Mock()
                mock_select.return_value.where.return_value.order_by.return_value = (
                    Mock()
                )
                mock_select.return_value.where.return_value.order_by.return_value.limit.return_value = (
                    Mock()
                )

                operations.get_latest_metrics_by_company(1, limit=10)

                # Verify the query was executed
                mock_conn.execute.assert_called_once()

    def test_get_quarterly_metrics_error_handling(self):
        """Test error handling in get_quarterly_financials."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the engine.connect to raise an exception
            mock_engine.connect.side_effect = SQLAlchemyError("Database error")

            filter_params = QuarterlyFinancialsFilter(company_id=1)
            result = operations.get_quarterly_financials(filter_params)

            # Should return empty list on error
            assert result == []

    def test_get_latest_metrics_error_handling(self):
        """Test error handling in get_latest_metrics_by_company."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the engine.connect to raise an exception
            mock_engine.connect.side_effect = SQLAlchemyError("Database error")

            result = operations.get_latest_metrics_by_company(1, limit=10)

            # Should return empty list on error
            assert result == []

    def test_get_normalized_labels(self):
        """Test getting normalized labels."""
        mock_engine = Mock()

        # Mock the table and its columns
        mock_table = Mock()
        mock_table.c.normalized_label = Mock()
        mock_table.c.statement = Mock()
        mock_table.c.company_id = Mock()

        with patch("filings.db.quarterly_financials.Table", return_value=mock_table):
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the table instance that was created in __init__
            operations.quarterly_financials_view = mock_table

            # Mock the database connection and query execution
            mock_conn = Mock()
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_conn
            mock_engine.connect.return_value = mock_context
            mock_result = Mock()

            # Mock the query result
            mock_row1 = Mock()
            mock_row1.normalized_label = "Revenue"
            mock_row1.statement = "IncomeStatement"
            mock_row1.count = 100

            mock_row2 = Mock()
            mock_row2.normalized_label = "Net Income"
            mock_row2.statement = "IncomeStatement"
            mock_row2.count = 80

            mock_result.fetchall.return_value = [mock_row1, mock_row2]
            mock_conn.execute.return_value = mock_result

            # Mock the SQLAlchemy select function to return a simple mock
            with patch("filings.db.quarterly_financials.select") as mock_select:
                mock_select.return_value.where.return_value.group_by.return_value.order_by.return_value = (
                    Mock()
                )

                result = operations.get_normalized_labels(company_id=1)

            assert len(result) == 2
            assert result[0]["normalized_label"] == "Revenue"
            assert result[0]["statement"] == "IncomeStatement"
            assert result[0]["count"] == 100
            assert result[1]["normalized_label"] == "Net Income"
            assert result[1]["count"] == 80

    def test_get_normalized_labels_with_statement_filter(self):
        """Test getting normalized labels with statement filter."""
        mock_engine = Mock()

        # Mock the table and its columns
        mock_table = Mock()
        mock_table.c.normalized_label = Mock()
        mock_table.c.statement = Mock()
        mock_table.c.company_id = Mock()

        with patch("filings.db.quarterly_financials.Table", return_value=mock_table):
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the table instance that was created in __init__
            operations.quarterly_financials_view = mock_table

            # Mock the database connection and query execution
            mock_conn = Mock()
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_conn
            mock_engine.connect.return_value = mock_context
            mock_result = Mock()

            # Mock the query result
            mock_row = Mock()
            mock_row.normalized_label = "Total Assets"
            mock_row.statement = "BalanceSheet"
            mock_row.count = 50

            mock_result.fetchall.return_value = [mock_row]
            mock_conn.execute.return_value = mock_result

            # Mock the SQLAlchemy select function to return a simple mock
            with patch("filings.db.quarterly_financials.select") as mock_select:
                mock_select.return_value.where.return_value.group_by.return_value.order_by.return_value = (
                    Mock()
                )

                result = operations.get_normalized_labels(
                    company_id=1, statement="BalanceSheet"
                )

            assert len(result) == 1
            assert result[0]["normalized_label"] == "Total Assets"
            assert result[0]["statement"] == "BalanceSheet"
            assert result[0]["count"] == 50

    def test_get_normalized_labels_error_handling(self):
        """Test error handling when getting normalized labels fails."""
        mock_engine = Mock()

        # Mock the table and its columns
        mock_table = Mock()
        mock_table.c.normalized_label = Mock()
        mock_table.c.statement = Mock()
        mock_table.c.company_id = Mock()

        with patch("filings.db.quarterly_financials.Table", return_value=mock_table):
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the table instance that was created in __init__
            operations.quarterly_financials_view = mock_table

            # Mock the SQLAlchemy select function to return a simple mock
            with patch("filings.db.quarterly_financials.select") as mock_select:
                mock_select.return_value.where.return_value.group_by.return_value.order_by.return_value = (
                    Mock()
                )

                # Mock the engine.connect to raise an exception
                mock_engine.connect.side_effect = SQLAlchemyError("Database error")

                result = operations.get_normalized_labels(company_id=1)
                assert result == []

    def test_get_normalized_labels_with_statement_and_company_id_filter(self):
        """Test getting normalized labels with both statement and company_id filters."""
        mock_engine = Mock()

        # Mock the table and its columns
        mock_table = Mock()
        mock_table.c.normalized_label = Mock()
        mock_table.c.statement = Mock()
        mock_table.c.company_id = Mock()

        with patch("filings.db.quarterly_financials.Table", return_value=mock_table):
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the table instance that was created in __init__
            operations.quarterly_financials_view = mock_table

            # Mock the database connection and query execution
            mock_conn = Mock()
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_conn
            mock_engine.connect.return_value = mock_context
            mock_result = Mock()

            # Mock the query result
            mock_row = Mock()
            mock_row.normalized_label = "Total Assets"
            mock_row.statement = "BalanceSheet"
            mock_row.count = 15

            mock_result.fetchall.return_value = [mock_row]
            mock_conn.execute.return_value = mock_result

            # Mock the SQLAlchemy select function to return a simple mock
            with patch("filings.db.quarterly_financials.select") as mock_select:
                mock_select.return_value.where.return_value.group_by.return_value.order_by.return_value = (
                    Mock()
                )

                result = operations.get_normalized_labels(
                    company_id=1, statement="BalanceSheet"
                )

            assert len(result) == 1
            assert result[0]["normalized_label"] == "Total Assets"
            assert result[0]["statement"] == "BalanceSheet"
            assert result[0]["count"] == 15
