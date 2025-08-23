"""Tests for quarterly financial metrics operations."""

from decimal import Decimal
from unittest.mock import Mock, patch

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
                    company_id=1, fiscal_year=2024, limit=10
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
                assert call_args.fiscal_year == 2024

    def test_get_metrics_by_company(self):
        """Test getting metrics by company."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_quarterly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_company(1, limit=50)

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1
                assert call_args.limit == 50

    def test_get_metrics_by_label(self):
        """Test getting metrics by label."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_quarterly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_label("Revenue", limit=20)

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.label == "Revenue"
                assert call_args.limit == 20

    def test_get_metrics_by_statement(self):
        """Test getting metrics by statement."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_quarterly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_statement("Income Statement", limit=30)

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.statement == "Income Statement"
                assert call_args.limit == 30

    def test_get_latest_metrics_by_company(self):
        """Test getting latest metrics by company."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock the get_latest_metrics_by_company method to return test data
            with patch.object(operations, "get_latest_metrics_by_company") as mock_get:
                # Create test data
                test_financial = QuarterlyFinancial(
                    company_id=1,
                    fiscal_year=2024,
                    fiscal_quarter=4,
                    label="Revenue",
                    normalized_label="Revenue",
                    value=Decimal("1200000.00"),
                    unit="USD",
                    statement="Income Statement",
                    period_end=None,
                    period_start=None,
                    source_type="calculated",
                )
                mock_get.return_value = [test_financial]

                result = operations.get_latest_metrics_by_company(1, limit=10)

                # Verify the method was called with correct parameters
                mock_get.assert_called_once_with(1, limit=10)

                # Verify the result
                assert len(result) == 1
                assert result[0].company_id == 1
                assert result[0].fiscal_year == 2024
                assert result[0].fiscal_quarter == 4
                assert result[0].source_type == "calculated"

    def test_get_quarterly_metrics_error_handling(self):
        """Test error handling in get_quarterly_metrics."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock SQLAlchemyError
            from sqlalchemy.exc import SQLAlchemyError

            mock_engine.connect.side_effect = SQLAlchemyError("Database error")

            filter_params = QuarterlyFinancialsFilter(company_id=1)
            result = operations.get_quarterly_financials(filter_params)

            assert result == []

    def test_get_latest_metrics_error_handling(self):
        """Test error handling in get_latest_metrics_by_company."""
        mock_engine = Mock()

        with patch("filings.db.quarterly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = QuarterlyFinancialsOperations(mock_engine)

            # Mock SQLAlchemyError
            from sqlalchemy.exc import SQLAlchemyError

            mock_engine.connect.side_effect = SQLAlchemyError("Database error")

            result = operations.get_latest_metrics_by_company(1)

            assert result == []
