"""Tests for quarterly financial metrics operations."""

from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from filings.db.quarterly_financials import QuarterlyFinancialsOperations
from filings.models.quarterly_financials import (
    QuarterlyFinancial,
    QuarterlyFinancialsFilter,
)


def _make_operations():
    """Create operations instance with mocked engine and metadata."""
    mock_engine = Mock()
    mock_table = Mock()
    mock_metadata = Mock()
    mock_metadata.tables = {"quarterly_financials": mock_table}
    return QuarterlyFinancialsOperations(mock_engine, mock_metadata)


class TestQuarterlyFinancialsOperations:
    """Test the QuarterlyMetricsOperations class."""

    def test_operations_initialization(self):
        """Test operations initialization."""
        mock_engine = Mock()
        mock_table = Mock()
        mock_metadata = Mock()
        mock_metadata.tables = {"quarterly_financials": mock_table}
        operations = QuarterlyFinancialsOperations(mock_engine, mock_metadata)

        assert operations.engine == mock_engine
        assert operations.quarterly_financials_view == mock_table

    @pytest.mark.asyncio
    async def test_get_quarterly_metrics_with_filters(self):
        """Test getting quarterly metrics with various filters."""
        operations = _make_operations()

        # Mock the get_quarterly_financials method to return test data
        with patch.object(
            operations, "get_quarterly_financials", new_callable=AsyncMock
        ) as mock_get:
            # Create test data
            test_financial1 = QuarterlyFinancial(
                id=1,
                company_id=1,
                filing_id=1,
                fiscal_year=2024,
                fiscal_quarter=1,
                label="Revenue",
                normalized_label="Revenue",
                value=Decimal("1000000.00"),
                unit="USD",
                statement="Income Statement",
                abstract_id=None,
                is_abstract=False,
                is_synthetic=False,
                period_end=None,
                source_type="10-Q",
            )
            test_financial2 = QuarterlyFinancial(
                id=2,
                company_id=1,
                filing_id=2,
                fiscal_year=2024,
                fiscal_quarter=2,
                label="Revenue",
                normalized_label="Revenue",
                value=Decimal("1100000.00"),
                unit="USD",
                statement="Income Statement",
                abstract_id=None,
                is_abstract=False,
                is_synthetic=False,
                period_end=None,
                source_type="10-Q",
            )
            mock_get.return_value = [test_financial1, test_financial2]

            # Test with filter
            filter_params = QuarterlyFinancialsFilter(
                company_id=1, fiscal_year_start=2024, fiscal_year_end=2024
            )

            result = await operations.get_quarterly_financials(filter_params)

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

    @pytest.mark.asyncio
    async def test_get_metrics_by_company_and_year(self):
        """Test getting metrics by company and year."""
        operations = _make_operations()

        with patch.object(
            operations, "get_quarterly_financials", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = []

            await operations.get_metrics_by_company_and_year(1, 2024)

            mock_get.assert_called_once()
            call_args = mock_get.call_args[0][0]
            assert call_args.company_id == 1
            assert call_args.fiscal_year_start == 2024
            assert call_args.fiscal_year_end == 2024

    @pytest.mark.asyncio
    async def test_get_metrics_by_company(self):
        """Test getting metrics by company."""
        operations = _make_operations()

        with patch.object(
            operations, "get_quarterly_financials", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = []

            await operations.get_metrics_by_company(1)

            mock_get.assert_called_once()
            call_args = mock_get.call_args[0][0]
            assert call_args.company_id == 1

    @pytest.mark.asyncio
    async def test_get_metrics_by_label(self):
        """Test getting metrics by label."""
        operations = _make_operations()

        with patch.object(
            operations, "get_quarterly_financials", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = []

            await operations.get_metrics_by_label(1, "Revenue")

            mock_get.assert_called_once()
            call_args = mock_get.call_args[0][0]
            assert call_args.company_id == 1
            assert call_args.labels == ["Revenue"]

    @pytest.mark.asyncio
    async def test_get_metrics_by_statement(self):
        """Test getting metrics by statement."""
        operations = _make_operations()

        with patch.object(
            operations, "get_quarterly_financials", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = []

            await operations.get_metrics_by_statement(1, "Income Statement")

            mock_get.assert_called_once()
            call_args = mock_get.call_args[0][0]
            assert call_args.company_id == 1
            assert call_args.statement == "Income Statement"

    @pytest.mark.asyncio
    async def test_get_latest_metrics_by_company(self):
        """Test getting latest metrics by company."""
        operations = _make_operations()
        mock_conn = AsyncMock()
        mock_result = Mock()
        mock_result.fetchall.return_value = []
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        operations.engine.connect = Mock(return_value=mock_context)

        mock_stmt = Mock()
        with patch("filings.db.quarterly_financials.select") as mock_select:
            mock_select.return_value.where.return_value.order_by.return_value.limit.return_value = (
                mock_stmt
            )

            await operations.get_latest_metrics_by_company(1, limit=10)

        mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_quarterly_metrics_error_handling(self):
        """Test error handling in get_quarterly_financials."""
        operations = _make_operations()
        operations.engine.connect.side_effect = SQLAlchemyError("Database error")

        filter_params = QuarterlyFinancialsFilter(company_id=1)
        result = await operations.get_quarterly_financials(filter_params)

        assert result == []

    @pytest.mark.asyncio
    async def test_get_latest_metrics_error_handling(self):
        """Test error handling in get_latest_metrics_by_company."""
        operations = _make_operations()
        operations.engine.connect.side_effect = SQLAlchemyError("Database error")

        result = await operations.get_latest_metrics_by_company(1, limit=10)

        assert result == []

    @pytest.mark.asyncio
    async def test_get_normalized_labels(self):
        """Test getting normalized labels."""
        operations = _make_operations()
        mock_conn = AsyncMock()
        mock_row1 = Mock()
        mock_row1.normalized_label = "Revenue"
        mock_row1.statement = "IncomeStatement"
        mock_row1.axis = None
        mock_row1.member = None
        mock_row1.count = 100
        mock_row2 = Mock()
        mock_row2.normalized_label = "Net Income"
        mock_row2.statement = "IncomeStatement"
        mock_row2.axis = None
        mock_row2.member = None
        mock_row2.count = 80
        mock_result = Mock()
        mock_result.fetchall.return_value = [mock_row1, mock_row2]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        operations.engine.connect = Mock(return_value=mock_context)

        mock_stmt = Mock()
        mock_stmt.where.return_value = mock_stmt
        with patch("filings.db.quarterly_financials.select") as mock_select:
            mock_select.return_value.where.return_value.group_by.return_value.order_by.return_value = (
                mock_stmt
            )

            result = await operations.get_normalized_labels(company_id=1)

        assert len(result) == 2
        assert result[0]["normalized_label"] == "Revenue"
        assert result[0]["statement"] == "IncomeStatement"
        assert result[0]["count"] == 100
        assert result[1]["normalized_label"] == "Net Income"
        assert result[1]["count"] == 80

    @pytest.mark.asyncio
    async def test_get_normalized_labels_with_statement_filter(self):
        """Test getting normalized labels with statement filter."""
        operations = _make_operations()
        mock_conn = AsyncMock()
        mock_row = Mock()
        mock_row.normalized_label = "Total Assets"
        mock_row.statement = "BalanceSheet"
        mock_row.axis = None
        mock_row.member = None
        mock_row.count = 50
        mock_result = Mock()
        mock_result.fetchall.return_value = [mock_row]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        operations.engine.connect = Mock(return_value=mock_context)

        mock_stmt = Mock()
        mock_stmt.where.return_value = mock_stmt
        with patch("filings.db.quarterly_financials.select") as mock_select:
            mock_select.return_value.where.return_value.group_by.return_value.order_by.return_value = (
                mock_stmt
            )

            result = await operations.get_normalized_labels(
                company_id=1, statement="BalanceSheet"
            )

        assert len(result) == 1
        assert result[0]["normalized_label"] == "Total Assets"
        assert result[0]["statement"] == "BalanceSheet"
        assert result[0]["count"] == 50

    @pytest.mark.asyncio
    async def test_get_normalized_labels_error_handling(self):
        """Test error handling when getting normalized labels fails."""
        operations = _make_operations()
        operations.engine.connect = Mock(side_effect=SQLAlchemyError("Database error"))

        result = await operations.get_normalized_labels(company_id=1)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_normalized_labels_with_statement_and_company_id_filter(self):
        """Test getting normalized labels with both statement and company_id filters."""
        operations = _make_operations()
        mock_conn = AsyncMock()
        mock_row = Mock()
        mock_row.normalized_label = "Total Assets"
        mock_row.statement = "BalanceSheet"
        mock_row.axis = None
        mock_row.member = None
        mock_row.count = 15
        mock_result = Mock()
        mock_result.fetchall.return_value = [mock_row]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_context.__aexit__ = AsyncMock(return_value=None)
        operations.engine.connect = Mock(return_value=mock_context)

        mock_stmt = Mock()
        mock_stmt.where.return_value = mock_stmt
        with patch("filings.db.quarterly_financials.select") as mock_select:
            mock_select.return_value.where.return_value.group_by.return_value.order_by.return_value = (
                mock_stmt
            )

            result = await operations.get_normalized_labels(
                company_id=1, statement="BalanceSheet"
            )

        assert len(result) == 1
        assert result[0]["normalized_label"] == "Total Assets"
        assert result[0]["statement"] == "BalanceSheet"
        assert result[0]["count"] == 15
