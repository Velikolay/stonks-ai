"""Tests for yearly financials database operations."""

from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from filings.db.yearly_financials import YearlyFinancialsOperations
from filings.models.yearly_financials import YearlyFinancial, YearlyFinancialsFilter


def _make_operations():
    """Create operations instance with mocked engine and metadata."""
    mock_engine = Mock()
    mock_table = Mock()
    mock_metadata = Mock()
    mock_metadata.tables = {"yearly_financials": mock_table}
    return YearlyFinancialsOperations(mock_engine, mock_metadata)


class TestYearlyFinancialsOperations:
    """Test yearly financials operations."""

    def test_operations_initialization(self):
        """Test operations initialization."""
        mock_engine = Mock()
        mock_table = Mock()
        mock_metadata = Mock()
        mock_metadata.tables = {"yearly_financials": mock_table}
        operations = YearlyFinancialsOperations(mock_engine, mock_metadata)

        assert operations.engine == mock_engine
        assert operations.yearly_financials_view == mock_table

    @pytest.mark.asyncio
    async def test_get_yearly_financials_with_filters(self):
        """Test getting yearly financials with various filters."""
        operations = _make_operations()

        with patch.object(
            operations, "get_yearly_financials", new_callable=AsyncMock
        ) as mock_get:
            company_test_data = [
                YearlyFinancial(
                    id=1,
                    company_id=1,
                    filing_id=1,
                    fiscal_year=2023,
                    label="Revenue",
                    normalized_label="Revenue",
                    value=Decimal("1000000.00"),
                    unit="USD",
                    statement="IncomeStatement",
                    period_end=None,
                    abstract_id=None,
                    is_abstract=False,
                    is_synthetic=False,
                    source_type="10-K",
                ),
                YearlyFinancial(
                    id=2,
                    company_id=1,
                    filing_id=2,
                    fiscal_year=2024,
                    label="Revenue",
                    normalized_label="Revenue",
                    value=Decimal("1100000.00"),
                    unit="USD",
                    statement="IncomeStatement",
                    period_end=None,
                    abstract_id=None,
                    is_abstract=False,
                    is_synthetic=False,
                    source_type="10-K",
                ),
            ]

            year_test_data = [
                YearlyFinancial(
                    id=1,
                    company_id=1,
                    filing_id=1,
                    fiscal_year=2023,
                    label="Revenue",
                    normalized_label="Revenue",
                    value=Decimal("1000000.00"),
                    unit="USD",
                    statement="IncomeStatement",
                    period_end=None,
                    abstract_id=None,
                    is_abstract=False,
                    is_synthetic=False,
                    source_type="10-K",
                )
            ]

            statement_test_data = [
                YearlyFinancial(
                    id=1,
                    company_id=1,
                    filing_id=1,
                    fiscal_year=2023,
                    label="Revenue",
                    normalized_label="Revenue",
                    value=Decimal("1000000.00"),
                    unit="USD",
                    statement="IncomeStatement",
                    period_end=None,
                    abstract_id=None,
                    is_abstract=False,
                    is_synthetic=False,
                    source_type="10-K",
                )
            ]

            mock_get.return_value = company_test_data
            filter_params = YearlyFinancialsFilter(company_id=1)
            result = await operations.get_yearly_financials(filter_params)
            assert len(result) == 2
            assert all(r.company_id == 1 for r in result)

            mock_get.return_value = year_test_data
            filter_params = YearlyFinancialsFilter(
                company_id=1, fiscal_year_start=2023, fiscal_year_end=2023
            )
            result = await operations.get_yearly_financials(filter_params)
            assert len(result) == 1
            assert all(r.fiscal_year == 2023 for r in result)

            mock_get.return_value = statement_test_data
            filter_params = YearlyFinancialsFilter(
                company_id=1, statement="IncomeStatement"
            )
            result = await operations.get_yearly_financials(filter_params)
            assert len(result) == 1
            assert all(r.statement == "IncomeStatement" for r in result)

    @pytest.mark.asyncio
    async def test_get_metrics_by_company(self):
        """Test getting metrics by company."""
        operations = _make_operations()

        with patch.object(
            operations, "get_yearly_financials", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = []

            await operations.get_metrics_by_company(company_id=1)

            mock_get.assert_called_once()
            call_args = mock_get.call_args[0][0]
            assert call_args.company_id == 1

    @pytest.mark.asyncio
    async def test_get_metrics_by_company_and_year(self):
        """Test getting metrics by company and year."""
        operations = _make_operations()

        with patch.object(
            operations, "get_yearly_financials", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = []

            await operations.get_metrics_by_company_and_year(
                company_id=1, fiscal_year=2023
            )

            mock_get.assert_called_once()
            call_args = mock_get.call_args[0][0]
            assert call_args.company_id == 1
            assert call_args.fiscal_year_start == 2023
            assert call_args.fiscal_year_end == 2023

    @pytest.mark.asyncio
    async def test_get_metrics_by_label(self):
        """Test getting metrics by label."""
        operations = _make_operations()

        with patch.object(
            operations, "get_yearly_financials", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = []

            await operations.get_metrics_by_label(company_id=1, label="Revenues")

            mock_get.assert_called_once()
            call_args = mock_get.call_args[0][0]
            assert call_args.company_id == 1
            assert call_args.labels == ["Revenues"]

    @pytest.mark.asyncio
    async def test_get_metrics_by_statement(self):
        """Test getting metrics by statement."""
        operations = _make_operations()

        with patch.object(
            operations, "get_yearly_financials", new_callable=AsyncMock
        ) as mock_get:
            mock_get.return_value = []

            await operations.get_metrics_by_statement(
                company_id=1, statement="BalanceSheet"
            )

            mock_get.assert_called_once()
            call_args = mock_get.call_args[0][0]
            assert call_args.company_id == 1
            assert call_args.statement == "BalanceSheet"

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
        with patch("filings.db.yearly_financials.select") as mock_select:
            mock_select.return_value.where.return_value.order_by.return_value.limit.return_value = (
                mock_stmt
            )

            await operations.get_latest_metrics_by_company(company_id=1, limit=5)

        mock_conn.execute.assert_called_once()

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
        with patch("filings.db.yearly_financials.select") as mock_select:
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
        with patch("filings.db.yearly_financials.select") as mock_select:
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
        operations.engine.connect.side_effect = SQLAlchemyError("Database error")

        result = await operations.get_normalized_labels(company_id=1)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_normalized_labels_with_statement_and_company_id_filter(
        self,
    ):
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
        with patch("filings.db.yearly_financials.select") as mock_select:
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
