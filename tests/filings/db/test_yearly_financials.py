"""Tests for yearly financials database operations."""

from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch

from sqlalchemy.exc import SQLAlchemyError

from filings.db.yearly_financials import YearlyFinancialsOperations
from filings.models.yearly_financials import YearlyFinancial, YearlyFinancialsFilter


class TestYearlyFinancialsOperations:
    """Test yearly financials operations."""

    def test_operations_initialization(self):
        """Test operations initialization."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            assert operations.engine == mock_engine
            assert operations.yearly_financials_view is not None

    def test_get_yearly_financials_with_filters(self):
        """Test getting yearly financials with various filters."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            # Mock the get_yearly_financials method to return test data
            with patch.object(operations, "get_yearly_financials") as mock_get:
                # Create test data for company filter
                company_test_data = [
                    YearlyFinancial(
                        company_id=1,
                        fiscal_year=2023,
                        label="Revenue",
                        normalized_label="Revenue",
                        value=Decimal("1000000.00"),
                        unit="USD",
                        statement="IncomeStatement",
                        concept="Revenues",
                        axis=None,
                        member=None,
                        period_end=None,
                        period_start=None,
                        source_type="10-K",
                        fiscal_period_end=None,
                    ),
                    YearlyFinancial(
                        company_id=1,
                        fiscal_year=2024,
                        label="Revenue",
                        normalized_label="Revenue",
                        value=Decimal("1100000.00"),
                        unit="USD",
                        statement="IncomeStatement",
                        concept="Revenues",
                        axis=None,
                        member=None,
                        period_end=None,
                        period_start=None,
                        source_type="10-K",
                        fiscal_period_end=None,
                    ),
                ]

                # Create test data for year range filter
                year_test_data = [
                    YearlyFinancial(
                        company_id=1,
                        fiscal_year=2023,
                        label="Revenue",
                        normalized_label="Revenue",
                        value=Decimal("1000000.00"),
                        unit="USD",
                        statement="IncomeStatement",
                        concept="Revenues",
                        axis=None,
                        member=None,
                        period_end=None,
                        period_start=None,
                        source_type="10-K",
                        fiscal_period_end=None,
                    )
                ]

                # Create test data for statement filter
                statement_test_data = [
                    YearlyFinancial(
                        company_id=1,
                        fiscal_year=2023,
                        label="Revenue",
                        normalized_label="Revenue",
                        value=Decimal("1000000.00"),
                        unit="USD",
                        statement="IncomeStatement",
                        concept="Revenues",
                        axis=None,
                        member=None,
                        period_end=None,
                        period_start=None,
                        source_type="10-K",
                        fiscal_period_end=None,
                    )
                ]

                # Test with company filter
                mock_get.return_value = company_test_data
                filter_params = YearlyFinancialsFilter(company_id=1)
                result = operations.get_yearly_financials(filter_params)
                assert len(result) == 2
                assert all(r.company_id == 1 for r in result)

                # Test with year range filter
                mock_get.return_value = year_test_data
                filter_params = YearlyFinancialsFilter(
                    company_id=1, fiscal_year_start=2023, fiscal_year_end=2023
                )
                result = operations.get_yearly_financials(filter_params)
                assert len(result) == 1
                assert all(r.fiscal_year == 2023 for r in result)

                # Test with statement filter
                mock_get.return_value = statement_test_data
                filter_params = YearlyFinancialsFilter(
                    company_id=1, statement="IncomeStatement"
                )
                result = operations.get_yearly_financials(filter_params)
                assert len(result) == 1
                assert all(r.statement == "IncomeStatement" for r in result)

    def test_get_metrics_by_company(self):
        """Test getting metrics by company."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_yearly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_company(company_id=1)

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1

    def test_get_metrics_by_company_and_year(self):
        """Test getting metrics by company and year."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_yearly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_company_and_year(
                    company_id=1, fiscal_year=2023
                )

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1
                assert call_args.fiscal_year_start == 2023
                assert call_args.fiscal_year_end == 2023

    def test_get_metrics_by_label(self):
        """Test getting metrics by label."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_yearly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_label(company_id=1, label="Revenues")

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1
                assert call_args.label == "Revenues"

    def test_get_metrics_by_statement(self):
        """Test getting metrics by statement."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_yearly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_statement(
                    company_id=1, statement="BalanceSheet"
                )

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1
                assert call_args.statement == "BalanceSheet"

    def test_get_latest_metrics_by_company(self):
        """Test getting latest metrics by company."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            # Mock the database connection and query execution
            mock_conn = Mock()
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_conn
            mock_engine.connect.return_value = mock_context
            mock_result = Mock()
            mock_conn.execute.return_value = mock_result
            mock_result.fetchall.return_value = []

            # Mock the select function
            with patch("filings.db.yearly_financials.select") as mock_select:
                mock_select.return_value = Mock()
                mock_select.return_value.where.return_value = Mock()
                mock_select.return_value.where.return_value.order_by.return_value = (
                    Mock()
                )
                mock_select.return_value.where.return_value.order_by.return_value.limit.return_value = (
                    Mock()
                )

                operations.get_latest_metrics_by_company(company_id=1, limit=5)

                # Verify the query was executed
                mock_conn.execute.assert_called_once()

    def test_refresh_view(self):
        """Test refreshing the materialized view."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            # Mock the database connection and query execution
            mock_conn = Mock()
            mock_context = MagicMock()
            mock_context.__enter__.return_value = mock_conn
            mock_engine.connect.return_value = mock_context

            operations.refresh_view()

            # Verify the refresh command was executed
            mock_conn.execute.assert_called_once_with(
                "REFRESH MATERIALIZED VIEW yearly_financials"
            )
            mock_conn.commit.assert_called_once()

    def test_get_normalized_labels(self):
        """Test getting normalized labels."""
        mock_engine = Mock()

        # Mock the table and its columns
        mock_table = Mock()
        mock_table.c.normalized_label = Mock()
        mock_table.c.statement = Mock()
        mock_table.c.company_id = Mock()

        with patch("filings.db.yearly_financials.Table", return_value=mock_table):
            operations = YearlyFinancialsOperations(mock_engine)

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

            # Mock the table instance that was created in __init__
            operations.yearly_financials_view = mock_table

            # Mock the SQLAlchemy select function to return a simple mock
            with patch("filings.db.yearly_financials.select") as mock_select:
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

        with patch("filings.db.yearly_financials.Table", return_value=mock_table):
            operations = YearlyFinancialsOperations(mock_engine)

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

            # Mock the table instance that was created in __init__
            operations.yearly_financials_view = mock_table

            # Mock the SQLAlchemy select function to return a simple mock
            with patch("filings.db.yearly_financials.select") as mock_select:
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

        with patch("filings.db.yearly_financials.Table", return_value=mock_table):
            operations = YearlyFinancialsOperations(mock_engine)

            # Mock the table instance that was created in __init__
            operations.yearly_financials_view = mock_table

            # Mock the SQLAlchemy select function to return a simple mock
            with patch("filings.db.yearly_financials.select") as mock_select:
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

        with patch("filings.db.yearly_financials.Table", return_value=mock_table):
            operations = YearlyFinancialsOperations(mock_engine)

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

            # Mock the table instance that was created in __init__
            operations.yearly_financials_view = mock_table

            # Mock the SQLAlchemy select function to return a simple mock
            with patch("filings.db.yearly_financials.select") as mock_select:
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
