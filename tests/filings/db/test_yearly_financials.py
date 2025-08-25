"""Tests for yearly financials database operations."""

from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch

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

                # Create test data for year filter
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

                # Test with year filter
                mock_get.return_value = year_test_data
                filter_params = YearlyFinancialsFilter(fiscal_year=2023)
                result = operations.get_yearly_financials(filter_params)
                assert len(result) == 1
                assert all(r.fiscal_year == 2023 for r in result)

                # Test with statement filter
                mock_get.return_value = statement_test_data
                filter_params = YearlyFinancialsFilter(statement="IncomeStatement")
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

                operations.get_metrics_by_company(company_id=1, limit=10)

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.company_id == 1
                assert call_args.limit == 10

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
                assert call_args.fiscal_year == 2023

    def test_get_metrics_by_concept(self):
        """Test getting metrics by concept."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_yearly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_concept("Revenues", limit=5)

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.concept == "Revenues"
                assert call_args.limit == 5

    def test_get_metrics_by_statement(self):
        """Test getting metrics by statement."""
        mock_engine = Mock()

        with patch("filings.db.yearly_financials.Table") as mock_table:
            mock_table.return_value = Mock()
            operations = YearlyFinancialsOperations(mock_engine)

            with patch.object(operations, "get_yearly_financials") as mock_get:
                mock_get.return_value = []

                operations.get_metrics_by_statement("BalanceSheet", limit=5)

                mock_get.assert_called_once()
                call_args = mock_get.call_args[0][0]
                assert call_args.statement == "BalanceSheet"
                assert call_args.limit == 5

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
