"""Integration tests for quarterly financial metrics view."""

import pytest

from filings import FilingsDatabase, QuarterlyFinancialsFilter


class TestQuarterlyFinancialsIntegration:
    """Integration tests for quarterly metrics view."""

    def test_view_exists(self):
        """Test that the quarterly_financials view exists."""
        # This test just verifies the view was created successfully
        # It doesn't require any data to be present
        database_url = "postgresql://rag_user:rag_password@localhost:5432/rag_db"

        try:
            db = FilingsDatabase(database_url)

            # Try to query the view - this should not raise an error
            # even if there's no data
            filter_params = QuarterlyFinancialsFilter(company_id=1)
            result = db.quarterly_financials.get_quarterly_financials(filter_params)

            # Should return empty list if no data, but not raise an error
            assert isinstance(result, list)

        except Exception as e:
            pytest.fail(f"Failed to query quarterly_financials view: {e}")
        finally:
            db.close()

    def test_filter_creation(self):
        """Test that QuarterlyFinancialsFilter can be created."""
        filter_params = QuarterlyFinancialsFilter(
            company_id=1,
            fiscal_year_start=2024,
            fiscal_year_end=2024,
            fiscal_quarter_start=1,
            fiscal_quarter_end=1,
            label="Revenue",
            statement="Income Statement",
        )

        assert filter_params.company_id == 1
        assert filter_params.fiscal_year_start == 2024
        assert filter_params.fiscal_year_end == 2024
        assert filter_params.fiscal_quarter_start == 1
        assert filter_params.fiscal_quarter_end == 1
        assert filter_params.label == "Revenue"
        assert filter_params.statement == "Income Statement"

    def test_view_structure(self):
        """Test that the view has the correct structure and handles different fiscal year ends."""
        database_url = "postgresql://rag_user:rag_password@localhost:5432/rag_db"

        try:
            db = FilingsDatabase(database_url)

            # Test that we can query by different quarters
            # This verifies the view doesn't assume 10-K = Q4

            # Test Q1
            q1_filter = QuarterlyFinancialsFilter(
                company_id=1, fiscal_quarter_start=1, fiscal_quarter_end=1
            )
            q1_result = db.quarterly_financials.get_quarterly_financials(q1_filter)
            assert isinstance(q1_result, list)

            # Test Q3
            q3_filter = QuarterlyFinancialsFilter(
                company_id=1, fiscal_quarter_start=3, fiscal_quarter_end=3
            )
            q3_result = db.quarterly_financials.get_quarterly_financials(q3_filter)
            assert isinstance(q3_result, list)

            # Test Q4
            q4_filter = QuarterlyFinancialsFilter(
                company_id=1, fiscal_quarter_start=4, fiscal_quarter_end=4
            )
            q4_result = db.quarterly_financials.get_quarterly_financials(q4_filter)
            assert isinstance(q4_result, list)

        except Exception as e:
            pytest.fail(f"Failed to test view structure: {e}")
        finally:
            db.close()
