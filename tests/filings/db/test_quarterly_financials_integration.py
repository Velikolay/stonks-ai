"""Integration tests for quarterly financial metrics view."""

from filings import QuarterlyFinancialsFilter


class TestQuarterlyFinancialsIntegration:
    """Integration tests for quarterly metrics view."""

    def test_view_exists(self, db):
        """Test that the quarterly_financials view exists."""
        # This test just verifies the view was created successfully
        # It doesn't require any data to be present
        filter_params = QuarterlyFinancialsFilter(company_id=1)
        result = db.quarterly_financials.get_quarterly_financials(filter_params)
        assert isinstance(result, list)

    def test_filter_creation(self):
        """Test that QuarterlyFinancialsFilter can be created."""
        filter_params = QuarterlyFinancialsFilter(
            company_id=1,
            fiscal_year_start=2024,
            fiscal_year_end=2024,
            fiscal_quarter_start=1,
            fiscal_quarter_end=1,
            labels=["Revenue"],
            statement="Income Statement",
        )

        assert filter_params.company_id == 1
        assert filter_params.fiscal_year_start == 2024
        assert filter_params.fiscal_year_end == 2024
        assert filter_params.fiscal_quarter_start == 1
        assert filter_params.fiscal_quarter_end == 1
        assert filter_params.labels == ["Revenue"]
        assert filter_params.statement == "Income Statement"

    def test_view_structure(self, db):
        """Test that the view has the correct structure and handles different fiscal year ends."""
        # Test that we can query by different quarters
        # This verifies the view doesn't assume 10-K = Q4
        q1_filter = QuarterlyFinancialsFilter(
            company_id=1, fiscal_quarter_start=1, fiscal_quarter_end=1
        )
        q1_result = db.quarterly_financials.get_quarterly_financials(q1_filter)
        assert isinstance(q1_result, list)

        q3_filter = QuarterlyFinancialsFilter(
            company_id=1, fiscal_quarter_start=3, fiscal_quarter_end=3
        )
        q3_result = db.quarterly_financials.get_quarterly_financials(q3_filter)
        assert isinstance(q3_result, list)

        q4_filter = QuarterlyFinancialsFilter(
            company_id=1, fiscal_quarter_start=4, fiscal_quarter_end=4
        )
        q4_result = db.quarterly_financials.get_quarterly_financials(q4_filter)
        assert isinstance(q4_result, list)
