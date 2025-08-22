"""Example usage of the quarterly financial metrics view.

This view correctly handles different fiscal year ends by using the actual
fiscal_quarter field from the filings table, rather than assuming 10-K = Q4.
For example, AAPL reports 10-K in Q3 and 10-Qs in Q1, Q2, and Q4.
"""

import logging

from filings import FilingsDatabase, QuarterlyFinancialsFilter

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection string (update with your actual database URL)
DATABASE_URL = "postgresql://rag_user:rag_password@localhost:5432/rag_db"


def example_get_quarterly_metrics():
    """Example: Get quarterly metrics for a company."""
    logger.info("Getting quarterly metrics...")

    # Initialize database
    database = FilingsDatabase(DATABASE_URL)

    try:
        # Example 1: Get all quarterly metrics for a specific company
        company_id = 1  # Replace with actual company ID
        metrics = database.quarterly_financials.get_metrics_by_company(
            company_id, limit=50
        )

        logger.info(f"Found {len(metrics)} quarterly metrics for company {company_id}")
        for metric in metrics[:5]:  # Show first 5
            logger.info(
                f"  {metric.fiscal_year} Q{metric.fiscal_quarter}: "
                f"{metric.label} = {metric.value} {metric.unit or ''} "
                f"({metric.source_type})"
            )

        # Example 2: Get metrics for a specific year
        year_metrics = database.quarterly_financials.get_metrics_by_company_and_year(
            company_id, 2024
        )
        logger.info(f"Found {len(year_metrics)} metrics for 2024")

        # Example 3: Get metrics by label (metric name)
        revenue_metrics = database.quarterly_financials.get_metrics_by_label(
            "Revenue", limit=20
        )
        logger.info(f"Found {len(revenue_metrics)} revenue metrics")

        # Example 4: Get metrics by financial statement
        income_stmt_metrics = database.quarterly_financials.get_metrics_by_statement(
            "Income Statement", limit=20
        )
        logger.info(f"Found {len(income_stmt_metrics)} income statement metrics")

        # Example 5: Use filter for complex queries
        filter_params = QuarterlyFinancialsFilter(
            company_id=company_id,
            fiscal_year=2024,
            fiscal_quarter=1,
            source_type="10-Q",
        )
        filtered_metrics = database.quarterly_financials.get_quarterly_financials(
            filter_params
        )
        logger.info(f"Found {len(filtered_metrics)} Q1 2024 10-Q metrics")

        # Example 6: Get latest metrics
        latest_metrics = database.quarterly_financials.get_latest_metrics_by_company(
            company_id, limit=10
        )
        logger.info(f"Found {len(latest_metrics)} latest metrics")

    finally:
        # Close database connection
        database.close()


def example_analyze_quarterly_trends():
    """Example: Analyze quarterly trends for specific metrics."""
    logger.info("Analyzing quarterly trends...")

    database = FilingsDatabase(DATABASE_URL)

    try:
        company_id = 1  # Replace with actual company ID

        # Get revenue trends
        revenue_filter = QuarterlyFinancialsFilter(
            company_id=company_id, label="Revenue", limit=100
        )
        revenue_metrics = database.quarterly_financials.get_quarterly_financials(
            revenue_filter
        )

        # Group by year and quarter
        revenue_by_period = {}
        for metric in revenue_metrics:
            key = f"{metric.fiscal_year} Q{metric.fiscal_quarter}"
            if key not in revenue_by_period:
                revenue_by_period[key] = []
            revenue_by_period[key].append(metric)

        logger.info("Revenue trends by quarter:")
        for period, metrics in sorted(revenue_by_period.items()):
            total_revenue = sum(m.value for m in metrics)
            logger.info(f"  {period}: {total_revenue}")

    finally:
        database.close()


def example_verify_fiscal_year_end():
    """Example: Verify that the view correctly handles different fiscal year ends."""
    logger.info("Verifying fiscal year end handling...")

    database = FilingsDatabase(DATABASE_URL)

    try:
        company_id = 1  # Replace with actual company ID

        # Get all metrics for a company to see the quarter distribution
        all_metrics = database.quarterly_financials.get_metrics_by_company(
            company_id, limit=200
        )

        # Group by source type and quarter to see the distribution
        distribution = {}
        for metric in all_metrics:
            key = f"{metric.source_type} Q{metric.fiscal_quarter}"
            if key not in distribution:
                distribution[key] = 0
            distribution[key] += 1

        logger.info("Quarter distribution by source type:")
        for key, count in sorted(distribution.items()):
            logger.info(f"  {key}: {count} metrics")

        # This should show that 10-K can appear in any quarter, not just Q4
        # For example, AAPL might show: 10-K Q3, 10-Q Q1, 10-Q Q2, 10-Q Q4

    finally:
        database.close()


if __name__ == "__main__":
    # Run examples
    example_get_quarterly_metrics()
    print("\n" + "=" * 50 + "\n")
    example_analyze_quarterly_trends()
    print("\n" + "=" * 50 + "\n")
    example_verify_fiscal_year_end()
