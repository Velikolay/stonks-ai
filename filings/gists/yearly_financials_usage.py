"""Example usage of yearly financials functionality."""

import os

from filings.db import FilingsDatabase
from filings.models.yearly_financials import YearlyFinancialsFilter


def example_yearly_financials_usage():
    """Demonstrate yearly financials functionality."""
    # Initialize database connection
    database_url = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/filings")
    db = FilingsDatabase(database_url)

    try:
        # Example 1: Get all yearly financials for a specific company
        company_id = 1
        yearly_metrics = db.yearly_financials.get_metrics_by_company(company_id)
        print(f"Found {len(yearly_metrics)} yearly metrics for company {company_id}")

        # Example 2: Get yearly financials for a specific company and year
        fiscal_year = 2023
        company_year_metrics = db.yearly_financials.get_metrics_by_company_and_year(
            company_id, fiscal_year
        )
        print(
            f"Found {len(company_year_metrics)} metrics for company {company_id} in {fiscal_year}"
        )

        # Example 3: Get metrics by label for a specific company
        revenue_metrics = db.yearly_financials.get_metrics_by_label(
            company_id, "Revenues"
        )
        print(f"Found {len(revenue_metrics)} revenue metrics for company {company_id}")

        # Example 4: Get metrics by statement type for a specific company
        income_statement_metrics = db.yearly_financials.get_metrics_by_statement(
            company_id, "IncomeStatement"
        )
        print(
            f"Found {len(income_statement_metrics)} income statement metrics for company {company_id}"
        )

        # Example 5: Get latest metrics for a company
        latest_metrics = db.yearly_financials.get_latest_metrics_by_company(
            company_id, limit=10
        )
        print(f"Found {len(latest_metrics)} latest metrics for company {company_id}")

        # Example 6: Use filter parameters for complex queries with ranges
        filter_params = YearlyFinancialsFilter(
            company_id=company_id,
            fiscal_year_start=2020,
            fiscal_year_end=2023,
            statement="IncomeStatement",
        )
        filtered_metrics = db.yearly_financials.get_yearly_financials(filter_params)
        print(f"Found {len(filtered_metrics)} filtered metrics for 2020-2023")

        # Example 7: Print some sample data
        if yearly_metrics:
            print("\nSample yearly financial metrics:")
            for metric in yearly_metrics[:5]:
                print(
                    f"Company {metric.company_id}, Year {metric.fiscal_year}: "
                    f"{metric.label} = {metric.value} {metric.unit or ''}"
                )

        # Example 8: Refresh the materialized view (useful after new data is loaded)
        print("\nRefreshing yearly financials view...")
        db.yearly_financials.refresh_view()
        print("View refreshed successfully")

    finally:
        db.close()


def compare_quarterly_vs_yearly():
    """Compare quarterly vs yearly financials for the same company."""
    database_url = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/filings")
    db = FilingsDatabase(database_url)

    try:
        company_id = 1
        fiscal_year = 2023

        # Get quarterly metrics
        quarterly_metrics = db.quarterly_financials.get_metrics_by_company_and_year(
            company_id, fiscal_year
        )

        # Get yearly metrics
        yearly_metrics = db.yearly_financials.get_metrics_by_company_and_year(
            company_id, fiscal_year
        )

        print(f"Quarterly metrics: {len(quarterly_metrics)}")
        print(f"Yearly metrics: {len(yearly_metrics)}")

        # Compare specific metrics
        quarterly_revenue = [
            m
            for m in quarterly_metrics
            if "revenue" in m.label.lower() and m.fiscal_quarter == 4
        ]
        yearly_revenue = [m for m in yearly_metrics if "revenue" in m.label.lower()]

        if quarterly_revenue and yearly_revenue:
            print(f"Q4 Revenue: {quarterly_revenue[0].value}")
            print(f"Annual Revenue: {yearly_revenue[0].value}")

    finally:
        db.close()


def example_range_queries():
    """Demonstrate range query functionality."""
    database_url = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/filings")
    db = FilingsDatabase(database_url)

    try:
        company_id = 1

        # Example 1: Get metrics for a range of years
        filter_params = YearlyFinancialsFilter(
            company_id=company_id,
            fiscal_year_start=2020,
            fiscal_year_end=2023,
        )
        range_metrics = db.yearly_financials.get_yearly_financials(filter_params)
        print(f"Found {len(range_metrics)} metrics for years 2020-2023")

        # Example 2: Get quarterly metrics for a range of quarters
        quarterly_filter_params = YearlyFinancialsFilter(
            company_id=company_id,
            fiscal_year_start=2023,
            fiscal_year_end=2023,
            fiscal_quarter_start=2,
            fiscal_quarter_end=4,
        )
        quarterly_range_metrics = db.quarterly_financials.get_quarterly_financials(
            quarterly_filter_params
        )
        print(f"Found {len(quarterly_range_metrics)} metrics for Q2-Q4 2023")

    finally:
        db.close()


if __name__ == "__main__":
    example_yearly_financials_usage()
    print("\n" + "=" * 50 + "\n")
    compare_quarterly_vs_yearly()
    print("\n" + "=" * 50 + "\n")
    example_range_queries()
