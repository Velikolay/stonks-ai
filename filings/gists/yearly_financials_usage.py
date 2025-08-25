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
        yearly_metrics = db.yearly_financials.get_metrics_by_company(
            company_id, limit=50
        )
        print(f"Found {len(yearly_metrics)} yearly metrics for company {company_id}")

        # Example 2: Get yearly financials for a specific company and year
        fiscal_year = 2023
        company_year_metrics = db.yearly_financials.get_metrics_by_company_and_year(
            company_id, fiscal_year
        )
        print(
            f"Found {len(company_year_metrics)} metrics for company {company_id} in {fiscal_year}"
        )

        # Example 3: Get metrics by concept (e.g., revenue)
        revenue_metrics = db.yearly_financials.get_metrics_by_concept(
            "Revenues", limit=20
        )
        print(f"Found {len(revenue_metrics)} revenue metrics")

        # Example 4: Get metrics by statement type
        income_statement_metrics = db.yearly_financials.get_metrics_by_statement(
            "IncomeStatement", limit=30
        )
        print(f"Found {len(income_statement_metrics)} income statement metrics")

        # Example 5: Get latest metrics for a company
        latest_metrics = db.yearly_financials.get_latest_metrics_by_company(
            company_id, limit=10
        )
        print(f"Found {len(latest_metrics)} latest metrics for company {company_id}")

        # Example 6: Use filter parameters for complex queries
        filter_params = YearlyFinancialsFilter(
            company_id=company_id,
            fiscal_year=2023,
            statement="IncomeStatement",
            limit=20,
        )
        filtered_metrics = db.yearly_financials.get_yearly_financials(filter_params)
        print(f"Found {len(filtered_metrics)} filtered metrics")

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


if __name__ == "__main__":
    example_yearly_financials_usage()
    print("\n" + "=" * 50 + "\n")
    compare_quarterly_vs_yearly()
