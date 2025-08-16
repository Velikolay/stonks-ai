"""Example usage of the database module."""

from datetime import date
from decimal import Decimal

from filings import CompanyCreate, FilingCreate, FilingsDatabase, FinancialFactCreate

# Database connection string (update with your actual database URL)
DATABASE_URL = "postgresql://rag_user:rag_password@localhost:5432/rag_db"


def example_usage():
    """Example of how to use the database module."""

    # Initialize database
    with FilingsDatabase(DATABASE_URL) as db:

        # 1. Create a company
        print("1. Creating company...")
        company_data = CompanyCreate(
            ticker="AAPL", exchange="NASDAQ", name="Apple Inc."
        )

        company = db.companies.get_or_create_company(company_data)
        if company:
            print(f"Company created/found: {company.name} (ID: {company.id})")

        # 2. Create a filing
        print("\n2. Creating filing...")
        filing_data = FilingCreate(
            company_id=company.id,
            source="SEC",
            filing_number="0000320193-25-000073",
            form_type="10-Q",
            filing_date=date(2024, 12, 19),
            fiscal_period_end=date(2024, 9, 28),
            fiscal_year=2024,
            fiscal_quarter=4,
            public_url="https://www.sec.gov/Archives/edgar/data/320193/000032019325000073/aapl-20240928.htm",
        )

        filing = db.filings.get_or_create_filing(filing_data)
        if filing:
            print(f"Filing created/found: {filing.filing_number} (ID: {filing.id})")

        # 3. Create financial facts
        print("\n3. Creating financial facts...")
        facts_data = [
            FinancialFactCreate(
                filing_id=filing.id,
                metric="Revenue",
                value=Decimal("89498.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period_start=date(2024, 6, 30),
            ),
            FinancialFactCreate(
                filing_id=filing.id,
                metric="Net Income",
                value=Decimal("22956.0"),
                unit="USD",
                statement="Income Statement",
                period_end=date(2024, 9, 28),
                period_start=date(2024, 6, 30),
            ),
            FinancialFactCreate(
                filing_id=filing.id,
                metric="Total Assets",
                value=Decimal("352755.0"),
                unit="USD",
                statement="Balance Sheet",
                period_end=date(2024, 9, 28),
            ),
        ]

        fact_ids = db.financial_facts.insert_financial_facts_batch(facts_data)
        print(f"Created {len(fact_ids)} financial facts")

        # 4. Query data
        print("\n4. Querying data...")

        # Get all companies
        companies = db.companies.get_all_companies()
        print(f"Total companies: {len(companies)}")

        # Get filings for the company
        filings = db.filings.get_filings_by_company(company.id, "10-Q")
        print(f"10-Q filings for {company.name}: {len(filings)}")

        # Get financial facts for the filing
        facts = db.financial_facts.get_financial_facts_by_filing(filing.id)
        print(f"Financial facts for filing: {len(facts)}")

        # 5. Display some results
        print("\n5. Sample data:")
        if facts:
            print("Financial facts:")
            for fact in facts[:3]:  # Show first 3 facts
                print(f"  - {fact.metric}: {fact.value} {fact.unit}")


if __name__ == "__main__":
    print("Database Module Example Usage")
    print("=" * 40)

    try:
        example_usage()
    except Exception as e:
        print(f"Error: {e}")
