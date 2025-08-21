"""Example usage of the SEC XBRL parser."""

import logging

from edgar import set_identity

from filings.parsers.geography import GeographyParser
from filings.parsers.sec_xbrl import SECXBRLParser

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def example_parse_company_filings():
    """Example: Parse multiple filings for a company."""
    print("=== Example: Parse Company Filings ===")

    ticker = "AAPL"
    set_identity("felinephonix@gmail.com")

    try:
        parser = SECXBRLParser(GeographyParser())

        # Parse the latest 3 10-Q filings for Apple
        facts = parser.parse_company_filings(ticker, form="10-Q", limit=1)

        print(f"Parsed {len(facts)} total financial facts from {ticker}")

        # Group facts by statement type
        by_statement = {}
        for fact in facts:
            statement = fact.statement
            if statement not in by_statement:
                by_statement[statement] = []
            by_statement[statement].append(fact)

        print("\nFacts by statement type:")
        for statement, statement_facts in by_statement.items():
            print(f"  {statement}: {len(statement_facts)} facts")

        # Show some revenue facts
        revenue_facts = [f for f in facts if "Revenue" in f.concept]
        if revenue_facts:
            print(f"\nFound {len(revenue_facts)} revenue-related facts:")
            for fact in revenue_facts[:3]:
                print(f"  - {fact.concept}: {fact.value:,.0f} {fact.unit or ''}")

    except Exception as e:
        print(f"Error parsing company filings: {e}")


if __name__ == "__main__":
    print("SEC 10-Q XBRL Parser Examples")
    print("=" * 50)

    example_parse_company_filings()
    print("\n" + "=" * 50)

    print("Examples completed!")
