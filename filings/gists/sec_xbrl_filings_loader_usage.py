"""Example usage of the XBRLFilingsLoader to download and persist SEC XBRL filings."""

import logging

from filings import FilingsDatabase
from filings.sec_xbrl_filings_loader import SECXBRLFilingsLoader

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection string (update with your actual database URL)
DATABASE_URL = "postgresql://rag_user:rag_password@localhost:5432/rag_db"


def example_load_aapl_filings():
    """Example: Load Apple's 10-Q XBRL filings."""
    logger.info("Loading Apple's 10-Q XBRL filings...")

    # Initialize database and loader
    database = FilingsDatabase(DATABASE_URL)
    loader = SECXBRLFilingsLoader(database)

    try:
        # Process filings
        result = loader.load_company_filings(
            ticker="NFLX",
            form="10-K",
            limit=20,
        )

        if "error" in result:
            logger.error(f"Error: {result['error']}")
        else:
            logger.info(f"Successfully loaded {result['filings_loaded']} filings")
            logger.info(f"Total facts extracted: {result['total_facts']}")
            logger.info(f"Company ID: {result['company_id']}")
    finally:
        # Close database connection
        database.close()


def example_load_with_override():
    """Example: Load XBRL filings with override to replace existing data."""
    logger.info("Loading Apple's 10-Q XBRL filings with override...")

    # Initialize database and loader
    database = FilingsDatabase(DATABASE_URL)
    loader = SECXBRLFilingsLoader(database)

    try:
        # Process filings with override
        result = loader.load_company_filings(
            ticker="AAPL",
            form="10-Q",
            limit=2,
            override=True,  # Override existing data
        )

        if "error" in result:
            logger.error(f"Error: {result['error']}")
        else:
            logger.info(f"Successfully loaded {result['filings_loaded']} filings")
            logger.info(f"Filings updated: {result['filings_updated']}")
            logger.info(f"Total facts extracted: {result['total_facts']}")
            logger.info(f"Override mode: {result['override_mode']}")
            logger.info(f"Company ID: {result['company_id']}")
    finally:
        # Close database connection
        database.close()


if __name__ == "__main__":
    # Run examples
    example_load_aapl_filings()
    # print("\n" + "=" * 50 + "\n")
    # example_load_with_override()
