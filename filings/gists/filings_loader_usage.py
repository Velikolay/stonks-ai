"""Example usage of the FilingsLoader to download and persist SEC filings."""

import logging

from filings import FilingsDatabase
from filings.filings_loader import FilingsLoader

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection string (update with your actual database URL)
DATABASE_URL = "postgresql://rag_user:rag_password@localhost:5432/rag_db"


def example_load_aapl_filings():
    """Example: Load Apple's 10-Q filings."""
    logger.info("Loading Apple's 10-Q filings...")

    # Initialize database and loader
    database = FilingsDatabase(DATABASE_URL)
    loader = FilingsLoader(database)

    try:
        # Process filings
        result = loader.load_company_filings(
            ticker="AAPL",
            form="10-Q",
            limit=1,
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


if __name__ == "__main__":
    # Run examples
    example_load_aapl_filings()
