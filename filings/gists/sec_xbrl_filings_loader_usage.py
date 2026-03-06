"""Example usage of the XBRLFilingsLoader to download and persist SEC XBRL filings."""

import asyncio
import logging

from filings.db import AsyncFilingsDatabase
from filings.sec_xbrl_filings_loader import SECXBRLFilingsLoader

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection string (update with your actual database URL)
DATABASE_URL = "postgresql://rag_user:rag_password@localhost:5432/rag_db"


async def example_load_filings():
    """Example: Load 10-Q/K XBRL filings."""
    logger.info("Loading 10-Q/K XBRL filings...")

    database = AsyncFilingsDatabase(DATABASE_URL)
    await database.initialize()
    loader = SECXBRLFilingsLoader(database)

    try:
        result = await loader.load_company_filings(
            ticker="GOOGL",
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
        await database.aclose()


async def example_load_with_override():
    """Example: Load XBRL filings with override to replace existing data."""
    logger.info("Loading 10-Q/K XBRL filings with override...")

    database = AsyncFilingsDatabase(DATABASE_URL)
    await database.initialize()
    loader = SECXBRLFilingsLoader(database)

    try:
        result = await loader.load_company_filings(
            ticker="AAPL",
            form="10-Q",
            limit=2,
            override=True,
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
        await database.aclose()


if __name__ == "__main__":
    asyncio.run(example_load_filings())
    # asyncio.run(example_load_with_override())
