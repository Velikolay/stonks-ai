"""Base database manager using SQLAlchemy query builder."""

import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Database manager using SQLAlchemy query builder."""

    def __init__(self, database_url: str):
        """Initialize database connection."""
        self.engine: Engine = create_engine(database_url)
        self._test_connection()

    def _test_connection(self) -> None:
        """Test database connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {e}")
            raise

    def close(self) -> None:
        """Close database connection."""
        self.engine.dispose()
        logger.info("Database connection closed")
