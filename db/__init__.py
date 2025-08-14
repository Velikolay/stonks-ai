"""Database module with unified interface for all operations."""

from .models.base import (
    Company,
    CompanyCreate,
    Filing,
    FilingCreate,
    FinancialFact,
    FinancialFactCreate,
)
from .operations.base import DatabaseManager
from .operations.companies import CompanyOperations
from .operations.filings import FilingOperations
from .operations.financial_facts import FinancialFactOperations

__all__ = [
    "DatabaseManager",
    "CompanyOperations",
    "FilingOperations",
    "FinancialFactOperations",
    "Company",
    "CompanyCreate",
    "Filing",
    "FilingCreate",
    "FinancialFact",
    "FinancialFactCreate",
]


class Database:
    """Unified database interface combining all operations."""

    def __init__(self, database_url: str):
        """Initialize database with all operation classes."""
        self.manager = DatabaseManager(database_url)
        self.companies = CompanyOperations(self.manager.engine)
        self.filings = FilingOperations(self.manager.engine)
        self.financial_facts = FinancialFactOperations(self.manager.engine)

    def close(self) -> None:
        """Close database connection."""
        self.manager.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
