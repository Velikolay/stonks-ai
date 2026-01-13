"""Database operations package."""

from .base import DatabaseManager
from .companies import CompanyOperations
from .concept_normalization_overrides import ConceptNormalizationOverridesOperations
from .dimension_normalization_overrides import DimensionNormalizationOverridesOperations
from .filings import FilingOperations
from .financial_facts import FinancialFactOperations
from .quarterly_financials import QuarterlyFinancialsOperations
from .yearly_financials import YearlyFinancialsOperations


class FilingsDatabase:
    """Unified database interface combining all operations."""

    def __init__(self, database_url: str):
        """Initialize database with all operation classes."""
        self.manager = DatabaseManager(database_url)
        self.companies = CompanyOperations(self.manager.engine)
        self.filings = FilingOperations(self.manager.engine)
        self.financial_facts = FinancialFactOperations(self.manager.engine)
        self.quarterly_financials = QuarterlyFinancialsOperations(self.manager.engine)
        self.yearly_financials = YearlyFinancialsOperations(self.manager.engine)
        self.concept_normalization_overrides = ConceptNormalizationOverridesOperations(
            self.manager.engine
        )
        self.dimension_normalization_overrides = (
            DimensionNormalizationOverridesOperations(self.manager.engine)
        )

    def close(self) -> None:
        """Close database connection."""
        self.manager.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
