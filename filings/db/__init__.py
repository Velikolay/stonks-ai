"""Database operations package."""

from sqlalchemy import text

from .base import DatabaseManager
from .companies import CompanyOperations
from .concept_normalization_overrides import ConceptNormalizationOverridesOperations
from .dimension_normalization_overrides import DimensionNormalizationOverridesOperations
from .filings import FilingOperations
from .financial_facts import FinancialFactOperations
from .financial_facts_overrides import FinancialFactsOverridesOperations
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
        self.financial_facts_overrides = FinancialFactsOverridesOperations(
            self.manager.engine
        )

    def close(self) -> None:
        """Close database connection."""
        self.manager.close()

    def refresh_financials_for_companies(self, company_ids: list[int]) -> None:
        """Recompute normalization + quarterly/yearly financials for companies."""
        if not company_ids:
            return
        with self.manager.engine.connect() as conn:
            conn.execute(
                text("CALL refresh_financials(:company_ids)"),
                {"company_ids": company_ids},
            )
            conn.commit()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
