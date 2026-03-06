"""Database operations package."""

from __future__ import annotations

import logging
from typing import List

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .companies import CompanyOperationsAsync
from .concept_normalization_overrides import (
    ConceptNormalizationOverridesOperationsAsync,
)
from .dimension_normalization_overrides import (
    DimensionNormalizationOverridesOperationsAsync,
)
from .filings import FilingOperationsAsync
from .financial_facts import FinancialFactOperationsAsync
from .financial_facts_overrides import FinancialFactsOverridesOperationsAsync
from .quarterly_financials import QuarterlyFinancialsOperationsAsync
from .yearly_financials import YearlyFinancialsOperationsAsync

logger = logging.getLogger(__name__)


def _to_async_url(database_url: str) -> str:
    """Convert sync database URL to async (postgresql+asyncpg)."""
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+asyncpg://", 1)
    if database_url.startswith("postgresql+pg8000://"):
        return database_url.replace("postgresql+pg8000://", "postgresql+asyncpg://", 1)
    return database_url


class AsyncFilingsDatabase:
    """Unified async database interface combining all native async operations."""

    def __init__(self, database_url: str):
        """Initialize async engine and all operation classes."""
        async_url = _to_async_url(database_url)
        self._engine: AsyncEngine = create_async_engine(async_url)
        self._metadata = MetaData()

    async def _reflect_metadata(self) -> None:
        """Reflect database schema into metadata."""
        async with self._engine.connect() as conn:
            await conn.run_sync(
                lambda sync_conn: self._metadata.reflect(bind=sync_conn)
            )

    async def initialize(self) -> None:
        """Reflect metadata and create operation instances. Call after construction."""
        await self._reflect_metadata()
        self.companies = CompanyOperationsAsync(self._engine, self._metadata)
        self.filings = FilingOperationsAsync(self._engine, self._metadata)
        self.financial_facts = FinancialFactOperationsAsync(
            self._engine, self._metadata
        )
        self.quarterly_financials = QuarterlyFinancialsOperationsAsync(
            self._engine, self._metadata
        )
        self.yearly_financials = YearlyFinancialsOperationsAsync(
            self._engine, self._metadata
        )
        self.concept_normalization_overrides = (
            ConceptNormalizationOverridesOperationsAsync(self._engine, self._metadata)
        )
        self.dimension_normalization_overrides = (
            DimensionNormalizationOverridesOperationsAsync(self._engine, self._metadata)
        )
        self.financial_facts_overrides = FinancialFactsOverridesOperationsAsync(
            self._engine, self._metadata
        )

    async def refresh_financials_for_companies(self, company_ids: List[int]) -> None:
        """Recompute normalization + quarterly/yearly financials for companies."""
        if not company_ids:
            return
        async with self._engine.begin() as conn:
            await conn.execute(
                text("CALL refresh_financials(:company_ids)"),
                {"company_ids": company_ids},
            )

    async def aclose(self) -> None:
        """Dispose of the async engine."""
        await self._engine.dispose()
        logger.info("Async database connection closed")

    @property
    def engine(self) -> AsyncEngine:
        """Return the async engine."""
        return self._engine


__all__ = [
    "AsyncFilingsDatabase",
    "CompanyOperationsAsync",
    "ConceptNormalizationOverridesOperationsAsync",
    "DimensionNormalizationOverridesOperationsAsync",
    "FilingOperationsAsync",
    "FinancialFactOperationsAsync",
    "FinancialFactsOverridesOperationsAsync",
    "QuarterlyFinancialsOperationsAsync",
    "YearlyFinancialsOperationsAsync",
]
