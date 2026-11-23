"""Stored foreign-currency rates used to translate API amounts into USD."""

import logging
from datetime import date, timedelta
from decimal import Decimal
from typing import Dict, Iterable, List, Sequence, Tuple

from sqlalchemy import MetaData, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from filings.exchange_rates import LOOKBACK_DAYS

logger = logging.getLogger(__name__)
# asyncpg rejects a statement with more than 32767 parameters. Three columns
# per row, so a batch stays well under that limit.
_UPSERT_BATCH = 5000


class ExchangeRateOperationsAsync:
    """Read daily rates from ``usd_exchange_rates``."""

    def __init__(self, engine: AsyncEngine, metadata: MetaData):
        """Initialize with the async engine and reflected metadata."""
        self.engine = engine
        self.rates_table = metadata.tables["usd_exchange_rates"]
        self.financial_facts_table = metadata.tables["financial_facts"]

    async def series(
        self, currencies: Sequence[str], start: date, end: date
    ) -> Dict[str, List[Tuple[date, Decimal]]]:
        """Return dated rates for each currency, sorted by date.

        Args:
            currencies: ISO currency codes.
            start: Earliest period end that needs a rate.
            end: Latest period end that needs a rate.

        Returns:
            Currency code to ``(rate_date, units_per_usd)`` pairs. A currency
            with no stored rate is absent.
        """
        if not currencies:
            return {}
        rates = self.rates_table
        statement = (
            select(rates.c.currency, rates.c.rate_date, rates.c.units_per_usd)
            .where(
                rates.c.currency.in_(list(currencies)),
                rates.c.rate_date.between(start - timedelta(days=LOOKBACK_DAYS), end),
            )
            .order_by(rates.c.currency, rates.c.rate_date)
        )
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(statement)
                rows = result.all()
        except SQLAlchemyError:
            logger.exception("Could not read USD exchange rates")
            raise
        series: Dict[str, List[Tuple[date, Decimal]]] = {}
        for currency, rate_date, units_per_usd in rows:
            series.setdefault(str(currency), []).append(
                (rate_date, Decimal(units_per_usd))
            )
        return series

    async def unit_counts(
        self, company_ids: Sequence[int]
    ) -> Dict[int, Dict[str, int]]:
        """Count stored unit refs for each company.

        Args:
            company_ids: Companies whose facts were just ingested.

        Returns:
            Company id to unit ref to fact count.
        """
        if not company_ids:
            return {}
        facts = self.financial_facts_table
        statement = (
            select(facts.c.company_id, facts.c.unit, func.count())
            .where(
                facts.c.company_id.in_(list(company_ids)),
                facts.c.unit.is_not(None),
                func.btrim(facts.c.unit) != "",
            )
            .group_by(facts.c.company_id, facts.c.unit)
        )
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(statement)
                rows = result.all()
        except SQLAlchemyError:
            logger.exception("Could not count fact units")
            raise
        counts: Dict[int, Dict[str, int]] = {}
        for company_id, unit, count in rows:
            counts.setdefault(int(company_id), {})[str(unit)] = int(count)
        return counts

    async def period_ends(self, company_ids: Sequence[int]) -> Dict[int, List[date]]:
        """Return the distinct fact period ends for each company.

        Args:
            company_ids: Companies whose statement dates need a rate.

        Returns:
            Company id to its period ends. A company with no dated facts is
            absent.
        """
        if not company_ids:
            return {}
        facts = self.financial_facts_table
        statement = (
            select(facts.c.company_id, facts.c.period_end)
            .where(
                facts.c.company_id.in_(list(company_ids)),
                facts.c.period_end.is_not(None),
            )
            .group_by(facts.c.company_id, facts.c.period_end)
        )
        try:
            async with self.engine.connect() as conn:
                result = await conn.execute(statement)
                rows = result.all()
        except SQLAlchemyError:
            logger.exception("Could not read fact period ends")
            raise
        ends: Dict[int, List[date]] = {}
        for company_id, period_end in rows:
            ends.setdefault(int(company_id), []).append(period_end)
        return ends

    async def upsert(self, rows: Iterable[Tuple[str, date, Decimal]]) -> int:
        """Insert rates, replacing a date that is already stored.

        Args:
            rows: ``(currency, rate_date, units_per_usd)`` observations.

        Returns:
            Number of rows written.
        """
        payload = [
            {
                "currency": currency,
                "rate_date": rate_date,
                "units_per_usd": units_per_usd,
            }
            for currency, rate_date, units_per_usd in rows
        ]
        if not payload:
            return 0
        try:
            async with self.engine.begin() as conn:
                for start in range(0, len(payload), _UPSERT_BATCH):
                    batch = payload[start : start + _UPSERT_BATCH]
                    statement = insert(self.rates_table).values(batch)
                    statement = statement.on_conflict_do_update(
                        index_elements=["currency", "rate_date"],
                        set_={"units_per_usd": statement.excluded.units_per_usd},
                    )
                    await conn.execute(statement)
        except SQLAlchemyError:
            logger.exception("Could not store USD exchange rates")
            raise
        return len(payload)
