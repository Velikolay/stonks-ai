"""Loaders for downloading and persisting SEC XBRL filings."""

import logging
from collections import Counter
from datetime import date, datetime
from typing import Any, Callable, Optional, Sequence, Tuple

from dotenv import load_dotenv
from edgar import Company

from filings.company_ids import locked_company_id
from filings.db import AsyncFilingsDatabase
from filings.fiscal_periods import FilingPeriod, corrected_periods
from filings.models.company import CompanyCreate, CompanyUpdate
from filings.models.filing import FilingCreate
from filings.parsers.sec_xbrl import SECXBRLParser
from filings.parsers.sec_xbrl.values import as_date

logger = logging.getLogger(__name__)

load_dotenv()

# A 52/53-week filer can put the calendar month-end on the submission header
# and the last Saturday in XBRL. Six days covers that gap when choosing the
# filing's own period. Prior-period columns are stored as their own facts.
_PERIOD_MATCH_DAYS = 6
# 20-F is the annual report of a foreign private issuer.
_PERIODIC_FORMS = ("10-K", "10-Q", "20-F")


def _cik_number(value: Any) -> Optional[int]:
    """Return a CIK as an int, ignoring blank and non-numeric values."""
    if value is None or isinstance(value, bool):
        return None
    text = str(value).strip()
    if not text.isdigit():
        return None
    return int(text)


def _report_period(filing: Any) -> Optional[date]:
    """Return the period a filing reports, when the filing exposes one."""
    for attr in ("report_date", "period_of_report"):
        value = getattr(filing, attr, None)
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str) and value:
            return date.fromisoformat(value[:10])
    return None


def _accession(filing: Any) -> str:
    """Return the filing's accession number."""
    return str(
        getattr(filing, "accession_number", None)
        or getattr(filing, "accession_no", "")
        or ""
    )


def _filer_ciks(filing: Any) -> Tuple[int, ...]:
    """Return the CIKs named as filers on one submission."""
    raw = getattr(filing, "all_ciks", None)
    if not isinstance(raw, (list, tuple)):
        return ()
    ciks = []
    for item in raw:
        cik = _cik_number(item)
        if cik is not None:
            ciks.append(cik)
    return tuple(ciks)


def _periodic_filings(company: Any) -> list:
    """Return the company's annual and quarterly filings, including amendments."""
    filings = company.get_filings(form=list(_PERIODIC_FORMS))
    if not filings:
        return []
    return list(filings)


def earlier_registrant_ciks(
    company: Any, resolve: Optional[Callable[[int], Any]] = None
) -> Tuple[str, ...]:
    """Return CIKs this company filed under before its current registrant.

    The earliest annual report (10-K or 20-F) lists every filer on that
    submission. Another filer is the previous registrant when its own periodic
    reports end at or before this company's first period. A co-registrant that
    keeps filing its own reports is left out. The earliest 10-Q is checked
    when no annual report names an earlier filer.

    Args:
        company: EDGAR company for the current registrant.
        resolve: Builds the EDGAR company for a candidate CIK.

    Returns:
        CIK strings, without leading zeros, in discovery order.
    """
    lookup = resolve or Company
    found: list[str] = []
    seen = set()
    pending = [company]
    while pending:
        current = pending.pop(0)
        current_cik = _cik_number(getattr(current, "cik", None))
        if current_cik is None or current_cik in seen:
            continue
        seen.add(current_cik)
        filings = _periodic_filings(current)
        if not filings:
            continue
        periods = [period for period in map(_report_period, filings) if period]
        if not periods:
            continue
        first_period = min(periods)
        accessions = {_accession(filing) for filing in filings}
        handoff = _earliest_handoff(filings)
        if handoff is None:
            continue
        for other_cik in _filer_ciks(handoff):
            if other_cik in seen:
                continue
            other = lookup(other_cik)
            other_filings = _periodic_filings(other)
            own_filings = [
                filing
                for filing in other_filings
                if _accession(filing) not in accessions
            ]
            own_periods = [
                period for period in map(_report_period, own_filings) if period
            ]
            if not own_periods or max(own_periods) > first_period:
                continue
            found.append(str(other_cik))
            pending.append(other)
    return tuple(found)


def _earliest_handoff(filings: Sequence[Any]) -> Any:
    """Return the earliest annual report, or the earliest 10-Q when there is none.

    A 20-F is the annual report of a foreign private issuer, so it is the
    handoff when the company files no 10-K.
    """
    annual = [
        filing
        for filing in filings
        if str(getattr(filing, "form", "")).startswith(("10-K", "20-F"))
    ]
    chosen = annual or [
        filing
        for filing in filings
        if str(getattr(filing, "form", "")).startswith("10-Q")
    ]
    if not chosen:
        return None
    return min(chosen, key=lambda filing: filing.filing_date)


class SECXBRLFilingsLoader:
    """Loader for downloading and persisting SEC XBRL filings."""

    def __init__(
        self, database: AsyncFilingsDatabase, parser: Optional[SECXBRLParser] = None
    ):
        """Initialize the XBRL filings loader.

        Args:
            database: AsyncFilingsDatabase instance to use for database operations
            parser: Parser instance for extracting financial facts from filings
        """
        self.database = database
        self.parser = parser or SECXBRLParser()

    async def load_company_filings(
        self,
        ticker: str,
        form: str = "10-Q",
        limit: int = 5,
        override: bool = False,
        status: Optional[str] = None,
    ) -> dict:
        """Download and persist filings for a company.

        Args:
            ticker: Company ticker symbol
            form: Form type (e.g., "10-Q", "10-K")
            limit: Maximum number of filings to load
            override: If True, replace existing filing data

        Returns:
            Dictionary with loading results
        """
        try:
            logger.info(
                f"Loading {form} filings for {ticker} (limit: {limit}, override: {override})"
            )

            edgar_company = Company(ticker)

            # Get or create company
            company = await self._get_or_create_company(edgar_company)
            if not company:
                return {"error": f"Failed to get or create company for ticker {ticker}"}

            cik = getattr(edgar_company, "cik", None)
            if cik is None:
                return {
                    "error": f"Failed to determine CIK for ticker {ticker} (required for filing_entities)"
                }
            filing_entity_id = (
                await self.database.companies.get_or_create_filing_entities_id(
                    company_id=company.id,
                    registry="SEC",
                    number=str(cik),
                    status="active",
                )
            )
            if filing_entity_id is None:
                return {
                    "error": f"Failed to get or create filing_entities for ticker {ticker}"
                }
            for number in earlier_registrant_ciks(edgar_company):
                logger.info("Attaching earlier registrant SEC:%s to %s", number, ticker)
                filing_entity_id = (
                    await self.database.companies.get_or_create_filing_entities_id(
                        company_id=company.id,
                        registry="SEC",
                        number=number,
                        status="active",
                    )
                )
                if filing_entity_id is None:
                    return {"error": f"Failed to attach SEC:{number} to {ticker}"}
            filing_entities = (
                await self.database.companies.get_filing_entities_by_company_id(
                    company_id=company.id, registry="SEC", status=status
                )
            )
            if not filing_entities:
                filing_entities = []

            total_loaded = 0
            total_updated = 0
            total_facts = 0

            # For each filing entity (e.g. SEC + CIK), load filings via edgar and persist them
            for filing_entity in filing_entities:
                edgar_company_for_entity = Company(filing_entity.number)
                filings = edgar_company_for_entity.get_filings(form=form, is_xbrl=True)
                if not filings:
                    continue

                loaded_for_entity = 0
                for filing in filings:
                    if loaded_for_entity >= limit:
                        break

                    try:
                        facts_count, was_updated = await self._load_single_filing(
                            filing, company.id, filing_entity.id, override
                        )
                        if facts_count <= 0:
                            continue

                        loaded_for_entity += 1
                        total_loaded += 1
                        total_facts += facts_count
                        if was_updated:
                            total_updated += 1

                        logger.info(
                            f"Loaded filing {filing.accession_number} with {facts_count} facts"
                            + (" (updated)" if was_updated else " (new)")
                        )
                    except Exception as e:
                        logger.error(
                            f"Error loading filing {getattr(filing, 'accession_number', '<unknown>')}: {e}"
                        )
                        continue

            corrected = await self._correct_fiscal_periods(company.id)

            if total_loaded == 0:
                logger.info(f"No {form} filings found for {ticker}")
                return {
                    "message": f"No {form} filings found for {ticker}",
                    "company_id": company.id,
                    "periods_corrected": corrected,
                }

            return {
                "ticker": ticker,
                "form": form,
                "filings_loaded": total_loaded,
                "filings_updated": total_updated,
                "total_facts": total_facts,
                "company_id": company.id,
                "override_mode": override,
                "periods_corrected": corrected,
            }

        except Exception as e:
            logger.error(f"Error loading filings for {ticker}: {e}")
            return {"error": f"Failed to load filings for {ticker}: {str(e)}"}

    async def _correct_fiscal_periods(self, company_id: int) -> int:
        """Correct stored fiscal periods a filing cover got wrong.

        Runs over all of the company's filings, so a filing stored by an
        earlier run is corrected too. See :mod:`filings.fiscal_periods`.
        """
        try:
            stored = await self.database.filings.get_filings_by_company(company_id)
            changes = corrected_periods(
                FilingPeriod(
                    id=f.id,
                    form_type=f.form_type,
                    period_end=f.fiscal_period_end,
                    fiscal_year=f.fiscal_year,
                    fiscal_quarter=f.fiscal_quarter,
                )
                for f in stored
            )
            by_id = {f.id: f for f in stored}
            for filing_id, (year, quarter) in sorted(changes.items()):
                f = by_id[filing_id]
                logger.info(
                    "Filing %s (%s, %s): fiscal period %s Q%s corrected to %s Q%s",
                    f.number,
                    f.form_type,
                    f.fiscal_period_end,
                    f.fiscal_year,
                    f.fiscal_quarter,
                    year,
                    quarter,
                )
            return await self.database.filings.update_fiscal_periods(changes)
        except Exception:
            logger.exception(
                "Could not check fiscal periods for company %s", company_id
            )
            return 0

    async def _get_or_create_company(self, edgar_company: Company):
        """Get existing company or create new one from the edgar Company object.

        Args:
            edgar_company: edgar Company instance

        Returns:
            Company object or None if failed
        """
        try:
            # Pull canonical metadata from edgar (may require EDGAR identity to be set)
            try:
                tickers = list(edgar_company.tickers) or []
            except Exception as e:
                logger.error(f"Failed to read tickers from edgar Company: {e}")
                tickers = []

            try:
                exchanges = list(edgar_company.get_exchanges()) or []
            except Exception:
                exchanges = []

            try:
                company_name = edgar_company.name or None
            except Exception:
                company_name = None

            if not tickers:
                logger.error(
                    "edgar Company returned no tickers; cannot create ticker mappings"
                )
                return None

            exchange = exchanges[0] if exchanges else "UNKNOWN"
            if exchanges and len(exchanges) > 1:
                logger.warning(
                    "Multiple exchanges returned by edgar (%s); using %s",
                    exchanges,
                    exchange,
                )

            # A locked ticker always uses its pinned id, including when the
            # ticker row is missing or stored under a different exchange.
            pinned_id = locked_company_id(tickers)
            if pinned_id is not None:
                return await self._use_locked_company(
                    company_id=pinned_id,
                    tickers=tickers,
                    exchange=exchange,
                    name=company_name or tickers[0],
                    industry=edgar_company.data.sic_description,
                )

            # 1) Try to resolve existing company by any returned ticker
            for ticker in tickers:
                existing = await self.database.companies.get_company_by_ticker(
                    ticker, exchange
                )
                if existing:
                    logger.info(
                        "Found existing company %s via edgar tickers %s",
                        existing.name,
                        tickers,
                    )
                    return existing

            # 2) Create new company, then attach all tickers to it
            name_for_create = company_name or tickers[0]
            industry_for_create = edgar_company.data.sic_description
            company_id = await self.database.companies.insert_company(
                CompanyCreate(name=name_for_create, industry=industry_for_create)
            )
            if not company_id:
                logger.error("Failed to insert company for edgar tickers %s", tickers)
                return None

            company = await self.database.companies.get_company_by_id(company_id)
            if not company:
                logger.error(
                    "Inserted company id=%s but could not reload it; tickers=%s",
                    company_id,
                    tickers,
                )
                return None

            for ticker in tickers:
                ok = await self.database.companies.upsert_ticker(
                    company_id=company.id,
                    ticker=ticker,
                    exchange=exchange,
                    status="active",
                )
                if not ok:
                    logger.warning(
                        "Failed to upsert ticker mapping %s (%s) for company_id=%s",
                        ticker,
                        exchange,
                        company.id,
                    )

            logger.info(
                "Created new company %s (id=%s) for edgar tickers %s",
                company.name,
                company.id,
                tickers,
            )
            return company

        except Exception as e:
            logger.error(f"Error getting/creating company from edgar Company: {e}")
            return None

    async def _use_locked_company(
        self,
        company_id: int,
        tickers: list[str],
        exchange: str,
        name: str,
        industry: Optional[str],
    ):
        """Return the pinned company, creating it at ``company_id`` when absent.

        Args:
            company_id: Id from the company-id lock file.
            tickers: Tickers to attach to that company.
            exchange: Exchange used for the ticker rows.
            name: Company name from EDGAR.
            industry: Industry from EDGAR.

        Returns:
            The company row, or None when the insert fails.
        """
        company = await self.database.companies.get_company_by_id(company_id)
        if company is None:
            inserted_id = await self.database.companies.insert_company(
                CompanyCreate(name=name, industry=industry),
                company_id=company_id,
            )
            if not inserted_id:
                logger.error(
                    "Failed to insert locked company id=%s for tickers %s",
                    company_id,
                    tickers,
                )
                return None
            company = await self.database.companies.get_company_by_id(company_id)
            if company is None:
                logger.error(
                    "Inserted locked company id=%s but could not reload it",
                    company_id,
                )
                return None
        else:
            updated = await self.database.companies.update_company(
                company_id=company_id,
                company=CompanyUpdate(name=name, industry=industry),
            )
            if updated is not None:
                company = updated

        for ticker in tickers:
            ok = await self.database.companies.upsert_ticker(
                company_id=company.id,
                ticker=ticker,
                exchange=exchange,
                status="active",
            )
            if not ok:
                logger.warning(
                    "Failed to upsert ticker mapping %s (%s) for company_id=%s",
                    ticker,
                    exchange,
                    company.id,
                )

        logger.info(
            "Using locked company %s (id=%s) for edgar tickers %s",
            company.name,
            company.id,
            tickers,
        )
        return company

    def _reporting_period_end(
        self,
        facts: Sequence,
        cover_period_end: Optional[date],
    ) -> Optional[date]:
        """Return the statement period that belongs to this filing.

        Facts dated within :data:`_PERIOD_MATCH_DAYS` of the cover date are
        the current period. When the cover date itself has no facts, the
        most common nearby statement date is used instead.

        Args:
            facts: Facts parsed from the filing, including comparative columns.
            cover_period_end: ``period_of_report`` from the submission header.

        Returns:
            The period end to store on the filing. Prior-period facts are kept.
        """
        if cover_period_end is None:
            return None

        near = [
            fact.period_end
            for fact in facts
            if getattr(fact, "period_end", None) is not None
            and abs((fact.period_end - cover_period_end).days) <= _PERIOD_MATCH_DAYS
        ]
        if not near:
            return cover_period_end

        counts = Counter(near)
        if counts[cover_period_end] == max(counts.values()):
            return cover_period_end
        return counts.most_common(1)[0][0]

    def _fiscal_period(
        self,
        period_end: Optional[date],
        focus: tuple[Optional[int], Optional[int]] = (None, None),
    ) -> tuple[Optional[int], Optional[int]]:
        """Return the filing's fiscal year and quarter.

        The cover page declares both. When either fact is missing, the year is
        the calendar year of the period end and the quarter is the calendar
        month bucket.

        Args:
            period_end: Statement period end stored on the filing.
            focus: Fiscal year and quarter the cover declares.

        Returns:
            Fiscal year and quarter, or None when the period end is missing
            and the cover did not declare them.
        """
        year, quarter = focus
        if isinstance(year, int) and isinstance(quarter, int):
            return year, quarter
        if period_end is None:
            return None, None
        return period_end.year, self._calculate_fiscal_quarter(period_end.isoformat())

    def _calculate_fiscal_quarter(
        self, fiscal_period_end: Optional[str]
    ) -> Optional[int]:
        """Calculate fiscal quarter from fiscal period end date.

        Args:
            fiscal_period_end: Fiscal period end date string (YYYY-MM-DD format)

        Returns:
            Fiscal quarter (1-4) or None if date cannot be parsed
        """
        if not fiscal_period_end:
            return None

        parsed_date = self._parse_date(fiscal_period_end)
        if not parsed_date:
            return None

        # Calculate quarter based on month
        month = parsed_date.month
        if month in [3, 4, 5]:
            return 1
        elif month in [6, 7, 8]:
            return 2
        elif month in [9, 10, 11]:
            return 3
        elif month in [12, 1, 2]:
            return 4
        else:
            logger.warning(f"Invalid month {month} for fiscal quarter calculation")
            return None

    async def _load_single_filing(
        self, filing, company_id: int, filing_entity_id: int, override: bool
    ) -> tuple[int, bool]:
        """Load a single filing and persist its data.

        Args:
            filing: edgartools Filing object
            company_id: Database company ID
            override: If True, replace existing filing data. If False, skip.

        Returns:
            Tuple of (number of facts extracted and persisted, was updated)
        """
        try:
            # Check if filing already exists
            existing_filing = await self.database.filings.get_filing_by_number(
                "SEC", filing.accession_number
            )

            if existing_filing and not override:
                logger.info(
                    f"Filing {filing.accession_number} already exists and override is False, skipping"
                )
                return 0, False

            # If filing exists and override is True, delete existing data
            if existing_filing and override:
                logger.info(f"Overriding existing filing {filing.accession_number}")
                # Delete existing financial facts first (due to foreign key constraint)
                await self.database.financial_facts.delete_facts_by_filing_id(
                    existing_filing.id
                )
                # Delete the filing
                await self.database.filings.delete_filing(existing_filing.id)
                logger.info(
                    f"Deleted existing filing {filing.accession_number} and its facts"
                )

            # Parse before insert so the filing period follows the statements
            # when the cover date and the XBRL date disagree by a few days.
            parsed = self.parser.parse_filing(filing)
            facts = parsed.facts
            cover_period_end = self._parse_date(filing.period_of_report)
            reporting_period_end = self._reporting_period_end(facts, cover_period_end)
            if (
                reporting_period_end is not None
                and reporting_period_end != cover_period_end
            ):
                logger.warning(
                    "Filing %s cover period %s; using statement period %s",
                    filing.accession_number,
                    cover_period_end,
                    reporting_period_end,
                )

            fiscal_year, fiscal_quarter = self._fiscal_period(
                reporting_period_end, parsed.fiscal_focus
            )

            # Create filing record
            filing_data = FilingCreate(
                company_id=company_id,
                filing_entity_id=filing_entity_id,
                registry="SEC",
                number=filing.accession_number,
                form_type=filing.form,
                filing_date=self._parse_date(filing.filing_date),
                fiscal_period_end=reporting_period_end,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                public_url=filing.url,
                is_shell=parsed.is_shell,
            )
            if parsed.is_shell:
                logger.info(
                    "Filing %s declares a shell company; the refresh skips it",
                    filing.accession_number,
                )

            filing_id = await self.database.filings.insert_filing(filing_data)

            if not filing_id:
                logger.error(f"Failed to insert filing {filing.accession_number}")
                return 0, False

            # A 10-K prints earlier years and a 10-Q prints the prior-year
            # quarter, so dimension rows keep those columns as facts for their
            # own period. A face row off the filing period is an earlier column
            # the parser fell back to; that year's own filing reports it.
            period_end = filing_data.fiscal_period_end
            valid_facts = [
                fact
                for fact in facts
                if fact.period_end is not None
                and (
                    fact.period_end == period_end
                    or (fact.axis and period_end and fact.period_end < period_end)
                )
            ]

            if not valid_facts:
                logger.warning(
                    f"No facts extracted from filing {filing.accession_number}"
                )
                return 0, False

            if len(valid_facts) != len(facts):
                logger.warning(
                    "Skipped %s facts with no period end for filing %s",
                    len(facts) - len(valid_facts),
                    filing.accession_number,
                )

            # Set filing_id for all valid facts
            for fact in valid_facts:
                fact.company_id = company_id
                fact.filing_id = filing_id
                fact.form_type = filing_data.form_type

            # Insert facts in batch
            inserted_facts = (
                await self.database.financial_facts.insert_financial_facts_batch(
                    valid_facts
                )
            )

            if inserted_facts:
                was_updated = existing_filing is not None and override
                logger.info(
                    f"Inserted {len(inserted_facts)} facts for filing {filing.accession_number}"
                    + (" (updated)" if was_updated else " (new)")
                )
                return len(inserted_facts), was_updated
            else:
                logger.error(
                    f"Failed to insert facts for filing {filing.accession_number}"
                )
                return 0, False

        except Exception as e:
            logger.error(f"Error loading filing {filing.accession_number}: {e}")
            return 0, False

    def _parse_date(self, value: object) -> Optional[date]:
        """A filing date field as a date, or None when missing or malformed."""
        return as_date(value)
