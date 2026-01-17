"""Loaders for downloading and persisting SEC XBRL filings."""

import logging
from datetime import date
from typing import Optional

from dotenv import load_dotenv
from edgar import Company

from filings import FilingsDatabase
from filings.models.company import CompanyCreate
from filings.models.filing import FilingCreate
from filings.parsers.geography import GeographyParser
from filings.parsers.product import ProductParser
from filings.parsers.sec_xbrl import SECXBRLParser

logger = logging.getLogger(__name__)

load_dotenv()


class SECXBRLFilingsLoader:
    """Loader for downloading and persisting SEC XBRL filings."""

    def __init__(self, database: FilingsDatabase, parser: SECXBRLParser = None):
        """Initialize the XBRL filings loader.

        Args:
            database: FilingsDatabase instance to use for database operations
            parser: Parser instance for extracting financial facts from filings
        """
        self.database = database
        self.parser = parser or SECXBRLParser(GeographyParser(), ProductParser())

    def load_company_filings(
        self, ticker: str, form: str = "10-Q", limit: int = 5, override: bool = False
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

            # Resolve edgar company once (used for both ticker metadata and filings)
            edgar_company = Company(ticker)

            # Get or create company
            company = self._get_or_create_company(edgar_company)
            if not company:
                return {"error": f"Failed to get or create company for ticker {ticker}"}

            cik = getattr(edgar_company, "cik", None)
            if cik is None:
                return {
                    "error": f"Failed to determine CIK for ticker {ticker} (required for filing_registry)"
                }
            registry_id = self.database.companies.get_or_create_filing_registry_id(
                company_id=company.id,
                registry="SEC",
                number=str(cik),
                status="active",
            )
            if registry_id is None:
                return {
                    "error": f"Failed to get or create filing_registry for ticker {ticker}"
                }

            # Get company filings
            filings = edgar_company.get_filings(
                form=form,
                is_xbrl=True,
            )

            if not filings:
                logger.info(f"No {form} filings found for {ticker}")
                return {"message": f"No {form} filings found for {ticker}"}

            # Load filings up to limit
            loaded_count = 0
            total_facts = 0
            updated_count = 0

            for filing in filings:
                if loaded_count >= limit:
                    break

                try:
                    facts_count, was_updated = self._load_single_filing(
                        filing, company.id, registry_id, override
                    )
                    if facts_count > 0:
                        loaded_count += 1
                        total_facts += facts_count
                        if was_updated:
                            updated_count += 1
                        logger.info(
                            f"Loaded filing {filing.accession_number} with {facts_count} facts"
                            + (" (updated)" if was_updated else " (new)")
                        )
                    else:
                        logger.warning(
                            f"No facts extracted from filing {filing.accession_number}"
                        )

                except Exception as e:
                    logger.error(f"Error loading filing {filing.accession_number}: {e}")
                    continue

            return {
                "ticker": ticker,
                "form": form,
                "filings_loaded": loaded_count,
                "filings_updated": updated_count,
                "total_facts": total_facts,
                "company_id": company.id,
                "override_mode": override,
            }

        except Exception as e:
            logger.error(f"Error loading filings for {ticker}: {e}")
            return {"error": f"Failed to load filings for {ticker}: {str(e)}"}

    def _get_or_create_company(self, edgar_company: Company):
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

            # 1) Try to resolve existing company by any returned ticker
            for ticker in tickers:
                existing = self.database.companies.get_company_by_ticker(
                    ticker, exchange
                )
                if existing:
                    logger.info(
                        "Found existing company %s via edgar tickers %s",
                        existing.name,
                        tickers,
                    )
                    return existing

            # 2) Create new company, then attach all tickers to it (explicit create flow)
            name_for_create = company_name or tickers[0]
            industry_for_create = edgar_company.data.sic_description
            company_id = self.database.companies.insert_company(
                CompanyCreate(name=name_for_create, industry=industry_for_create)
            )
            if not company_id:
                logger.error("Failed to insert company for edgar tickers %s", tickers)
                return None

            company = self.database.companies.get_company_by_id(company_id)
            if not company:
                logger.error(
                    "Inserted company id=%s but could not reload it; tickers=%s",
                    company_id,
                    tickers,
                )
                return None

            for ticker in tickers:
                ok = self.database.companies.upsert_ticker(
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

    def _load_single_filing(
        self, filing, company_id: int, registry_id: int, override: bool
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
            existing_filing = self.database.filings.get_filing_by_number(
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
                self.database.financial_facts.delete_facts_by_filing_id(
                    existing_filing.id
                )
                # Delete the filing
                self.database.filings.delete_filing(existing_filing.id)
                logger.info(
                    f"Deleted existing filing {filing.accession_number} and its facts"
                )

            # Calculate fiscal quarter from period end date
            fiscal_quarter = self._calculate_fiscal_quarter(filing.period_of_report)

            # Create filing record
            filing_data = FilingCreate(
                company_id=company_id,
                registry_id=registry_id,
                registry="SEC",
                number=filing.accession_number,
                form_type=filing.form,
                filing_date=self._parse_date(filing.filing_date),
                fiscal_period_end=self._parse_date(filing.period_of_report),
                fiscal_year=(
                    self._parse_date(filing.period_of_report).year
                    if filing.period_of_report
                    else None
                ),
                fiscal_quarter=fiscal_quarter,
                public_url=filing.url,
            )

            filing_id = self.database.filings.insert_filing(filing_data)

            if not filing_id:
                logger.error(f"Failed to insert filing {filing.accession_number}")
                return 0, False

            # Parse financial facts
            facts = self.parser.parse_filing(filing)

            # Filter invalid facts
            valid_facts = [
                fact
                for fact in facts
                if fact.period_end == filing_data.fiscal_period_end
            ]

            if not valid_facts:
                logger.warning(
                    f"No facts extracted from filing {filing.accession_number}"
                )
                return 0, False

            if len(valid_facts) != len(facts):
                logger.warning(
                    f"Skipped {len(facts) - len(valid_facts)} facts due to period end mismatch for filing {filing.accession_number} and period end {filing.period_of_report}"
                )

            # Set filing_id for all valid facts
            for fact in valid_facts:
                fact.company_id = company_id
                fact.filing_id = filing_id
                fact.form_type = filing_data.form_type

            # Insert facts in batch
            inserted_facts = self.database.financial_facts.insert_financial_facts_batch(
                valid_facts
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

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object.

        Args:
            date_str: Date string to parse

        Returns:
            Date object or None if parsing fails
        """
        if not date_str:
            return None

        try:
            from datetime import datetime

            # Try parsing ISO format first
            if isinstance(date_str, str):
                return datetime.strptime(date_str, "%Y-%m-%d").date()
            return date_str
        except (ValueError, TypeError):
            logger.warning(f"Could not parse date: {date_str}")
            return None
