"""Loaders for downloading and persisting SEC filings."""

import logging
from datetime import date
from typing import Optional

from dotenv import load_dotenv
from edgar import Company

from filings import FilingsDatabase
from filings.models.company import CompanyCreate
from filings.models.filing import FilingCreate
from filings.parsers.sec_10q import SEC10QParser

logger = logging.getLogger(__name__)

load_dotenv()


class FilingsLoader:
    """Loader for downloading and persisting SEC filings."""

    def __init__(self, database: FilingsDatabase, parser: SEC10QParser = None):
        """Initialize the filings loader.

        Args:
            database: FilingsDatabase instance to use for database operations
            parser: Parser instance for extracting financial facts from filings
        """
        self.database = database
        self.parser = parser or SEC10QParser()

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

            # Get or create company
            company = self._get_or_create_company(ticker)
            if not company:
                return {"error": f"Failed to get or create company for ticker {ticker}"}

            # Get company filings
            edgar_company = Company(ticker)
            filings = edgar_company.get_filings(form=form)

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
                        filing, company.id, override
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

    def _get_or_create_company(self, ticker: str):
        """Get existing company or create new one.

        Args:
            ticker: Company ticker symbol

        Returns:
            Company object or None if failed
        """
        try:
            # Try to get existing company
            company = self.database.companies.get_company_by_ticker(ticker)
            if company:
                logger.info(f"Found existing company: {company.name} ({ticker})")
                return company

            # Create new company
            logger.info(f"Creating new company for ticker: {ticker}")
            company_data = CompanyCreate(
                ticker=ticker,
                exchange="NASDAQ",  # Default exchange
                name=ticker,  # Could be enhanced to fetch actual company name
                cik=None,  # Could be enhanced to fetch CIK
                sector=None,
                industry=None,
            )

            company = self.database.companies.get_or_create_company(company_data)
            if company:
                logger.info(f"Created new company: {company.name} ({ticker})")
                return company
            else:
                logger.error(f"Failed to create company for ticker {ticker}")
                return None

        except Exception as e:
            logger.error(f"Error getting/creating company for {ticker}: {e}")
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
        if month in [1, 2, 3]:
            return 1
        elif month in [4, 5, 6]:
            return 2
        elif month in [7, 8, 9]:
            return 3
        elif month in [10, 11, 12]:
            return 4
        else:
            logger.warning(f"Invalid month {month} for fiscal quarter calculation")
            return None

    def _load_single_filing(
        self, filing, company_id: int, override: bool
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
                source="SEC",
                filing_number=filing.accession_number,
                form_type=filing.form,
                filing_date=filing.filing_date,
                fiscal_period_end=filing.period_of_report,
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
            if not facts:
                logger.warning(
                    f"No facts extracted from filing {filing.accession_number}"
                )
                return 0, False

            # Set filing_id for all facts
            for fact in facts:
                fact.filing_id = filing_id

            # Insert facts in batch
            inserted_facts = self.database.financial_facts.insert_financial_facts_batch(
                facts
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
