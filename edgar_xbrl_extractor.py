"""
Minimal EDGAR XBRL extractor using sec-edgar-downloader and python-xbrl.

This module provides a simple way to download 10-Q filings and extract
financial data for a single ticker, with optional database storage.
"""

import logging
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from sec_edgar_downloader import Downloader
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from xbrl import GAAPSerializer, XBRLParser

from models import Base, Company, Filing, FinancialFact

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MinimalEdgarExtractor:
    """
    Minimal EDGAR XBRL extractor for downloading and parsing 10-Q data.

    Uses sec-edgar-downloader to download filings and python-xbrl to parse them.
    Supports both CSV export and database storage.
    """

    def __init__(
        self,
        download_path: str = "edgar_downloads",
        company_name: str = "Company Name",
        email_address: str = "admin@company.com",
        database_url: Optional[str] = None,
    ):
        """
        Initialize the extractor.

        Args:
            download_path: Directory to store downloaded filings
            company_name: Company name for SEC requests
            email_address: Email address for SEC requests
            database_url: Optional database URL for storing data
        """
        self.download_path = Path(download_path)
        self.download_path.mkdir(exist_ok=True)

        # Initialize the downloader with required parameters
        self.downloader = Downloader(
            company_name=company_name,
            email_address=email_address,
            download_folder=str(self.download_path),
        )

        # Initialize XBRL parser
        self.xbrl_parser = XBRLParser()

        # Initialize database connection if URL provided
        self.database_url = database_url
        self.engine = None
        self.SessionLocal = None

        if database_url:
            self.engine = create_engine(database_url)
            self.SessionLocal = sessionmaker(
                autocommit=False, autoflush=False, bind=self.engine
            )
            # Create tables
            Base.metadata.create_all(bind=self.engine)
            logger.info("Database connection initialized")

    def get_or_create_company(
        self, ticker: str, name: str, exchange: str = "NASDAQ", session=None
    ) -> Optional[Company]:
        """
        Get or create a company record.

        Args:
            ticker: Company ticker symbol
            name: Company name
            exchange: Stock exchange
            session: SQLAlchemy session (optional)

        Returns:
            Company object or None if failed
        """
        try:
            # Use provided session or create new one
            if session is None:
                session = self.SessionLocal()
                should_close = True
            else:
                should_close = False

            # Check if company already exists
            company = session.query(Company).filter_by(ticker=ticker).first()

            if not company:
                # Create new company
                company = Company(ticker=ticker, name=name, exchange=exchange)
                session.add(company)
                session.flush()  # Get the ID without committing
                logger.info(f"Created new company: {ticker} - {name}")
            else:
                logger.info(f"Found existing company: {ticker} - {name}")

            if should_close:
                session.close()

            return company

        except Exception as e:
            logger.error(f"Error getting or creating company: {e}")
            if session and should_close:
                session.close()
            return None

    def create_filing_record(
        self,
        company_id: int,
        source: str,
        filing_number: str,
        form_type: str,
        filing_date: datetime,
        fiscal_period_end: datetime,
        fiscal_year: int,
        fiscal_quarter: int,
        public_url: Optional[str] = None,
        session=None,
    ) -> Optional[Filing]:
        """
        Create a filing record in the database.

        Args:
            company_id: Company ID
            source: Data source (e.g., "SEC")
            filing_number: Unique filing number
            form_type: Form type (e.g., "10-Q")
            filing_date: Filing date
            fiscal_period_end: Fiscal period end date
            fiscal_year: Fiscal year
            fiscal_quarter: Fiscal quarter
            public_url: Public URL to filing
            session: SQLAlchemy session (optional)

        Returns:
            Filing object or None if failed
        """
        try:
            # Use provided session or create new one
            if session is None:
                session = self.SessionLocal()
                should_close = True
            else:
                should_close = False

            # Check if filing already exists
            existing_filing = (
                session.query(Filing)
                .filter_by(source=source, filing_number=filing_number)
                .first()
            )

            if existing_filing:
                logger.info(f"Filing already exists: {filing_number}")
                if should_close:
                    session.close()
                return existing_filing

            # Create new filing
            filing = Filing(
                company_id=company_id,
                source=source,
                filing_number=filing_number,
                form_type=form_type,
                filing_date=filing_date,
                fiscal_period_end=fiscal_period_end,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                public_url=public_url,
            )

            session.add(filing)
            session.flush()  # Get the ID without committing
            logger.info(f"Created new filing: {filing_number}")

            if should_close:
                session.close()

            return filing

        except Exception as e:
            logger.error(f"Error creating filing record: {e}")
            if session and should_close:
                session.close()
            return None

    def create_financial_facts(
        self, filing_id: int, xbrl_data: Dict[str, Any], session=None
    ) -> List[FinancialFact]:
        """
        Create financial facts from XBRL data.

        Args:
            filing_id: Filing ID
            xbrl_data: Parsed XBRL data
            session: SQLAlchemy session (optional)

        Returns:
            List of FinancialFact objects
        """
        facts = []
        try:
            # Use provided session or create new one
            if session is None:
                session = self.SessionLocal()
                should_close = True
            else:
                should_close = False

            for key, value in xbrl_data.items():
                try:
                    # Skip non-dict values or empty values
                    if not isinstance(value, dict) or not value.get("value"):
                        continue

                    # Get the raw value
                    raw_value = value.get("value")

                    # Convert value to numeric, handling various formats
                    numeric_value = None
                    if raw_value:
                        # Remove common non-numeric characters
                        cleaned_value = (
                            str(raw_value).strip().replace(",", "").replace("$", "")
                        )

                        # Handle boolean values
                        if cleaned_value.lower() in ["true", "false", "yes", "no"]:
                            continue  # Skip boolean values

                        # Handle empty or non-numeric values
                        if cleaned_value in ["", "null", "none", "n/a", "-"]:
                            continue

                        try:
                            # Try to convert to numeric
                            numeric_value = Decimal(cleaned_value)
                        except (ValueError, TypeError, InvalidOperation):
                            # If conversion fails, skip this fact
                            logger.debug(
                                f"Skipping non-numeric value: {raw_value} for key: {key}"
                            )
                            continue

                    # Create financial fact
                    fact = FinancialFact(
                        filing_id=filing_id,
                        taxonomy="US-GAAP",
                        tag=value.get("element_name", key),
                        value=numeric_value,
                        unit=value.get("unit"),
                        section=value.get("context"),
                        start_date=None,  # Could be extracted from context
                        end_date=None,  # Could be extracted from context
                    )

                    session.add(fact)
                    facts.append(fact)

                except Exception as e:
                    logger.warning(f"Error creating financial fact for {key}: {e}")
                    continue

            if should_close:
                session.close()

            logger.info(f"Created {len(facts)} financial facts")
            return facts

        except Exception as e:
            logger.error(f"Error creating financial facts: {e}")
            if session and should_close:
                session.close()
            return []

    def store_in_database(
        self,
        ticker: str,
        company_name: str,
        xbrl_data: Dict[str, Any],
        filing_date: Optional[datetime] = None,
        fiscal_period_end: Optional[datetime] = None,
        fiscal_year: Optional[int] = None,
        fiscal_quarter: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Store EDGAR data in the database.

        Args:
            ticker: Company ticker symbol
            company_name: Company name
            xbrl_data: Parsed XBRL data
            filing_date: Filing date (defaults to current date)
            fiscal_period_end: Fiscal period end date
            fiscal_year: Fiscal year
            fiscal_quarter: Fiscal quarter

        Returns:
            Dictionary with company, filing, and facts info or None if failed
        """
        if not self.database_url:
            logger.error("Database URL not provided")
            return None

        session = None
        try:
            session = self.SessionLocal()

            # Get or create company
            company = self.get_or_create_company(ticker, company_name, session=session)
            if not company:
                return None

            # Set default dates if not provided
            if not filing_date:
                filing_date = datetime.now()
            if not fiscal_period_end:
                fiscal_period_end = datetime.now()
            if not fiscal_year:
                fiscal_year = filing_date.year
            if not fiscal_quarter:
                fiscal_quarter = (filing_date.month - 1) // 3 + 1

            # Create filing record
            filing_number = f"10-Q-{ticker}-{fiscal_year}Q{fiscal_quarter}"
            filing = self.create_filing_record(
                company_id=company.id,
                source="SEC",
                filing_number=filing_number,
                form_type="10-Q",
                filing_date=filing_date,
                fiscal_period_end=fiscal_period_end,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                session=session,
            )
            if not filing:
                return None

            # Create financial facts
            facts = self.create_financial_facts(filing.id, xbrl_data, session=session)

            # Commit all changes
            session.commit()

            # Return data with IDs (not objects to avoid session issues)
            return {
                "company": {
                    "id": company.id,
                    "ticker": company.ticker,
                    "name": company.name,
                },
                "filing": {
                    "id": filing.id,
                    "form_type": filing.form_type,
                    "filing_date": filing.filing_date,
                },
                "facts_count": len(facts),
            }

        except Exception as e:
            logger.error(f"Error storing data in database: {e}")
            if session:
                session.rollback()
            return None
        finally:
            if session:
                session.close()

    def download_10q_filing(self, ticker: str, amount: int = 1) -> Optional[str]:
        """
        Download the most recent 10-Q filing for a ticker.

        Args:
            ticker: Company ticker symbol
            amount: Number of filings to download (default: 1)

        Returns:
            Path to the downloaded filing or None if failed
        """
        try:
            logger.info(f"Downloading 10-Q filing for {ticker}")

            # Download the most recent 10-Q filing
            self.downloader.get("10-Q", ticker, limit=amount)

            # Find the downloaded file - check both old and new path structures
            possible_paths = [
                self.download_path / ticker.upper() / "10-Q",
                self.download_path / "sec-edgar-filings" / ticker.upper() / "10-Q",
            ]

            for ticker_path in possible_paths:
                if ticker_path.exists():
                    # Get the most recent filing
                    filings = list(ticker_path.glob("*"))
                    if filings:
                        latest_filing = max(filings, key=lambda x: x.stat().st_mtime)
                        logger.info(f"Downloaded filing: {latest_filing}")
                        return str(latest_filing)

            logger.warning(f"No 10-Q filing found for {ticker}")
            return None

        except Exception as e:
            logger.error(f"Error downloading 10-Q for {ticker}: {e}")
            return None

    def parse_xbrl_filing(self, filing_path: str) -> Optional[Dict[str, Any]]:
        """
        Parse XBRL filing and extract financial data.

        Args:
            filing_path: Path to the XBRL filing

        Returns:
            Dictionary containing parsed financial data
        """
        try:
            logger.info(f"Parsing XBRL filing: {filing_path}")

            filing_dir = Path(filing_path)

            # Check if the directory exists
            if not filing_dir.exists():
                logger.error(f"Filing directory does not exist: {filing_path}")
                return None

            # Look for different file types
            xbrl_files = []

            # Check for individual XML files first
            xml_files = list(filing_dir.rglob("*.xml"))
            logger.info(f"Found {len(xml_files)} XML files")
            xbrl_files.extend(xml_files)

            # Check for full submission file
            full_submission_files = list(filing_dir.glob("full-submission.txt"))
            logger.info(f"Found {len(full_submission_files)} full submission files")
            if full_submission_files:
                xbrl_files.extend(full_submission_files)

            # Also check for any .txt files that might contain XBRL
            txt_files = list(filing_dir.rglob("*.txt"))
            logger.info(f"Found {len(txt_files)} TXT files")
            xbrl_files.extend(txt_files)

            if not xbrl_files:
                logger.warning(
                    f"No XBRL files found in filing directory: {filing_path}"
                )
                logger.info(f"Directory contents: {list(filing_dir.iterdir())}")
                return None

            # Use the first file found
            xbrl_file = xbrl_files[0]
            logger.info(f"Parsing file: {xbrl_file}")

            # Read the file content
            try:
                with open(xbrl_file, "r", encoding="utf-8") as f:
                    content = f.read()
                logger.info(f"Successfully read file with {len(content)} characters")
            except UnicodeDecodeError:
                # Try with different encoding
                with open(xbrl_file, "r", encoding="latin-1") as f:
                    content = f.read()
                logger.info(
                    f"Successfully read file with latin-1 encoding, {len(content)} characters"
                )
            except Exception as e:
                logger.error(f"Error reading file {xbrl_file}: {e}")
                return None

            # If it's a full submission file, extract XBRL content
            if xbrl_file.name == "full-submission.txt":
                logger.info("Processing full submission file")
                # Extract XBRL content from the full submission
                xbrl_content = self._extract_xbrl_from_full_submission(content)
                if not xbrl_content:
                    logger.warning("No XBRL content found in full submission file")
                    return None

                # Parse the extracted XBRL content using StringIO
                try:
                    xbrl_content_io = StringIO(xbrl_content)
                    xbrl_obj = self.xbrl_parser.parse(xbrl_content_io)
                    logger.info("Successfully parsed extracted XBRL content")
                except Exception as e:
                    logger.error(f"Error parsing extracted XBRL content: {e}")
                    return None
            else:
                # Parse the XML file directly
                try:
                    xbrl_obj = self.xbrl_parser.parse(str(xbrl_file))
                    logger.info("Successfully parsed XML file directly")
                except Exception as e:
                    logger.error(f"Error parsing XML file directly: {e}")
                    # Try parsing the content string using StringIO instead
                    try:
                        content_io = StringIO(content)
                        xbrl_obj = self.xbrl_parser.parse(content_io)
                        logger.info("Successfully parsed content string using StringIO")
                    except Exception as e2:
                        logger.error(
                            f"Error parsing content string with StringIO: {e2}"
                        )
                        return None

            # Extract GAAP data
            try:
                # Use the correct method to parse GAAP data
                gaap_obj = self.xbrl_parser.parseGAAP(xbrl_obj)
                serializer = GAAPSerializer(gaap_obj)

                # Get all available data
                data = serializer.data
                logger.info(
                    f"Successfully extracted GAAP data with {len(data)} data points"
                )
                return data
            except Exception as e:
                logger.error(f"Error extracting GAAP data: {e}")
                logger.info("Attempting fallback: extracting raw XBRL data")

                # Fallback: extract raw XBRL data without GAAP parsing
                try:
                    raw_data = self._extract_raw_xbrl_data(xbrl_obj)
                    logger.info(
                        f"Successfully extracted raw XBRL data with {len(raw_data)} items"
                    )
                    return raw_data
                except Exception as fallback_error:
                    logger.error(f"Fallback extraction also failed: {fallback_error}")
                    return None

        except Exception as e:
            logger.error(f"Error parsing XBRL filing: {e}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def _extract_raw_xbrl_data(self, xbrl_obj) -> Dict[str, Any]:
        """
        Extract raw XBRL data as a fallback when GAAP parsing fails.

        Args:
            xbrl_obj: Parsed XBRL object

        Returns:
            Dictionary with raw XBRL data
        """
        try:
            raw_data = {}

            # Extract numeric elements
            numeric_elements = xbrl_obj.find_all(
                name=re.compile("nonnumeric|numeric", re.IGNORECASE)
            )

            logger.info(
                f"Found {len(numeric_elements)} numeric elements: {numeric_elements}"
            )

            for element in numeric_elements:
                try:
                    # Get element name
                    element_name = element.get("name", element.get("id", "unknown"))

                    # Get value
                    value = element.get_text(strip=True)

                    # Get context
                    context = element.get("contextref", "")

                    # Get unit
                    unit = element.get("unitref", "")

                    # Get period
                    period = element.get("period", "")

                    # Create key
                    key = f"{element_name}_{context}_{period}"

                    raw_data[key] = {
                        "value": value,
                        "context": context,
                        "unit": unit,
                        "period": period,
                        "element_name": element_name,
                    }

                except Exception as e:
                    logger.warning(f"Error processing XBRL element: {e}")
                    continue

            # Also extract any other relevant elements
            all_elements = xbrl_obj.find_all()
            for element in all_elements:
                try:
                    element_name = element.name
                    if element_name and element_name not in [
                        "xbrl",
                        "html",
                        "body",
                        "head",
                    ]:
                        text_content = element.get_text(strip=True)
                        if (
                            text_content and len(text_content) < 1000
                        ):  # Avoid huge text blocks
                            raw_data[f"element_{element_name}"] = {
                                "value": text_content,
                                "element_type": element_name,
                            }
                except Exception:
                    continue

            return raw_data

        except Exception as e:
            logger.error(f"Error in raw XBRL extraction: {e}")
            return {}

    def _extract_xbrl_from_full_submission(self, content: str) -> Optional[str]:
        """
        Extract XBRL content from a full submission file.

        Args:
            content: Content of the full submission file

        Returns:
            XBRL content as string or None if not found
        """
        try:
            logger.info(f"Extracting XBRL from content with {len(content)} characters")

            # Look for XBRL document markers
            xbrl_start_markers = [
                "<XBRL>",
                "<?xml version",
                "<xbrl",
                "<ix:nonNumeric",
                "<XBRL",
            ]

            xbrl_end_markers = ["</XBRL>", "</xbrl>", "</XBRL"]

            # Find XBRL content
            for start_marker in xbrl_start_markers:
                start_pos = content.find(start_marker)
                if start_pos != -1:
                    logger.info(
                        f"Found start marker: {start_marker} at position {start_pos}"
                    )
                    # Find the corresponding end marker
                    for end_marker in xbrl_end_markers:
                        end_pos = content.find(end_marker, start_pos)
                        if end_pos != -1:
                            xbrl_content = content[
                                start_pos : end_pos + len(end_marker)
                            ]
                            logger.info(
                                f"Found XBRL content with {len(xbrl_content)} characters"
                            )
                            return xbrl_content

            # If no structured XBRL found, try to extract XML-like content
            # Look for XML content between < and > tags
            import re

            # Try different XML patterns
            xml_patterns = [
                r"<\?xml[^>]*>.*?</[^>]*>",
                r"<[^>]*>.*?</[^>]*>",
                r"<xbrl[^>]*>.*?</xbrl>",
                r"<XBRL[^>]*>.*?</XBRL>",
            ]

            for pattern in xml_patterns:
                xml_matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
                if xml_matches:
                    # Use the longest XML match
                    longest_match = max(xml_matches, key=len)
                    logger.info(
                        f"Found XML content with pattern {pattern}, {len(longest_match)} characters"
                    )
                    return longest_match

            # Try to find any XML-like structure
            logger.info("No structured XML found, looking for any XML-like content")

            # Look for content between any XML tags
            xml_tag_pattern = r"<[^>]+>.*?</[^>]+>"
            xml_matches = re.findall(xml_tag_pattern, content, re.DOTALL)

            if xml_matches:
                # Use the longest match
                longest_match = max(xml_matches, key=len)
                logger.info(
                    f"Found XML-like content with {len(longest_match)} characters"
                )
                return longest_match

            logger.warning("No XBRL content found in full submission file")
            logger.info(f"First 500 characters of content: {content[:500]}")
            return None

        except Exception as e:
            logger.error(f"Error extracting XBRL from full submission: {e}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def extract_financial_metrics(self, xbrl_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Extract key financial metrics from XBRL data.

        Args:
            xbrl_data: Parsed XBRL data (either GAAP or raw format)

        Returns:
            DataFrame with financial metrics
        """
        if not xbrl_data:
            return pd.DataFrame()

        extracted_data = []

        # Check if this is GAAP data (has numeric fields)
        if any(isinstance(v, (int, float)) for v in xbrl_data.values()):
            # This is GAAP data
            logger.info("Processing GAAP data format")

            # Define key financial metrics to extract
            key_metrics = [
                "Revenues",
                "NetIncomeLoss",
                "Assets",
                "Liabilities",
                "StockholdersEquity",
                "CashAndCashEquivalents",
                "TotalCurrentAssets",
                "TotalCurrentLiabilities",
                "OperatingIncomeLoss",
                "GrossProfit",
                "CostOfGoodsAndServicesSold",
                "OperatingExpenses",
                "InterestExpense",
                "IncomeTaxExpense",
                "EarningsPerShareBasic",
                "EarningsPerShareDiluted",
            ]

            for metric_name in key_metrics:
                if metric_name in xbrl_data:
                    metric_data = xbrl_data[metric_name]

                    # Handle different data structures
                    if isinstance(metric_data, list):
                        for item in metric_data:
                            if isinstance(item, dict):
                                extracted_data.append(
                                    {
                                        "metric": metric_name,
                                        "value": item.get("value", ""),
                                        "unit": item.get("unit", ""),
                                        "context": item.get("context", ""),
                                        "period": item.get("period", ""),
                                    }
                                )
                    elif isinstance(metric_data, dict):
                        extracted_data.append(
                            {
                                "metric": metric_name,
                                "value": metric_data.get("value", metric_data),
                                "unit": metric_data.get("unit", ""),
                                "context": metric_data.get("context", ""),
                                "period": metric_data.get("period", ""),
                            }
                        )
                    else:
                        extracted_data.append(
                            {
                                "metric": metric_name,
                                "value": metric_data,
                                "unit": "",
                                "context": "",
                                "period": "",
                            }
                        )
        else:
            # This is raw XBRL data
            logger.info("Processing raw XBRL data format")

            for key, data in xbrl_data.items():
                if isinstance(data, dict):
                    extracted_data.append(
                        {
                            "metric": data.get("element_name", key),
                            "value": data.get("value", ""),
                            "unit": data.get("unit", ""),
                            "context": data.get("context", ""),
                            "period": data.get("period", ""),
                        }
                    )
                else:
                    extracted_data.append(
                        {
                            "metric": key,
                            "value": str(data),
                            "unit": "",
                            "context": "",
                            "period": "",
                        }
                    )

        df = pd.DataFrame(extracted_data)

        if not df.empty:
            # Clean up the data
            df = df.dropna(subset=["value"])
            df = df[df["value"] != ""]

            # Convert numeric values, handling various formats
            def convert_to_numeric(value):
                if pd.isna(value) or value == "":
                    return None

                # Convert to string and clean
                str_value = str(value).strip().replace(",", "").replace("$", "")

                # Skip boolean and non-numeric values
                if str_value.lower() in [
                    "true",
                    "false",
                    "yes",
                    "no",
                    "null",
                    "none",
                    "n/a",
                    "-",
                ]:
                    return None

                try:
                    return pd.to_numeric(str_value, errors="coerce")
                except Exception:
                    return None

            df["numeric_value"] = df["value"].apply(convert_to_numeric)

            # Remove rows with no numeric value
            df = df.dropna(subset=["numeric_value"])

            logger.info(f"Extracted {len(df)} financial metrics with numeric values")
        else:
            logger.warning("No financial metrics extracted")

        return df

    def save_to_csv(self, df: pd.DataFrame, output_file: str) -> bool:
        """
        Save financial data to CSV file.

        Args:
            df: DataFrame with financial data
            output_file: Output CSV file path

        Returns:
            True if successful, False otherwise
        """
        try:
            if df.empty:
                logger.warning("No data to save")
                return False

            # Create output directory if it doesn't exist
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Save to CSV
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(df)} financial metrics to {output_file}")

            return True

        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return False

    def get_10q_data_for_ticker(
        self, ticker: str, output_file: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        Get 10-Q data for a single ticker and optionally save to CSV.

        Args:
            ticker: Company ticker symbol
            output_file: Optional CSV output file path

        Returns:
            DataFrame with financial data or None if failed
        """
        try:
            logger.info(f"Processing 10-Q data for {ticker}")

            # Download the filing
            filing_path = self.download_10q_filing(ticker)
            if not filing_path:
                logger.error(f"Failed to download 10-Q filing for {ticker}")
                return None

            # Parse the XBRL data
            xbrl_data = self.parse_xbrl_filing(filing_path)
            if not xbrl_data:
                logger.error(f"Failed to parse XBRL data for {ticker}")
                return None

            # Extract financial metrics
            df = self.extract_financial_metrics(xbrl_data)

            if df.empty:
                logger.warning(f"No financial metrics found for {ticker}")
                return None

            # Save to CSV if output file specified
            if output_file:
                self.save_to_csv(df, output_file)

            return df

        except Exception as e:
            logger.error(f"Error getting 10-Q data for {ticker}: {e}")
            return None

    def process_and_store_10q_data(
        self,
        ticker: str,
        company_name: str,
        output_file: Optional[str] = None,
        store_in_db: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """
        Download, parse, and store 10-Q data for a ticker.

        Args:
            ticker: Company ticker symbol
            company_name: Company name
            output_file: Optional CSV output file path
            store_in_db: Whether to store data in database (default: True)

        Returns:
            Dictionary with processing results or None if failed
        """
        try:
            logger.info(f"Processing and storing 10-Q data for {ticker}")

            # Download the filing
            filing_path = self.download_10q_filing(ticker)
            if not filing_path:
                logger.error(f"Failed to download 10-Q filing for {ticker}")
                return None

            # Parse the XBRL data
            xbrl_data = self.parse_xbrl_filing(filing_path)
            if not xbrl_data:
                logger.error(f"Failed to parse XBRL data for {ticker}")
                return None

            # Extract financial metrics for CSV export
            df = self.extract_financial_metrics(xbrl_data)

            result = {
                "ticker": ticker,
                "company_name": company_name,
                "filing_path": filing_path,
                "xbrl_data": xbrl_data,
                "financial_metrics_df": df,
                "csv_saved": False,
                "database_stored": False,
            }

            # Save to CSV if output file specified
            if output_file and not df.empty:
                if self.save_to_csv(df, output_file):
                    result["csv_saved"] = True

            # Store in database if requested and database is available
            if store_in_db and self.database_url:
                db_result = self.store_in_database(ticker, company_name, xbrl_data)
                if db_result:
                    result.update(db_result)
                    result["database_stored"] = True
                    logger.info(f"Successfully stored {ticker} data in database")
                else:
                    logger.warning(f"Failed to store {ticker} data in database")
            elif store_in_db and not self.database_url:
                logger.warning(
                    "Database storage requested but no database URL provided"
                )

            return result

        except Exception as e:
            logger.error(f"Error processing and storing 10-Q data for {ticker}: {e}")
            return None


def main():
    """Example usage of the minimal EDGAR XBRL extractor with database integration."""

    # Example 1: CSV-only processing (no database)
    print("=" * 60)
    print("Example 1: CSV-only processing")
    print("=" * 60)

    extractor_csv = MinimalEdgarExtractor()

    df = extractor_csv.get_10q_data_for_ticker(
        ticker="AAPL", output_file="apple_10q_data.csv"
    )

    if df is not None:
        print(f"✅ Successfully extracted {len(df)} financial metrics")
        print("\nFinancial Metrics:")
        print(df.head().to_string(index=False))
    else:
        print("❌ Failed to extract data")

    # Example 2: Database processing (if database URL provided)
    print("\n" + "=" * 60)
    print("Example 2: Database processing")
    print("=" * 60)

    # You can set the database URL here or use environment variable
    database_url = "postgresql://rag_user:rag_password@localhost:5432/rag_db"

    extractor_db = MinimalEdgarExtractor(database_url=database_url)

    result = extractor_db.process_and_store_10q_data(
        ticker="AAPL",
        company_name="Apple Inc.",
        output_file="apple_10q_data_db.csv",
        store_in_db=True,
    )

    if result:
        print(f"✅ Successfully processed {result['ticker']}")
        print(f"   - Company: {result['company_name']}")
        print(f"   - Filing Path: {result['filing_path']}")
        print(f"   - CSV Saved: {result['csv_saved']}")
        print(f"   - Database Stored: {result['database_stored']}")

        if result.get("company"):
            print(f"   - Company ID: {result['company'].id}")
        if result.get("filing"):
            print(f"   - Filing ID: {result['filing'].id}")
        if result.get("facts_count"):
            print(f"   - Financial Facts: {result['facts_count']}")

        if result["financial_metrics_df"] is not None:
            print(
                f"\n📊 Financial Metrics ({len(result['financial_metrics_df'])} found):"
            )
            print(result["financial_metrics_df"].head().to_string(index=False))
    else:
        print("❌ Failed to process data")
        print(
            "   (This is expected if SEC is rate-limited or database is not available)"
        )


if __name__ == "__main__":
    main()
