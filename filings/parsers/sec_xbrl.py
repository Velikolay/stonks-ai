"""SEC 10-Q XBRL Parser using edgartools."""

import logging
from datetime import date
from decimal import Decimal
from typing import List, Optional

import pandas as pd
from edgar import Company, Filing

from ..models import FinancialFact, FinancialFactAbstract
from .geography import GeographyParser

logger = logging.getLogger(__name__)


class SECXBRLParser:
    """Parser for SEC XBRL (10-Q, 10-K, etc.) filings using edgartools."""

    def __init__(self, geography_parser: GeographyParser):
        """Initialize the parser.

        Args:
            geography_parser: Geography parser instance.
        """
        self.geography_parser = geography_parser

    def parse_filing(self, filing: Filing) -> List[FinancialFact]:
        """Parse an XBRL filing and extract financial facts.

        Args:
            filing: The edgartools Filing object

        Returns:
            List of FinancialFact objects
        """
        try:
            # Get XBRL data from the filing
            xbrl = filing.xbrl()

            facts = []

            # Parse income statement
            income_facts = self._parse_statement(
                xbrl.statements.income_statement().to_dataframe(), "Income Statement"
            )
            facts.extend(income_facts)

            # Parse balance sheet
            balance_facts = self._parse_statement(
                xbrl.statements.balance_sheet().to_dataframe(), "Balance Sheet"
            )
            facts.extend(balance_facts)

            # Parse cash flow statement
            cashflow_facts = self._parse_statement(
                xbrl.statements.cashflow_statement().to_dataframe(),
                "Cash Flow Statement",
            )
            facts.extend(cashflow_facts)

            # Parse disaggregated revenues
            disaggregated_revenue_facts = self._parse_disaggregated_revenues(xbrl)
            facts.extend(disaggregated_revenue_facts)

            # Parse disaggregated operating income
            disaggregated_operating_income_facts = (
                self._parse_disaggregated_operating_income(xbrl)
            )
            facts.extend(disaggregated_operating_income_facts)

            logger.info(
                f"Parsed {len(facts)} financial facts from filing {filing.accession_number}"
            )
            return facts

        except Exception as e:
            logger.error(f"Error parsing filing {filing.accession_number}: {e}")
            return []

    def _parse_statement(
        self, statement_df: pd.DataFrame, statement_type: str
    ) -> List[FinancialFact]:
        """Parse a financial statement dataframe and extract facts.

        Args:
            statement_df: DataFrame from edgartools statement
            statement_type: Type of statement (Income Statement, Balance Sheet, etc.)

        Returns:
            List of FinancialFact objects
        """
        facts = []

        if statement_df is None or statement_df.empty:
            logger.warning(f"No data found for {statement_type}")
            return facts

        try:
            # Get the column names to identify period columns
            columns = statement_df.columns.tolist()

            # Find the column with the latest date (period column)
            period_columns = []

            for col in columns:
                if isinstance(col, str) and ("-" in col or "/" in col):
                    period_columns.append(col)

            if not period_columns:
                logger.warning(f"No period columns found in {statement_type}")
                return facts

            # Sort by date string (YYYY-MM-DD format sorts lexicographically)
            latest_period_col = sorted(period_columns)[-1]

            # Extract date from the latest period column
            # Get "2025-06-28" from "2025-06-28 (Q2)"
            date_str = latest_period_col.split(" ")[0]

            # Track the hierarchy of abstracts
            abstract_hierarchy = []

            # Iterate through each row in the statement
            for _, row in statement_df.iterrows():
                level = row.get("level", 1)
                concept = row.get("concept", "")
                label = row.get("label", "")
                value = row.get(latest_period_col)

                # Detect abstract by lack of numeric value
                is_abstract = (
                    value is None
                    or value == 0
                    or (isinstance(value, str) and not value.strip())
                )

                # Update abstract hierarchy based on level
                while (
                    len(abstract_hierarchy) > 0
                    and abstract_hierarchy[-1]["level"] >= level
                ):
                    abstract_hierarchy.pop()

                # Add current abstract to hierarchy if it's an abstract
                if is_abstract:
                    abstract_hierarchy.append(
                        {"level": level, "concept": concept, "label": label}
                    )

                # Create fact if there's a value (not an abstract)
                if value is not None and value != 0 and not is_abstract:
                    fact = self._create_financial_fact_with_hierarchy(
                        row,
                        statement_type,
                        latest_period_col,
                        date_str,
                        abstract_hierarchy,
                    )
                    if fact:
                        facts.append(fact)

        except Exception as e:
            logger.error(f"Error parsing {statement_type}: {e}")

        return facts

    def _parse_disaggregated_metrics(self, xbrl, metric: str) -> List[FinancialFact]:
        """Parse disaggregated metrics using XBRL queries.

        Args:
            xbrl: XBRL object from edgartools
            metric: The metric to parse (e.g., "Revenue", "OperatingIncome", etc.)

        Returns:
            List of FinancialFact objects for disaggregated metrics
        """
        facts = []

        try:
            # Query disaggregated metrics by product/service
            product_df = (
                xbrl.query()
                .by_concept(metric)
                .by_dimension("ProductOrServiceAxis")
                .to_dataframe()
            )

            if product_df is not None and not product_df.empty:
                # Get the latest period for each product/service
                product_metrics = product_df.loc[
                    product_df.groupby(self._to_df_dim("srt:ProductOrServiceAxis"))[
                        "period_start"
                    ].idxmax()
                ]

                for _, row in product_metrics.iterrows():
                    fact = self._create_disaggregated_metric_fact(
                        row,
                        metric=metric,
                        dimension="srt:ProductOrServiceAxis",
                        dimension_type="Product",
                    )
                    if fact:
                        facts.append(fact)

            # Query disaggregated metrics by geographic region
            geographic_df = (
                xbrl.query()
                .by_concept(metric)
                .by_dimension("StatementGeographicAxis")
                .to_dataframe()
            )

            if geographic_df is not None and not geographic_df.empty:
                # Get the latest period for each geographic region
                geographic_metrics = geographic_df.loc[
                    geographic_df.groupby(
                        self._to_df_dim("srt:StatementGeographicAxis")
                    )["period_start"].idxmax()
                ]

                for _, row in geographic_metrics.iterrows():
                    fact = self._create_disaggregated_metric_fact(
                        row,
                        metric=metric,
                        dimension="srt:StatementGeographicAxis",
                        dimension_type="Geographic",
                    )
                    if fact:
                        facts.append(fact)

            # Query disaggregated metrics by business segments (geographic regions)
            business_segments_df = (
                xbrl.query()
                .by_concept(metric)
                .by_dimension("StatementBusinessSegmentsAxis")
                .to_dataframe()
            )

            if business_segments_df is not None and not business_segments_df.empty:
                # Get the latest period for each business segment
                business_segments_metrics = business_segments_df.loc[
                    business_segments_df.groupby(
                        self._to_df_dim("us-gaap:StatementBusinessSegmentsAxis")
                    )["period_start"].idxmax()
                ]

                for _, row in business_segments_metrics.iterrows():
                    # Check if this business segment contains geographic region information
                    segment_member = row.get(
                        self._to_df_dim("us-gaap:StatementBusinessSegmentsAxis"), ""
                    )
                    if self.geography_parser.is_geography_text(segment_member):
                        fact = self._create_disaggregated_metric_fact(
                            row,
                            metric=metric,
                            dimension="us-gaap:StatementBusinessSegmentsAxis",
                            dimension_type="Geographic",
                        )
                        if fact:
                            facts.append(fact)

            logger.info(f"Extracted {len(facts)} disaggregated {metric} facts")

        except Exception as e:
            logger.error(f"Error parsing disaggregated {metric}: {e}")

        return facts

    def _parse_disaggregated_revenues(self, xbrl) -> List[FinancialFact]:
        """Parse disaggregated revenues using XBRL queries.

        Args:
            xbrl: XBRL object from edgartools

        Returns:
            List of FinancialFact objects for disaggregated revenues
        """
        return self._parse_disaggregated_metrics(xbrl, "Revenue")

    def _parse_disaggregated_operating_income(self, xbrl) -> List[FinancialFact]:
        """Parse disaggregated operating income using XBRL queries.

        Args:
            xbrl: XBRL object from edgartools

        Returns:
            List of FinancialFact objects for disaggregated operating income
        """
        return self._parse_disaggregated_metrics(xbrl, "OperatingIncome")

    def _create_disaggregated_metric_fact(
        self, row, metric: str, dimension: str, dimension_type: str
    ) -> Optional[FinancialFact]:
        """Create a FinancialFact for disaggregated metrics.

        Args:
            row: DataFrame row from XBRL query
            dimension: The dimension axis
            dimension_type: Type of dimension (Product, Geographic)
            metric: The metric being disaggregated (e.g., "Revenue", "OperatingIncome")

        Returns:
            FinancialFact object or None if invalid
        """
        try:
            # Extract basic information
            concept = row.get("concept", metric)
            value = row.get("value")

            # Get the original label from the XBRL data
            original_label = row.get("label", concept)

            axis = dimension
            member = row.get(self._to_df_dim(dimension), "UnknownMember")

            # Skip if no value
            if not value:
                return None

            # Convert value to Decimal
            try:
                decimal_value = Decimal(str(value))
            except (ValueError, TypeError):
                logger.warning(f"Invalid value for disaggregated {metric}: {value}")
                return None

            # Parse dates
            period_start = self._parse_date(row.get("period_start"))
            period_end = self._parse_date(row.get("period_end"))

            # Create label using original concept label and member
            label = f"{original_label} - {member}" if member else original_label

            # Create the financial fact
            fact = FinancialFact(
                id=0,  # Will be set by database
                filing_id=0,  # Will be set by caller
                concept=concept,
                label=label,
                value=decimal_value,
                unit="USD",  # Default unit
                axis=axis,
                member=member,
                statement=f"Disaggregated {metric} ({dimension_type})",
                period_end=period_end,
                period_start=period_start,
                abstracts=None,  # No hierarchy for disaggregated data
            )

            return fact

        except Exception as e:
            logger.error(f"Error creating disaggregated {metric} fact: {e}")
            return None

    def _to_df_dim(self, sec_dim: str) -> str:
        dim = sec_dim.replace(":", "_")
        return f"dim_{dim}"

    def _create_financial_fact_with_hierarchy(
        self,
        row,
        statement_type: str,
        period_col: str,
        date_str: str,
        abstract_hierarchy: list,
    ) -> Optional[FinancialFact]:
        """Create a FinancialFact from a statement row with hierarchical abstracts.

        Args:
            row: DataFrame row from statement
            statement_type: Type of financial statement
            period_col: The period column name (e.g., "2025-06-28 (Q2)")
            date_str: The date string (e.g., "2025-06-28")
            abstract_hierarchy: List of parent abstracts in hierarchy

        Returns:
            FinancialFact object or None if invalid
        """
        try:
            # Extract basic information
            concept = row.get("concept", "")
            label = row.get("label", concept)
            value = row.get(period_col)

            # Skip if no value or concept
            if not value or not concept:
                return None

            # Convert value to Decimal
            try:
                decimal_value = Decimal(str(value))
            except (ValueError, TypeError):
                logger.warning(f"Invalid value for concept {concept}: {value}")
                return None

            # Parse the date
            period_end = self._parse_date(date_str)

            # Create abstracts from hierarchy (only parent abstracts, not the element itself)
            abstracts = []

            # Add parent abstracts from hierarchy
            for abstract in abstract_hierarchy:
                abstracts.append(
                    FinancialFactAbstract(
                        concept=abstract["concept"], label=abstract["label"]
                    )
                )

            # Create the financial fact
            fact = FinancialFact(
                id=0,  # Will be set by database
                filing_id=0,  # Will be set by caller
                concept=concept,
                label=label,
                value=decimal_value,
                unit="USD",  # Default unit, could be extracted from data if available
                axis=None,  # Could be extracted from dimension data if available
                member=None,  # Could be extracted from dimension data if available
                statement=statement_type,
                period_end=period_end,
                period_start=None,  # Could be calculated if needed
                abstracts=abstracts if abstracts else None,
            )

            return fact

        except Exception as e:
            logger.error(f"Error creating financial fact from row: {e}")
            return None

    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object.

        Args:
            date_str: Date string from XBRL

        Returns:
            date object or None if invalid
        """
        if not date_str:
            return None

        try:
            # Handle common XBRL date formats
            if "T" in date_str:
                # ISO format with time: 2024-09-28T00:00:00
                date_str = date_str.split("T")[0]

            return date.fromisoformat(date_str)
        except (ValueError, TypeError):
            logger.warning(f"Invalid date format: {date_str}")
            return None

    def parse_company_filings(
        self, ticker: str, form: str = "10-Q", limit: int = 5
    ) -> List[FinancialFact]:
        """Parse multiple filings for a company.

        Args:
            ticker: Company ticker symbol
            form: Form type (default: 10-Q)
            limit: Maximum number of filings to parse

        Returns:
            List of FinancialFact objects
        """
        try:
            company = Company(ticker)
            filings = company.get_filings(form=form)

            all_facts = []
            # Limit the number of filings to process
            for i, filing in enumerate(filings):
                if i >= limit:
                    break
                facts = self.parse_filing(filing)
                all_facts.extend(facts)

            logger.info(
                f"Parsed {len(all_facts)} total facts from {min(len(filings), limit)} filings for {ticker}"
            )
            return all_facts

        except Exception as e:
            logger.error(f"Error parsing filings for {ticker}: {e}")
            return []
