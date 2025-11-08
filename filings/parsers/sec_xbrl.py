"""SEC 10-Q XBRL Parser using edgartools."""

import logging
import math
import uuid
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Optional

import pandas as pd
from edgar import Company, Filing

from ..models import FinancialFactCreate, PeriodType
from .geography import GeographyParser
from .product import ProductParser

logger = logging.getLogger(__name__)


@dataclass
class HierarchyEntry:
    key: str
    level: int


class SECXBRLParser:
    """Parser for SEC XBRL (10-Q, 10-K, etc.) filings using edgartools."""

    def __init__(
        self,
        geography_parser: GeographyParser,
        product_parser: ProductParser,
    ):
        """Initialize the parser.

        Args:
            geography_parser: Geography parser instance.
        """
        self.geography_parser = geography_parser
        self.product_parser = product_parser

    def parse_filing(self, filing: Filing) -> list[FinancialFactCreate]:
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
                xbrl.statements.income_statement().to_dataframe(include_unit=True),
                "Income Statement",
            )
            facts.extend(income_facts)

            # Parse balance sheet
            balance_facts = self._parse_statement(
                xbrl.statements.balance_sheet().to_dataframe(include_unit=True),
                "Balance Sheet",
            )
            facts.extend(balance_facts)

            # Parse cash flow statement
            cashflow_facts = self._parse_statement(
                xbrl.statements.cashflow_statement().to_dataframe(include_unit=True),
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
    ) -> list[FinancialFactCreate]:
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
            period_columns = [
                col
                for col in columns
                if isinstance(col, str)
                and ("-" in col or "/" in col)
                and not self._is_column_mostly_empty(statement_df, col)
            ]

            if not period_columns:
                logger.warning(f"No period columns found in {statement_type}")
                return facts

            # Sort by date string (YYYY-MM-DD format sorts lexicographically)
            latest_period_col = sorted(period_columns)[-1]
            comparative_period_col = self._extract_comparative_period_column(
                period_columns, latest_period_col
            )

            # Track the hierarchy of abstracts
            hierarchy = []

            # Track global fact position (only for facts, not abstracts)
            position = 0

            # Filter out rows where dimension=True (if dimension column exists)
            if "dimension" in statement_df.columns:
                statement_df = statement_df[
                    statement_df["dimension"] != True  # noqa: E712
                ]

            # Iterate through each row in the statement
            for _, row in statement_df.iterrows():
                level = row.get("level", 1)

                # Update hierarchy based on level
                while len(hierarchy) > 0 and hierarchy[-1].level >= level:
                    hierarchy.pop()

                # Create financial fact with hierarchy context
                fact = self._create_financial_fact_with_hierarchy(
                    row,
                    statement_type,
                    latest_period_col,
                    comparative_period_col,
                    hierarchy,
                    position,
                )

                if fact:
                    facts.append(fact)
                    position += 1

                # Add current abstract to hierarchy if it's an abstract
                if fact and fact.is_abstract:
                    hierarchy.append(
                        HierarchyEntry(
                            level=level,
                            key=fact.key,
                        )
                    )

        except Exception:
            logger.exception(f"Error parsing {statement_type}")

        return facts

    def _parse_disaggregated_metrics(
        self, xbrl, metric: str
    ) -> list[FinancialFactCreate]:
        """Parse disaggregated metrics using XBRL queries.

        Args:
            xbrl: XBRL object from edgartools
            metric: The metric to parse (e.g., "Revenue", "OperatingIncome", etc.)

        Returns:
            List of FinancialFact objects for disaggregated metrics
        """
        facts = []
        position = 0

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
                    position += 1
                    segment_member = row.get(
                        self._to_df_dim("srt:ProductOrServiceAxis"), ""
                    )

                    fact = self._create_disaggregated_metric_fact(
                        row,
                        metric=metric,
                        dimension="srt:ProductOrServiceAxis",
                        dimension_parsed="Product",
                        dimension_value_parsed=self.product_parser.parse_product(
                            segment_member
                        ).product,
                        position=position,
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
                    position += 1
                    fact = self._create_disaggregated_metric_fact(
                        row,
                        metric=metric,
                        dimension="srt:StatementGeographicAxis",
                        dimension_parsed="Geographic",
                        dimension_value_parsed=None,
                        position=position,
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
                        position += 1
                        fact = self._create_disaggregated_metric_fact(
                            row,
                            metric=metric,
                            dimension="us-gaap:StatementBusinessSegmentsAxis",
                            dimension_parsed="Geographic",
                            dimension_value_parsed=self.geography_parser.parse_geography(
                                segment_member
                            ).geography,
                            position=position,
                        )
                        if fact:
                            facts.append(fact)

            logger.info(f"Extracted {len(facts)} disaggregated {metric} facts")

        except Exception as e:
            logger.error(f"Error parsing disaggregated {metric}: {e}")

        return facts

    def _parse_disaggregated_revenues(self, xbrl) -> list[FinancialFactCreate]:
        """Parse disaggregated revenues using XBRL queries.

        Args:
            xbrl: XBRL object from edgartools

        Returns:
            List of FinancialFact objects for disaggregated revenues
        """
        return self._parse_disaggregated_metrics(xbrl, "Revenue")

    def _parse_disaggregated_operating_income(self, xbrl) -> list[FinancialFactCreate]:
        """Parse disaggregated operating income using XBRL queries.

        Args:
            xbrl: XBRL object from edgartools

        Returns:
            List of FinancialFact objects for disaggregated operating income
        """
        return self._parse_disaggregated_metrics(xbrl, "OperatingIncome")

    def _create_disaggregated_metric_fact(
        self,
        row,
        metric: str,
        dimension: str,
        dimension_parsed: str,
        dimension_value_parsed: Optional[str],
        position: int,
    ) -> Optional[FinancialFactCreate]:
        """Create a FinancialFact for disaggregated metrics.

        Args:
            row: DataFrame row from XBRL query
            metric: The metric being disaggregated (e.g., "Revenue", "OperatingIncome")
            dimension: The dimension axis
            dimension_parsed: The parsed dimension (Product, Geographic)
            dimension_value_parsed: The parsed dimension value
            position: Global position of this fact

        Returns:
            FinancialFact object or None if invalid
        """
        try:
            # Extract basic information
            concept = row.get("concept", metric)
            value = row.get("numeric_value")
            weight = row.get("weight")
            unit = row.get("unit", "usd")

            # Get the label from the XBRL data
            label = row.get("label", concept)

            axis = dimension
            member = row.get(self._to_df_dim(dimension), "UnknownMember")
            parsed_axis = dimension_parsed
            parsed_member = dimension_value_parsed

            # Skip if no value
            if not value or math.isnan(value):
                return None

            # Convert value to Decimal
            try:
                value_decimal = Decimal(str(value))
            except (ValueError, TypeError):
                logger.warning(f"Invalid value for disaggregated {metric}: {value}")
                return None

            weight_decimal = None
            if weight and not math.isnan(weight):
                try:
                    weight_decimal = Decimal(str(weight))
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid weight for disaggregated {metric}: {weight}"
                    )

            # Parse dates
            period_start = self._parse_date(row.get("period_start"))
            period_end = self._parse_date(row.get("period_end"))

            # Determine period type based on dates
            period = self._determine_period_type(period_start, period_end)

            # Create the financial fact
            fact = FinancialFactCreate(
                key=str(uuid.uuid4()),
                parent_key=None,
                filing_id=0,
                concept=concept,
                label=label,
                is_abstract=False,
                value=value_decimal,
                weight=weight_decimal,
                comparative_value=None,  # Temporarily unsupported
                unit=unit,
                axis=axis,
                member=member,
                parsed_axis=parsed_axis,
                parsed_member=parsed_member,
                statement="Income Statement",
                period_end=period_end,
                comparative_period_end=None,
                period=period,
                position=position,
            )

            return fact

        except Exception:
            logger.exception(f"Error creating disaggregated {metric}, {dimension} fact")
            return None

    def _determine_period_type(
        self, period_start: Optional[date], period_end: Optional[date]
    ) -> PeriodType:
        """Determine period type based on start and end dates.

        Args:
            period_start: Start date of the period
            period_end: End date of the period

        Returns:
            PeriodType.YTD if it's year-to-date, PeriodType.Q if it's a quarter
        """
        if not period_start or not period_end:
            # Default to Q if we can't determine
            return PeriodType.Q

        # Calculate the number of days in the period
        days_in_period = (period_end - period_start).days

        # If the period is approximately 3 months (90 days +/- 10 days), it's a quarter
        if 80 <= days_in_period <= 100:
            return PeriodType.Q
        # If the period is longer (like 6-12 months), it's likely YTD
        elif days_in_period > 100:
            return PeriodType.YTD
        # Default to Q for shorter periods
        else:
            return PeriodType.Q

    def _determine_period_type_from_column(
        self, period_col: str, statement_type: str
    ) -> Optional[PeriodType]:
        """Determine period type based on the period column name.

        Args:
            period_col: The period column name (e.g., "2025-06-28 (Q2)", "2025-12-31")

        Returns:
            PeriodType.Q if it's a quarter, PeriodType.YTD if it's year-to-date, None if no period info
        """

        if statement_type == "Balance Sheet":
            return None

        # Validate that the period column contains an ISO date (YYYY-MM-DD format)
        import re

        iso_date_pattern = r"\b\d{4}-\d{2}-\d{2}\b"
        if not re.search(iso_date_pattern, period_col):
            # If no ISO date found, return None (for balance sheet items, etc.)
            return None

        # Check if the column name contains quarter indicators
        period_col_lower = period_col.lower()
        if any(
            indicator in period_col_lower
            for indicator in ["q1", "q2", "q3", "q4", "quarter"]
        ):
            return PeriodType.Q

        # If we can't determine the period type, default to YTD
        return PeriodType.YTD

    def _extract_comparative_period_column(
        self, period_columns: list[str], latest_period_col: str
    ) -> Optional[str]:
        period_info = latest_period_col.split(" ")
        if len(period_info) == 1:
            period_date = period_info[0]
            return next(
                (
                    x
                    for _, x in enumerate(period_columns)
                    if len(x.split(" ")) == 1 and period_date not in x
                ),
                None,
            )
        elif len(period_info) == 2:
            period_date = period_info[0]
            period_quarter = period_info[1]
            return next(
                (
                    x
                    for _, x in enumerate(period_columns)
                    if len(x.split(" ")) == 2
                    and period_quarter in x
                    and period_date not in x
                ),
                None,
            )
        else:
            return None

    def _is_column_mostly_empty(
        self, df: pd.DataFrame, column_name: str, threshold: float = 20.0
    ) -> bool:
        """Check if a specific column has mostly empty values.

        This is a workaround for edgartools issue #408

        Args:
            df: DataFrame to check
            column_name: Name of the column to check
            threshold: Percentage threshold (default 20%). If non-null values are below this, column is considered mostly empty.

        Returns:
            True if the column is mostly empty (below threshold), False otherwise
        """
        if df is None or df.empty or column_name not in df.columns:
            return True

        # Calculate the percentage of non-null, non-empty values for the specific column
        # Count values that are not null/NaN and not empty strings
        non_empty_count = (
            df[column_name]
            .apply(lambda x: x is not None and x != "" and pd.notna(x))
            .sum()
        )
        total_rows = len(df)
        non_empty_percentage = (non_empty_count / total_rows) * 100
        return non_empty_percentage < threshold

    def _to_df_dim(self, sec_dim: str) -> str:
        dim = sec_dim.replace(":", "_", 1)
        return f"dim_{dim}"

    def _to_sec_concept(self, concept: str) -> str:
        return concept.replace("_", ":", 1)

    def _create_financial_fact_with_hierarchy(
        self,
        row,
        statement_type: str,
        period_col: str,
        comparative_period_col: Optional[str],
        hierarchy: list[HierarchyEntry],
        position: int,
    ) -> Optional[FinancialFactCreate]:
        """Create a FinancialFact from a statement row with hierarchical abstracts.

        Args:
            row: DataFrame row from statement
            statement_type: Type of financial statement
            period_col: The period column name (e.g., "2025-06-28 (Q2)")
            comparative_period_col: The comparative period (past year) column name (e.g., "2024-06-28 (Q2)")
            hierarchy: List of parent abstracts in hierarchy
            position: Global position of this fact in the statement

        Returns:
            FinancialFact object or None if invalid
        """
        try:
            # Extract basic information
            concept = self._to_sec_concept(row.get("concept", ""))
            label = row.get("label", concept)
            is_abstract = row.get("abstract")
            unit = row.get("unit", "usd" if not is_abstract else None)
            value = row.get(period_col)
            comparative_value = (
                row.get(comparative_period_col) if comparative_period_col else None
            )
            weight = row.get("weight")

            # Skip if concept is empty
            if not concept:
                return None

            # Skip facts without a value
            if not is_abstract and (not value or math.isnan(value)):
                return None

            # Skip undesired abstracts
            if is_abstract and any(
                pattern in label
                for pattern in [
                    "[Abstract]",
                    "[Table]",
                    "[Line Items]",
                    "[Axis]",
                    "[Domain]",
                ]
            ):
                return None

            # Convert value to Decimal
            value_decimal = None
            if value and not math.isnan(value):
                try:
                    value_decimal = Decimal(str(value))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid value for concept {concept}: {value}")
                    return None

            # Convert comparative value to Decimal
            comparative_value_decimal = None
            if comparative_value and not math.isnan(comparative_value):
                try:
                    comparative_value_decimal = Decimal(str(comparative_value))
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid comparative value for concept {concept}: {comparative_value}"
                    )

            weight_decimal = None
            if weight and not math.isnan(weight):
                try:
                    weight_decimal = Decimal(str(weight))
                except (ValueError, TypeError):
                    logger.warning(f"Invalid weight for concept {concept}: {weight}")

            # Get "2025-06-28" from "2025-06-28 (Q2)"
            period_end = self._parse_date(period_col.split(" ")[0])
            comparative_period_end = (
                self._parse_date(comparative_period_col.split(" ")[0])
                if comparative_period_col
                else None
            )

            # Determine period type based on the period column name
            period = self._determine_period_type_from_column(period_col, statement_type)

            # Create the financial fact
            fact = FinancialFactCreate(
                key=str(uuid.uuid4()),
                parent_key=hierarchy[-1].key if hierarchy else None,
                filing_id=0,
                concept=concept,
                label=label,
                value=value_decimal,
                is_abstract=is_abstract,
                comparative_value=comparative_value_decimal,
                weight=weight_decimal,
                unit=unit,
                axis=None,  # Could be extracted from dimension data if available
                member=None,  # Could be extracted from dimension data if available
                statement=statement_type,
                period_end=period_end,
                comparative_period_end=comparative_period_end,
                period=period,
                position=position,
            )

            return fact

        except Exception:
            logger.exception(f"Error creating financial fact from row {row}")
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
    ) -> list[FinancialFactCreate]:
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
