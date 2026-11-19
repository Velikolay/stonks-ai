"""SEC 10-Q XBRL Parser using edgartools."""

import logging
import math
import uuid
from datetime import date
from decimal import Decimal
from typing import Optional

import pandas as pd
from edgar import Company, Filing

from ..models import FinancialFactCreate, PeriodType

logger = logging.getLogger(__name__)

# Cover-page period focus. A 10-K declares FY; a 10-Q declares Q1, Q2, or Q3.
_FOCUS_QUARTER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "FY": 4}


def fiscal_focus(
    entity_info: Optional[dict],
) -> tuple[Optional[int], Optional[int]]:
    """Return the fiscal year and quarter declared on the filing cover.

    ``DocumentFiscalYearFocus`` and ``DocumentFiscalPeriodFocus`` are read by
    edgartools into ``entity_info``. A missing or unrecognized value is left
    unset so the caller can fall back to the period end.

    Args:
        entity_info: edgartools entity info, or None.

    Returns:
        Fiscal year and quarter. Either is None when the cover fact is absent.
    """
    if not entity_info:
        return None, None
    year_text = str(entity_info.get("fiscal_year") or "").strip()
    year = int(year_text) if year_text.isdigit() and len(year_text) == 4 else None
    period = str(entity_info.get("fiscal_period") or "").strip().upper()
    return year, _FOCUS_QUARTER.get(period)


class SECXBRLParser:
    """Parser for SEC XBRL (10-Q, 10-K, etc.) filings using edgartools."""

    # Axes used for product, geography, and reportable-segment cuts. Disclosure
    # tables also carry customer and residual axes; those are not series we keep.
    _SEGMENT_AXES = frozenset(
        {
            "srt:ProductOrServiceAxis",
            "srt:StatementGeographicalAxis",
            "srt:ConsolidationItemsAxis",
            "srt:StatementBusinessSegmentsAxis",
            "us-gaap:ProductOrServiceAxis",
            "us-gaap:StatementGeographicalAxis",
            "us-gaap:ConsolidationItemsAxis",
            "us-gaap:StatementBusinessSegmentsAxis",
        }
    )

    # Concepts that are the total revenue line, whatever a filing calls it.
    # A note often tags the same line with a different concept than the face.
    _TOTAL_REVENUE_CONCEPTS = frozenset(
        {
            "us-gaap:Revenues",
            "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
            "us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax",
            "us-gaap:SalesRevenueNet",
            "us-gaap:SalesRevenueGoodsNet",
            "us-gaap:SalesRevenueServicesNet",
        }
    )
    # Pieces of that total. Kept when they name a member the total does not.
    # An undimensioned pair that adds up to the face total is stored as filed.
    # A fact override places the lines on the product axis.
    _COMPONENT_REVENUE_CONCEPTS = frozenset(
        {
            "us-gaap:AdvertisingRevenue",
            "us-gaap:OtherSalesRevenueNet",
        }
    )

    def __init__(self):
        """Initialize the parser."""
        self.fiscal_focus: tuple[Optional[int], Optional[int]] = (None, None)

    @staticmethod
    def _is_nullish(value: object) -> bool:
        """Return True when a scalar is None/NaN/pandas NA.

        This is safe for non-numeric values (unlike calling math.isnan directly).
        """
        if value is None:
            return True
        try:
            is_na = math.isnan(value)
        except Exception:
            return True
        return is_na

    def _print_sign(self, row: object) -> Optional[Decimal]:
        """Return the presentation-linkbase sign without changing the value.

        edgartools names this ``preferred_sign``. It is a print instruction
        (+1 or -1). The instance amount stays in ``value``.
        """
        getter = getattr(row, "get", None)
        sign = getter("preferred_sign") if getter else None
        if self._is_nullish(sign):
            return None
        try:
            return self._to_decimal(sign)
        except (ValueError, TypeError):
            logger.warning(f"Invalid preferred_sign: {sign}")
            return None

    @staticmethod
    def _to_decimal(value: object) -> Decimal:
        """Convert a statement amount to Decimal.

        edgartools returns binary floats, so 54.65 can arrive as
        54.650000000000006. The fact model allows five decimal places.

        Args:
            value: Raw amount from a statement row.

        Returns:
            The amount rounded to five decimal places when it has more.

        Raises:
            ValueError: ``value`` is not a number.
            TypeError: ``value`` cannot be converted to a string.
        """
        parsed = Decimal(str(value))
        exponent = parsed.as_tuple().exponent
        if isinstance(exponent, int) and exponent < -5:
            return parsed.quantize(Decimal("0.00001"))
        return parsed

    def parse_filing(self, filing: Filing) -> list[FinancialFactCreate]:
        """Parse an XBRL filing and extract financial facts.

        Args:
            filing: The edgartools Filing object

        Returns:
            List of FinancialFact objects
        """
        self.fiscal_focus = (None, None)
        try:
            # Get XBRL data from the filing
            xbrl = filing.xbrl()
            self.fiscal_focus = fiscal_focus(getattr(xbrl, "entity_info", None))

            facts = []

            # Parse income statement
            income_facts = self._parse_statement(
                xbrl.statements.income_statement().to_dataframe(
                    include_unit=True,
                    include_point_in_time=True,
                    presentation=False,
                ),
                "Income Statement",
                include_dimensions=True,
            )
            facts.extend(income_facts)
            facts.extend(self._supplement_segment_dimensions(xbrl, income_facts))

            # Parse balance sheet
            balance_facts = self._parse_statement(
                xbrl.statements.balance_sheet().to_dataframe(
                    include_unit=True,
                    include_point_in_time=True,
                    presentation=False,
                ),
                "Balance Sheet",
                include_dimensions=False,
            )
            facts.extend(balance_facts)

            # Parse cash flow statement
            cashflow_facts = self._parse_statement(
                xbrl.statements.cashflow_statement().to_dataframe(
                    include_unit=True,
                    include_point_in_time=True,
                    presentation=False,
                ),
                "Cash Flow Statement",
                include_dimensions=False,
            )
            facts.extend(cashflow_facts)

            # Parse comprehensive income
            try:
                comprehensive_income_facts = self._parse_statement(
                    xbrl.statements.comprehensive_income().to_dataframe(
                        include_unit=True,
                        include_point_in_time=True,
                        presentation=False,
                    ),
                    "Comprehensive Income",
                    include_dimensions=False,
                )
                facts.extend(comprehensive_income_facts)
            except Exception:
                logger.exception("Error parsing comprehensive income")

            # Parse statement of shareholders equity
            try:
                equity_facts = self._parse_statement(
                    xbrl.statements.statement_of_equity().to_dataframe(
                        include_unit=True,
                        include_point_in_time=True,
                        presentation=False,
                    ),
                    "Statement of Equity",
                    include_dimensions=False,
                )
                facts.extend(equity_facts)
            except Exception:
                logger.exception("Error parsing statement of equity")

            logger.info(
                f"Parsed {len(facts)} financial facts from filing {filing.accession_number}"
            )
            return facts

        except Exception as e:
            logger.error(f"Error parsing filing {filing.accession_number}: {e}")
            return []

    def _supplement_segment_dimensions(
        self,
        xbrl,
        income_facts: list[FinancialFactCreate],
    ) -> list[FinancialFactCreate]:
        """Add segment members that the face income statement did not carry.

        Some filings tag market, geography, and reportable-segment amounts only
        in a note. The face statement then has the total and none of the
        members, which leaves a one-year hole in every series. A disclosure row
        is kept when it sits on a segment axis and either reuses an
        income-statement concept or label, or tags the revenue line with a
        different revenue concept than the face. A company extension whose
        local name matches a standard revenue concept, ignoring
        capitalization, is that concept. A company extension axis is
        kept on the same terms when its total-revenue members for a period
        equal the face revenue for that period. The revenue concept and label
        are rewritten to the face line so the member joins that series. A
        member the face already reports is not added again. Other sales tagged
        on a segment member that already carries the total is the residual of
        that segment, so it is stored as its own member. Undimensioned
        advertising and other-revenue lines that add up to the face total are
        kept as filed, with no axis, so a fact override can place them on the
        product axis. An undimensioned row is otherwise kept only when it is
        the plug between the face total and one axis of members for that
        period. A repeated total, and any other note line, stays out of the
        income statement.

        Args:
            xbrl: XBRL object from edgartools
            income_facts: Facts already parsed from the face income statement

        Returns:
            Facts to add alongside the face statement
        """
        face_concepts = {fact.concept for fact in income_facts if not fact.axis}
        face_labels = {
            fact.label.strip().lower()
            for fact in income_facts
            if not fact.axis and fact.label
        }
        if not face_concepts and not face_labels:
            return []

        face_revenue = self._face_revenue_fact(income_facts)
        revenue_concepts = (
            self._TOTAL_REVENUE_CONCEPTS | self._COMPONENT_REVENUE_CONCEPTS
        )
        seen: set[tuple] = set()
        for fact in income_facts:
            if not fact.axis:
                continue
            seen.add((fact.concept, fact.axis, fact.member, fact.period_end))
            # A member already reported under another revenue concept is the
            # same member once both are rewritten onto the face revenue line.
            if (
                face_revenue is not None
                and fact.concept in revenue_concepts
                and fact.concept != face_revenue.concept
            ):
                seen.add(
                    (
                        face_revenue.concept,
                        fact.axis,
                        fact.member,
                        fact.period_end,
                    )
                )
        face_revenue_by_period = {
            (fact.period_end, fact.period): fact.value
            for fact in income_facts
            if face_revenue is not None
            and not fact.axis
            and not fact.is_abstract
            and fact.concept == face_revenue.concept
            and fact.value is not None
        }
        added: dict[tuple, FinancialFactCreate] = {}
        # Concept each added fact had before it was rewritten onto the face line.
        # A revenue total replaces a component that claimed the same member first.
        origin: dict[tuple, str] = {}
        try:
            available = xbrl.statements.list_available()
        except Exception:
            logger.exception("Could not list statements for segment disclosures")
            return []

        for _, row in available.iterrows():
            if str(row.get("category", "")) != "disclosure":
                continue
            name = str(row.get("name", ""))
            if not self._is_segment_disclosure(name):
                continue
            role = row.get("role_name")
            if not role or (isinstance(role, float) and math.isnan(role)):
                continue
            try:
                statement = xbrl.statements.get(role)
                if statement is None:
                    continue
                frame = statement.to_dataframe(
                    include_unit=True,
                    include_point_in_time=True,
                    presentation=False,
                )
            except Exception:
                logger.exception("Could not read segment disclosure %s", role)
                continue

            parsed = self._parse_statement(
                frame, "Income Statement", include_dimensions=True
            )
            undimensioned = []
            before = set(added)
            for fact in parsed:
                if not fact.axis:
                    undimensioned.append(fact)
                    continue
                if fact.axis not in self._SEGMENT_AXES:
                    continue
                self._adopt_segment_fact(
                    fact,
                    face_concepts,
                    face_labels,
                    face_revenue,
                    seen,
                    added,
                    origin,
                )
            for fact in self._extension_revenue_members(parsed, face_revenue_by_period):
                self._adopt_segment_fact(
                    fact,
                    face_concepts,
                    face_labels,
                    face_revenue,
                    seen,
                    added,
                    origin,
                )
            self._keep_revenue_components(
                undimensioned,
                income_facts,
                face_revenue,
                face_revenue_by_period,
                added,
            )
            members = [
                fact
                for fact in income_facts
                if face_revenue is not None
                and fact.axis
                and fact.concept == face_revenue.concept
                and isinstance(fact.value, Decimal)
            ]
            members.extend(
                added[key]
                for key in added.keys() - before
                if face_revenue is not None
                and added[key].concept == face_revenue.concept
            )
            for fact in self._undimensioned_plugs(
                undimensioned, members, face_revenue_by_period
            ):
                key = (fact.concept, fact.period_end, fact.period)
                if key not in added:
                    added[key] = fact

        if added:
            logger.info(
                "Added %s segment-disclosure facts missing from the income statement",
                len(added),
            )
        return list(added.values())

    @classmethod
    def _is_segment_disclosure(cls, name: str) -> bool:
        """Return True for a note that disaggregates revenue or segments.

        Args:
            name: Statement name from the filing's presentation linkbase.

        Returns:
            True when the note is a segment, geography, or revenue-source table.
        """
        if "Discontinued" in name:
            return False
        return any(token in name for token in ("Segment", "Geographic", "Revenue"))

    @staticmethod
    def _undimensioned_plugs(
        undimensioned: list[FinancialFactCreate],
        members: list[FinancialFactCreate],
        face_revenue_by_period: dict[tuple, object],
    ) -> list[FinancialFactCreate]:
        """Return note rows that close one axis to the face total.

        Args:
            undimensioned: Disclosure rows with no axis.
            members: Revenue members kept from the same note.
            face_revenue_by_period: Face revenue by period end and period type.

        Returns:
            The reconciling rows. Totals and unrelated note lines are omitted.
        """
        sums: dict[tuple, object] = {}
        for member in members:
            if not isinstance(member.value, Decimal):
                continue
            key = (member.period_end, member.period, member.axis)
            sums[key] = sums.get(key, 0) + member.value

        kept = []
        for fact in undimensioned:
            if fact.is_abstract or fact.value is None or fact.value == 0:
                continue
            face_value = face_revenue_by_period.get((fact.period_end, fact.period))
            if face_value is None:
                continue
            closes_an_axis = any(
                face_value - total == fact.value
                for (period_end, period, _axis), total in sums.items()
                if period_end == fact.period_end and period == fact.period
            )
            if closes_an_axis:
                kept.append(fact)
        return kept

    @classmethod
    def _is_total_revenue_concept(cls, concept: str) -> bool:
        """Return True when ``concept`` is a total-revenue line.

        A company extension that copies a standard revenue concept and only
        changes the local-name capitalization is the same line. S&P Global
        tags the ASC 606 total as
        ``spgi:RevenuefromContractwithCustomerExcludingAssessedTax``.

        Args:
            concept: SEC concept, ``prefix:LocalName``.

        Returns:
            True for a standard total or a capitalization variant of one.
        """
        if concept in cls._TOTAL_REVENUE_CONCEPTS:
            return True
        local = concept.split(":")[-1].casefold()
        return any(
            known.split(":")[-1].casefold() == local
            for known in cls._TOTAL_REVENUE_CONCEPTS
        )

    def _face_revenue_fact(self, income_facts: list) -> Optional[FinancialFactCreate]:
        """Return the undimensioned revenue total on the face statement.

        Args:
            income_facts: Facts parsed from the face income statement.

        Returns:
            The revenue fact with the largest absolute value, or None.
        """
        candidates = [
            fact
            for fact in income_facts
            if not fact.axis
            and fact.concept in self._TOTAL_REVENUE_CONCEPTS
            and fact.value is not None
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda fact: abs(fact.value))

    def _keep_revenue_components(
        self,
        undimensioned: list[FinancialFactCreate],
        income_facts: list[FinancialFactCreate],
        face_revenue: Optional[FinancialFactCreate],
        face_revenue_by_period: dict[tuple, object],
        added: dict,
    ) -> None:
        """Keep undimensioned advertising and other-revenue lines from a note.

        The lines stay on their filed concepts, with no axis. A fact override
        places them on the product axis. A line is kept only when the
        components for that period add up to the face total and that period
        has no product split yet.

        Args:
            undimensioned: Disclosure rows with no axis.
            income_facts: Facts already parsed from the face income statement.
            face_revenue: Undimensioned revenue total, when the face has one.
            face_revenue_by_period: Face revenue by period end and period type.
            added: Facts accepted so far.
        """
        if face_revenue is None:
            return

        product_axes = {
            "srt:ProductOrServiceAxis",
            "us-gaap:ProductOrServiceAxis",
        }
        grouped: dict[tuple, list[FinancialFactCreate]] = {}
        for fact in undimensioned:
            if fact.is_abstract or fact.concept not in self._COMPONENT_REVENUE_CONCEPTS:
                continue
            if not isinstance(fact.value, Decimal) or fact.value == 0:
                continue
            grouped.setdefault((fact.period_end, fact.period), []).append(fact)

        for key, components in grouped.items():
            if len(components) < 2:
                continue
            face_value = face_revenue_by_period.get(key)
            if not isinstance(face_value, Decimal):
                continue
            if sum((component.value for component in components), Decimal("0")) != (
                face_value
            ):
                continue
            period_end, period = key
            already_split = any(
                fact.period_end == period_end
                and fact.period == period
                and fact.concept == face_revenue.concept
                and fact.axis in product_axes
                and isinstance(fact.value, Decimal)
                for fact in list(income_facts) + list(added.values())
            )
            if already_split:
                continue
            for component in components:
                stored_key = (component.concept, component.period_end, component.period)
                if stored_key not in added:
                    added[stored_key] = component

    def _extension_revenue_members(
        self,
        facts: list[FinancialFactCreate],
        face_revenue_by_period: dict[tuple, object],
    ) -> list[FinancialFactCreate]:
        """Return extension-axis revenue members that equal face revenue.

        ``_SEGMENT_AXES`` is the standard product, geography, and segment
        axes. A filer often reports the same split on its own axis, and only
        in a note, after the face concept changes. Those members are kept
        when, for one axis and period, the total-revenue amounts add up to
        the undimensioned face revenue. An axis that falls short is left out,
        which drops partial country lists, customers, and asset breakdowns.

        Args:
            facts: Facts parsed from one disclosure.
            face_revenue_by_period: Face revenue by period end and period type.

        Returns:
            Members of each extension axis and period that adds up.
        """
        grouped: dict[tuple, list[FinancialFactCreate]] = {}
        for fact in facts:
            if not fact.axis or fact.axis in self._SEGMENT_AXES:
                continue
            if fact.is_abstract or not self._is_total_revenue_concept(fact.concept):
                continue
            if not isinstance(fact.value, Decimal) or fact.period_end is None:
                continue
            grouped.setdefault((fact.axis, fact.period_end, fact.period), []).append(
                fact
            )

        kept: list[FinancialFactCreate] = []
        for (_axis, period_end, period), members in grouped.items():
            face_value = face_revenue_by_period.get((period_end, period))
            if not isinstance(face_value, Decimal) or len(members) < 2:
                continue
            total = sum((member.value for member in members), Decimal("0"))
            if total == face_value:
                kept.extend(members)
        return kept

    def _adopt_segment_fact(
        self,
        fact: FinancialFactCreate,
        face_concepts: set[str],
        face_labels: set[str],
        face_revenue: Optional[FinancialFactCreate],
        seen: set[tuple],
        added: dict,
        origin: dict,
    ) -> None:
        """Keep a disclosure fact that extends an income-statement line.

        Args:
            fact: Dimension fact parsed from a disclosure.
            face_concepts: Concepts on the undimensioned face statement.
            face_labels: Lower-cased labels on the undimensioned face statement.
            face_revenue: Undimensioned revenue total, when the face has one.
            seen: Concept, axis, member, and period already reported on the face.
            added: Facts accepted so far, keyed by concept, axis, member, and period.
            origin: Original concept of each accepted fact, before rewriting.
        """
        original_concept = fact.concept
        is_revenue = (
            self._is_total_revenue_concept(original_concept)
            or original_concept in self._COMPONENT_REVENUE_CONCEPTS
        )
        rewritten = (
            face_revenue is not None
            and is_revenue
            and original_concept not in face_concepts
        )
        if rewritten and face_revenue is not None:
            fact.concept = face_revenue.concept
            fact.label = face_revenue.label
        else:
            label = fact.label.strip().lower() if fact.label else ""
            if fact.concept not in face_concepts and label not in face_labels:
                return

        key = (fact.concept, fact.axis, fact.member, fact.period_end)
        if key in seen or key in added:
            # Other sales tagged on the segment total is that segment's
            # residual, not a second copy of the total.
            if original_concept != "us-gaap:OtherSalesRevenueNet":
                if (
                    key in added
                    and self._is_total_revenue_concept(original_concept)
                    and origin.get(key) in self._COMPONENT_REVENUE_CONCEPTS
                ):
                    added[key] = fact
                    origin[key] = original_concept
                return
            fact.member = "Other sales"
            fact.member_label = "Other sales"
            key = (fact.concept, fact.axis, fact.member, fact.period_end)
            if key in seen or key in added:
                return

        added[key] = fact
        origin[key] = original_concept

    @classmethod
    def _keeps_published_quarter(cls, fact: FinancialFactCreate) -> bool:
        """Return True when the face row should keep its printed quarter column.

        Revenue totals keep the quarter so a dimension split can match the
        printed total after a restatement. Per-share amounts keep it because
        they are not additive across sub-periods: subtracting year-to-date
        totals invents a false quarter after a stock split.

        Args:
            fact: Face fact already built from the latest period column.

        Returns:
            Whether a same-date quarter column should also be stored.
        """
        if fact.concept in cls._TOTAL_REVENUE_CONCEPTS:
            return True
        unit = (fact.unit or "").casefold()
        if unit in {"usdpershare", "usdeps"}:
            return True
        local = fact.concept.split(":")[-1]
        return local.endswith("PerShare") or local.startswith("EarningsPerShare")

    def _published_quarter_fact(
        self,
        row,
        statement_type: str,
        ytd_fact: FinancialFactCreate,
        quarter_period_col: str,
        quarter_comparative_col: Optional[str],
        latest_period_col: str,
    ) -> list[FinancialFactCreate]:
        """Keep a filing's published quarter beside the year-to-date total.

        The face statement otherwise stores only the year-to-date column, and
        the quarterly procedure reconstructs the quarter by subtraction. A
        restated comparative makes that residual miss the quarter the filing
        printed, which drops a dimension split that matches the printed total.
        Per-share amounts are not additive, so their printed quarter is kept
        for the same reason.

        Args:
            row: Undimensioned statement row.
            statement_type: Statement name.
            ytd_fact: Fact already created from the latest column.
            quarter_period_col: Column for the filing's current quarter.
            quarter_comparative_col: Prior-year column for that quarter.
            latest_period_col: Column used for ``ytd_fact``.

        Returns:
            The published quarter fact, or nothing when the row does not keep
            a printed quarter.
        """
        if (
            ytd_fact.is_abstract
            or ytd_fact.axis
            or not self._keeps_published_quarter(ytd_fact)
            or quarter_period_col == latest_period_col
        ):
            return []
        quarter_fact = self._create_financial_fact_with_hierarchy(
            row,
            statement_type,
            quarter_period_col,
            quarter_comparative_col,
            ytd_fact.position or 0,
        )
        if quarter_fact is None or quarter_fact.period != PeriodType.Q:
            return []
        quarter_fact.key = f"{quarter_fact.key}|q"
        return [quarter_fact]

    def _parse_statement(
        self,
        statement_df: pd.DataFrame,
        statement_type: str,
        include_dimensions: bool,
    ) -> list[FinancialFactCreate]:
        """Parse a financial statement dataframe and extract facts.

        Args:
            statement_df: DataFrame from edgartools statement
            statement_type: Type of statement (Income Statement, Balance Sheet, etc.)
            include_dimensions: If True, parse dimension facts. Pass True only for
                Income Statement; pass False for other statements.

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

            # Face rows keep the latest column. A year-to-date column sorts
            # after the quarter column of the same date, and the quarterly
            # procedure reduces that total. Dimension rows keep every column
            # of that shape, including prior years.
            latest_period_col = sorted(period_columns)[-1]
            comparative_period_col = self._extract_comparative_period_column(
                period_columns, latest_period_col
            )
            quarter_period_col = self._select_current_period_column(period_columns)
            quarter_comparative_col = self._extract_comparative_period_column(
                period_columns, quarter_period_col
            )
            ytd_period_col = self._same_date_ytd_column(
                period_columns, quarter_period_col
            )

            # Track global fact position (only for facts, not abstracts)
            position = 0

            base_df = statement_df[statement_df["dimension"] != True]  # noqa: E712
            dimensions_df = statement_df[
                statement_df["dimension"] == True  # noqa: E712
            ]

            # Map concept -> label from the base (non-dimension) facts so dimension facts
            # can reuse the same label and avoid "hierarchy" generation.
            base_label_by_concept: dict[str, str] = {}

            # Iterate through each row in the statement
            for _, row in base_df.iterrows():
                # Create financial fact with hierarchy context
                fact = self._create_financial_fact_with_hierarchy(
                    row,
                    statement_type,
                    latest_period_col,
                    comparative_period_col,
                    position,
                )

                if fact:
                    facts.append(fact)
                    if not fact.is_abstract:
                        base_label_by_concept.setdefault(fact.concept, fact.label)
                    position += 1
                    facts.extend(
                        self._published_quarter_fact(
                            row,
                            statement_type,
                            fact,
                            quarter_period_col,
                            quarter_comparative_col,
                            latest_period_col,
                        )
                    )

            # Handle dimension facts (rows where dimension=True); only for income statement
            if (
                include_dimensions
                and dimensions_df is not None
                and not dimensions_df.empty
            ):
                collapsed_members, ambiguous_suffixes = (
                    self._collapsed_dimension_members(dimensions_df)
                )
                for _, row in dimensions_df.iterrows():
                    facts.extend(
                        self._dimension_period_facts(
                            row,
                            statement_type,
                            period_columns,
                            quarter_period_col,
                            ytd_period_col,
                            base_label_by_concept,
                            collapsed_members,
                            ambiguous_suffixes,
                        )
                    )

        except Exception:
            logger.exception(f"Error parsing {statement_type}")

        return facts

    def _dimension_period_facts(
        self,
        row,
        statement_type: str,
        period_columns: list[str],
        quarter_period_col: str,
        ytd_period_col: Optional[str],
        base_label_by_concept: dict[str, str],
        collapsed_members: set[str],
        ambiguous_suffixes: set[str],
    ) -> list[FinancialFactCreate]:
        """Emit one fact for every populated column of this dimension row.

        A 10-K prints two or three years, and a 10-Q prints the prior-year
        quarter beside the current one. Keeping only the latest column leaves
        the earlier periods out of the series. Each column becomes its own
        fact. The closest earlier column of the same shape stays the
        comparative, for rename chaining. A year-to-date column beside a
        quarter of the same date keeps the ``|ytd`` key suffix.

        Args:
            row: Dimension row from the statement frame.
            statement_type: Statement name.
            period_columns: Period columns that contain values.
            quarter_period_col: Column for the filing's current period.
            ytd_period_col: Year-to-date column ending on that date, if any.
            base_label_by_concept: Face label for each concept.
            collapsed_members: Member ids reused for several children.
            ambiguous_suffixes: Suffixes shared by more than one reused parent.

        Returns:
            Facts for the columns that have a value.
        """
        # Section headers are not period amounts. numpy bools compare equal
        # to True and are not the same object.
        if row.get("abstract") == True:  # noqa: E712
            fact = self._create_dimension_fact(
                row=row,
                statement_type=statement_type,
                period_col=quarter_period_col,
                comparative_period_col=None,
                base_label_by_concept=base_label_by_concept,
                collapsed_members=collapsed_members,
                ambiguous_suffixes=ambiguous_suffixes,
            )
            return [fact] if fact else []

        current_columns = self._columns_of_same_shape(
            period_columns, quarter_period_col
        )
        ytd_columns: list[str] = []
        if ytd_period_col and ytd_period_col not in current_columns:
            ytd_columns = self._columns_of_same_shape(period_columns, ytd_period_col)

        facts: list[FinancialFactCreate] = []
        quarter_ends = set()
        for column in current_columns:
            fact = self._create_dimension_fact(
                row=row,
                statement_type=statement_type,
                period_col=column,
                comparative_period_col=self._prior_period_column(
                    current_columns, column
                ),
                base_label_by_concept=base_label_by_concept,
                collapsed_members=collapsed_members,
                ambiguous_suffixes=ambiguous_suffixes,
            )
            if fact is None:
                continue
            if fact.period == PeriodType.Q:
                quarter_ends.add(fact.period_end)
            facts.append(fact)

        for column in ytd_columns:
            fact = self._create_dimension_fact(
                row=row,
                statement_type=statement_type,
                period_col=column,
                comparative_period_col=self._prior_period_column(ytd_columns, column),
                base_label_by_concept=base_label_by_concept,
                collapsed_members=collapsed_members,
                ambiguous_suffixes=ambiguous_suffixes,
            )
            if fact is None or fact.period != PeriodType.YTD:
                continue
            if fact.period_end in quarter_ends:
                fact.key = f"{fact.key}|ytd"
            facts.append(fact)
        return facts

    @classmethod
    def _column_shape(cls, column: str) -> str:
        """Return the period token after the date, such as ``q`` or ``fy``.

        Every quarter is ``q``: edgartools numbers quarters from the calendar
        for some filers, so a fiscal Q1 can sit beside a prior-year "Q2".

        Args:
            column: A period column name.

        Returns:
            The lowercased token, or an empty string when the column is a date.
        """
        parts = column.split(" ", 1)
        if len(parts) < 2:
            return ""
        if cls._is_quarter_column(column):
            return "q"
        return parts[1].lower()

    @staticmethod
    def _columns_of_same_shape(period_columns: list[str], exemplar: str) -> list[str]:
        """Return columns with the same period token as exemplar.

        A Q3 column matches the prior-year Q3, not the YTD beside it and not
        a Q1 in the same frame.

        Args:
            period_columns: Period column names.
            exemplar: Column whose token to match.

        Returns:
            Matching columns, oldest first.
        """
        shape = SECXBRLParser._column_shape(exemplar)
        matched = [
            column
            for column in period_columns
            if SECXBRLParser._column_shape(column) == shape
        ]
        return sorted(matched, key=lambda column: column.split(" ")[0])

    @staticmethod
    def _prior_period_column(columns: list[str], column: str) -> Optional[str]:
        """Return the closest earlier column of the same shape.

        Args:
            columns: Columns sorted oldest first, one shape only.
            column: The column whose comparative is needed.

        Returns:
            The prior column, or None for the oldest.
        """
        end = column.split(" ")[0]
        earlier = [candidate for candidate in columns if candidate.split(" ")[0] < end]
        if not earlier:
            return None
        return earlier[-1]

    def _collapsed_dimension_members(
        self, dimensions_df: pd.DataFrame
    ) -> tuple[set[str], set[str]]:
        """Return reused member ids and suffixes those members share.

        Args:
            dimensions_df: Statement rows with ``dimension`` true.

        Returns:
            Member ids, in ``prefix:Name`` form, whose ``"{label} - "`` suffix
            is not unique, and suffixes that must keep their parent. A suffix
            keeps its parent when more than one reused member claims it, or
            when the same statement has that name as its own member. Otherwise
            the elimination of Subscription lands on Subscription.
        """
        suffixes: dict[str, set[str]] = {}
        own_labels: set[str] = set()
        if dimensions_df is None or dimensions_df.empty:
            return set(), set()

        for _, row in dimensions_df.iterrows():
            raw_member = row.get("dimension_member")
            member_label = row.get("dimension_member_label")
            label = row.get("label")
            if raw_member is None or not isinstance(member_label, str):
                continue
            if not isinstance(label, str):
                continue
            prefix = f"{member_label} - "
            if not label.startswith(prefix):
                own_labels.add(label.strip())
                continue
            suffix = label[len(prefix) :].strip()
            if not suffix:
                continue
            member = str(raw_member).replace("_", ":")
            suffixes.setdefault(member, set()).add(suffix)

        collapsed = {member for member, found in suffixes.items() if len(found) > 1}
        parents_by_suffix: dict[str, set[str]] = {}
        for member, found in suffixes.items():
            if member not in collapsed:
                continue
            for suffix in found:
                parents_by_suffix.setdefault(suffix, set()).add(member)
        ambiguous = {
            suffix
            for suffix, parents in parents_by_suffix.items()
            if len(parents) > 1 or suffix in own_labels
        }
        return collapsed, ambiguous

    def _create_dimension_fact(
        self,
        row,
        statement_type: str,
        period_col: str,
        comparative_period_col: Optional[str],
        base_label_by_concept: dict[str, str],
        collapsed_members: Optional[set[str]] = None,
        ambiguous_suffixes: Optional[set[str]] = None,
    ) -> Optional[FinancialFactCreate]:
        """Create a FinancialFact from a dimension row (dimension=True).

        Dimension facts must not create hierarchy (no parent/abstract keys) and must
        reuse the label of the corresponding base fact.

        Args:
            row: DataFrame row from statement (dimension=True)
            statement_type: Type of financial statement
            period_col: Latest period column name (e.g. "2025-06-28 (Q2)")
            comparative_period_col: Comparative period column name (if any)
            base_label_by_concept: Mapping from SEC concept -> base fact label
            collapsed_members: Member ids reused for several children. Their
                label suffix replaces the member.
            ambiguous_suffixes: Suffixes shared by more than one reused parent.
                Those keep the full label so the parents stay distinct.

        Returns:
            FinancialFactCreate or None if invalid
        """
        try:
            concept = self._to_sec_concept(row.get("concept", ""))
            if not concept:
                return None

            axis = row.get("dimension_axis")
            member = row.get("dimension_member").replace("_", ":")
            member_label = row.get("dimension_member_label")

            # edgartools stamps the parent member on every child and puts the
            # child name after "{parent label} - ". Rewrite only those reused
            # member ids. A distinct member labeled "{member} - {group}"
            # already names itself; the suffix is the group, not a new member.
            label_text = row.get("label")
            prefix = f"{member_label} - " if isinstance(member_label, str) else None
            if (
                collapsed_members
                and member in collapsed_members
                and prefix
                and isinstance(label_text, str)
                and label_text.startswith(prefix)
            ):
                suffix = label_text[len(prefix) :].strip()
                if ambiguous_suffixes and suffix in ambiguous_suffixes:
                    member = label_text.strip()
                    member_label = member
                else:
                    member = label_text[len(prefix) :]
                    member_label = member

            if not axis or member is None:
                return None

            is_abstract = row.get("abstract")
            label = base_label_by_concept.get(concept, row.get("label", concept))
            unit = row.get("unit", "usd" if not is_abstract else None)

            value = row.get(period_col)
            comparative_value = (
                row.get(comparative_period_col) if comparative_period_col else None
            )
            weight = row.get("weight")

            # Skip facts without a value
            if not is_abstract and self._is_nullish(value):
                return None

            # Convert value to Decimal
            value_decimal = None
            if not is_abstract and not self._is_nullish(value):
                try:
                    value_decimal = self._to_decimal(value)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid value for concept {concept}: {value}")
                    return None

            # Convert comparative value to Decimal
            comparative_value_decimal = None
            if not is_abstract and not self._is_nullish(comparative_value):
                try:
                    comparative_value_decimal = self._to_decimal(comparative_value)
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid comparative value for concept {concept}: {comparative_value}"
                    )

            weight_decimal = None
            if not is_abstract and not self._is_nullish(weight):
                try:
                    weight_decimal = self._to_decimal(weight)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid weight for concept {concept}: {weight}")

            period_end = self._parse_date(period_col.split(" ")[0])
            comparative_period_end = (
                self._parse_date(comparative_period_col.split(" ")[0])
                if comparative_period_col
                else None
            )
            period = self._determine_period_type_from_column(
                period_col,
                statement_type,
                point_in_time=row.get("point_in_time"),
            )

            period_end_str = period_end.isoformat() if period_end else ""
            fact_key = str(
                uuid.uuid5(
                    uuid.NAMESPACE_OID,
                    f"{concept}|{statement_type}|{period_end_str}|{axis}|{member}|{member_label}",
                )
            )

            return FinancialFactCreate(
                key=fact_key,
                parent_key=None,
                abstract_key=None,
                company_id=0,
                filing_id=0,
                form_type="",
                concept=concept,
                label=label,
                value=value_decimal,
                is_abstract=is_abstract,
                comparative_value=comparative_value_decimal,
                weight=weight_decimal,
                print_sign=self._print_sign(row),
                unit=unit,
                axis=axis,
                member=member,
                member_label=member_label,
                statement=statement_type,
                period_end=period_end,
                comparative_period_end=comparative_period_end,
                period=period,
                position=None,
            )
        except Exception:
            logger.exception(f"Error creating dimension fact from row {row}")
            return None

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

        try:
            # Query disaggregated metrics by product/service
            product_facts = self._process_disaggregated_axis(
                xbrl, metric, "ProductOrServiceAxis"
            )
            facts.extend(product_facts)

            # Query disaggregated metrics by geographic region
            geographic_facts = self._process_disaggregated_axis(
                xbrl, metric, "StatementGeographicalAxis", base_position=len(facts)
            )
            facts.extend(geographic_facts)

            # Query disaggregated metrics by business segments
            business_segments_facts = self._process_disaggregated_axis(
                xbrl, metric, "StatementBusinessSegmentsAxis", base_position=len(facts)
            )
            facts.extend(business_segments_facts)

            logger.info(f"Extracted {len(facts)} disaggregated {metric} facts")

        except Exception:
            logger.exception(f"Error parsing disaggregated {metric}")

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

    def _parse_disaggregated_cogs(self, xbrl) -> list[FinancialFactCreate]:
        """Parse disaggregated cost of goods sold using XBRL queries.

        Args:
            xbrl: XBRL object from edgartools

        Returns:
            List of FinancialFact objects for disaggregated cost of goods sold
        """
        return self._parse_disaggregated_metrics(xbrl, "CostOfGoods")

    def _process_disaggregated_axis(
        self,
        xbrl,
        metric: str,
        axis_name: str,
        base_position: int = 0,
    ) -> list:
        """Process disaggregated metrics for a given axis dimension.

        Args:
            xbrl: XBRL object from edgartools
            metric: The metric being disaggregated
            axis_name: Name of the axis dimension (e.g., "ProductOrServiceAxis")
            base_position: Base position for facts in this axis (default: 0)

        Returns:
            List of facts for this axis
        """
        facts = []

        # Query disaggregated metrics by axis
        df = xbrl.query().by_concept(metric).by_dimension(axis_name).to_dataframe()

        if (
            df is None
            or df.empty
            or "period_start" not in df.columns
            or "period_end" not in df.columns
        ):
            return facts

        # Find the column containing the axis
        dim_col = self._find_dim_column(df, axis_name)

        # If an axis member is associated with multiple different values in another
        # dimension column (dim_*), ignore those "multi-dimension" rows and keep only
        # the rows where the other dim_* column is NaN.
        other_dim_cols = [
            col for col in df.columns if col.startswith("dim_") and col != dim_col
        ]
        for other_dim_col in other_dim_cols:
            non_null_nunique = df.groupby(dim_col)[other_dim_col].transform(
                lambda s: s.dropna().nunique()
            )
            ambiguous_member = non_null_nunique >= 2
            df = df[~ambiguous_member | df[other_dim_col].isna()]

        # Handle members that appear more than once for the same period_key.
        # If a (member, period_key) combination is duplicated:
        # - keep it if the duplicated rows have the same value (keep one row)
        # - drop it if the duplicated rows disagree on value (drop all rows in group)
        if "period_key" in df.columns:
            group_cols = [dim_col, "period_key"]

            num_value_col = "numeric_value" if "numeric_value" in df.columns else None
            str_value_col = "value" if "value" in df.columns else None
            value_col = num_value_col or str_value_col

            if value_col is None:
                # Without a value column, duplicated groups are ambiguous → drop all.
                df = df[~df.duplicated(subset=group_cols, keep=False)]
            else:
                duplicated_group = df.duplicated(subset=group_cols, keep=False)
                nunique_by_group = df.groupby(group_cols)[value_col].transform(
                    lambda s: s.nunique(dropna=False)
                )
                inconsistent_group = duplicated_group & (nunique_by_group > 1)
                df = df[~inconsistent_group]
                # For remaining duplicates with identical values, keep one row.
                df = df.drop_duplicates(subset=group_cols, keep="first")

        # Get unique values from the axis column
        members = df[dim_col].unique()

        position = base_position

        # Iterate over each unique value
        for member in members:
            # Get the first two rows matching this value, sorted by period_start desc
            member_rows = (
                df[df[dim_col] == member]
                .sort_values("period_start", ascending=False)
                .head(2)
            )

            # Derive position from base position and current facts list length
            position += 1
            # Extract the dimension name from the column
            dimension = self._dim_from_column(dim_col)

            fact = self._create_disaggregated_metric_fact(
                row=member_rows.iloc[0],
                comparative_row=(member_rows.iloc[1] if len(member_rows) > 1 else None),
                metric=metric,
                dimension=dimension,
                form_type=getattr(xbrl, "document_type", "") or "",
                position=position,
            )
            if fact:
                facts.append(fact)

        return facts

    def _create_disaggregated_metric_fact(
        self,
        row,
        comparative_row,
        metric: str,
        dimension: str,
        form_type: str,
        position: int,
    ) -> Optional[FinancialFactCreate]:
        """Create a FinancialFact for disaggregated metrics.

        Args:
            row: DataFrame row from XBRL query
            metric: The metric being disaggregated (e.g., "Revenue", "OperatingIncome")
            dimension: The dimension axis
            position: Global position of this fact

        Returns:
            FinancialFact object or None if invalid
        """
        try:
            # Extract basic information
            concept = row.get("concept", metric)
            value = row.get("numeric_value", row.get("value"))
            weight = row.get("weight")
            unit = row.get("unit", "usd")

            # Get the label from the XBRL data
            label = row.get("label", concept)

            axis = dimension

            member = row.get(self._to_df_dim(dimension), "")
            member_label = row.get("dimension_member_label") or member

            # Skip if no value
            if self._is_nullish(value):
                return None

            # Convert value to Decimal
            try:
                value_decimal = self._to_decimal(value)
            except (ValueError, TypeError):
                logger.warning(f"Invalid value for disaggregated {metric}: {value}")
                return None

            # Convert comparative value to Decimal
            comparative_value_decimal = None
            comparative_value = (
                comparative_row.get("numeric_value")
                if comparative_row is not None
                else None
            )
            if not self._is_nullish(comparative_value):
                try:
                    comparative_value_decimal = self._to_decimal(comparative_value)
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid comparative value for disaggregated {metric}: {comparative_value}"
                    )

            weight_decimal = None
            if not self._is_nullish(weight):
                try:
                    weight_decimal = self._to_decimal(weight)
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid weight for disaggregated {metric}: {weight}"
                    )

            # Parse dates
            period_end = self._parse_date(row.get("period_end"))
            comparative_period_end = (
                self._parse_date(comparative_row.get("period_end"))
                if comparative_row is not None
                else None
            )

            if period_end is None:
                logger.warning(
                    f"Invalid period end for disaggregated {metric}, {dimension} fact: {row.get('period_end')}"
                )
                return None

            # Determine period type based on dates
            period_start = self._parse_date(row.get("period_start"))
            period = self._determine_period_type(period_start, period_end, form_type)

            # Create the financial fact
            fact = FinancialFactCreate(
                key=str(uuid.uuid4()),
                parent_key=None,
                abstract_key=None,
                company_id=0,
                filing_id=0,
                form_type="",
                concept=concept,
                label=label,
                is_abstract=False,
                value=value_decimal,
                weight=weight_decimal,
                print_sign=self._print_sign(row),
                comparative_value=comparative_value_decimal,
                unit=unit,
                axis=axis,
                member=member,
                member_label=member_label,
                statement="Income Statement",
                period_end=period_end,
                comparative_period_end=comparative_period_end,
                period=period,
                position=position,
            )

            return fact

        except Exception:
            logger.exception(f"Error creating disaggregated {metric}, {dimension} fact")
            return None

    def _determine_period_type(
        self, period_start: Optional[date], period_end: Optional[date], form_type: str
    ) -> PeriodType:
        """Determine period type based on start and end dates.

        Args:
            period_start: Start date of the period
            period_end: End date of the period

        Returns:
            PeriodType.YTD if it's year-to-date, PeriodType.Q if it's a quarter
        """
        if form_type == "10-K" or form_type == "10-K/A":
            return PeriodType.YTD

        if not period_start or not period_end:
            # Default to Q if we can't determine
            return PeriodType.Q

        # Calculate the number of days in the period
        days_in_period = (period_end - period_start).days

        # If the period is approximately 3 months (90 days +/- 10 days), it's a quarter
        if days_in_period <= 100:
            return PeriodType.Q
        # If the period is longer (like 6-12 months), it's likely YTD
        else:
            return PeriodType.YTD

    def _determine_period_type_from_column(
        self,
        period_col: str,
        statement_type: str,
        point_in_time: Optional[bool] = None,
    ) -> Optional[PeriodType]:
        """Determine period type based on the period column name.

        Instant (point-in-time) XBRL facts are stored with period=NULL regardless
        of statement. Duration facts on non-balance-sheet statements use the
        column label (Q* → Q, otherwise YTD).

        edgartools ``include_point_in_time`` is True for instant and False for
        duration (fixed in 5.58.0+ for instant facts under duration columns).

        Args:
            period_col: The period column name (e.g., "2025-06-28 (Q2)", "2025-12-31")
            statement_type: Financial statement name
            point_in_time: From edgartools ``include_point_in_time``

        Returns:
            PeriodType.Q if it's a quarter, PeriodType.YTD if it's year-to-date,
            None for balance-sheet / instant facts
        """
        # Normalize pandas/numpy bools; leave missing as None.
        if point_in_time is True or point_in_time is False:
            is_point_in_time: Optional[bool] = bool(point_in_time)
        elif point_in_time is None:
            is_point_in_time = None
        else:
            try:
                if point_in_time != point_in_time:  # NaN
                    is_point_in_time = None
                else:
                    is_point_in_time = bool(point_in_time)
            except (TypeError, ValueError):
                is_point_in_time = None

        if is_point_in_time is True:
            return None

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

        # Duration facts without a quarter marker default to YTD
        return PeriodType.YTD

    def _select_current_period_column(self, period_columns: list[str]) -> str:
        """Choose the column for the filing's current period.

        A quarter column and a year-to-date column often share an end date,
        and the year-to-date name sorts last. The quarter is the period the
        column reports.

        Args:
            period_columns: Period column names that contain values.

        Returns:
            The quarter column for the latest end date, or the latest column.
        """
        latest_end = max(column.split(" ")[0] for column in period_columns)
        current = [
            column for column in period_columns if column.split(" ")[0] == latest_end
        ]
        quarters = [column for column in current if self._is_quarter_column(column)]
        if quarters:
            return sorted(quarters)[-1]
        return sorted(current)[-1]

    def _same_date_ytd_column(
        self, period_columns: list[str], quarter_column: str
    ) -> Optional[str]:
        """Return the year-to-date column that ends on the quarter column's date.

        Args:
            period_columns: Period column names that contain values.
            quarter_column: Column selected for the current period.

        Returns:
            The matching year-to-date column, or None.
        """
        if not self._is_quarter_column(quarter_column):
            return None
        end = quarter_column.split(" ")[0]
        ytd_columns = [
            column
            for column in period_columns
            if column.split(" ")[0] == end and not self._is_quarter_column(column)
        ]
        if not ytd_columns:
            return None
        return sorted(ytd_columns)[-1]

    @staticmethod
    def _is_quarter_column(period_col: str) -> bool:
        """Return True when a column name marks a single quarter."""
        lowered = period_col.lower()
        return any(token in lowered for token in ("q1", "q2", "q3", "q4", "quarter"))

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
            shape = self._column_shape(latest_period_col)
            return next(
                (
                    x
                    for _, x in enumerate(period_columns)
                    if len(x.split(" ")) == 2
                    and self._column_shape(x) == shape
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

    def _find_dim_column(self, df: pd.DataFrame, axis_name: str) -> Optional[str]:
        """Find the column name that contains the given axis name.

        Args:
            df: DataFrame to search in
            axis_name: The axis name to search for (e.g., "ProductOrServiceAxis")

        Returns:
            Column name that contains the axis name, or None if not found
        """
        if df is None or df.empty:
            return None

        for col in df.columns:
            if axis_name in col:
                return col
        return None

    def _dim_from_column(self, column_name: str) -> str:
        """Extract the dimension name from a DataFrame column name.

        Args:
            column_name: Column name (e.g., "dim_srt_ProductOrServiceAxis")

        Returns:
            Dimension name (e.g., "srt:ProductOrServiceAxis")
        """
        return column_name.replace("dim_", "").replace("_", ":", 1)

    def _to_sec_concept(self, concept: str) -> str:
        return concept.replace("_", ":", 1)

    def _create_financial_fact_with_hierarchy(
        self,
        row,
        statement_type: str,
        period_col: str,
        comparative_period_col: Optional[str],
        position: int,
    ) -> Optional[FinancialFactCreate]:
        """Create a FinancialFact from a statement row with hierarchical abstracts.

        Args:
            row: DataFrame row from statement
            statement_type: Type of financial statement
            period_col: The period column name (e.g., "2025-06-28 (Q2)")
            comparative_period_col: The comparative period (past year) column name (e.g., "2024-06-28 (Q2)")
            position: Global position of this fact in the statement

        Returns:
            FinancialFact object or None if invalid
        """
        try:
            # Extract basic information
            concept = self._to_sec_concept(row.get("concept", ""))
            parent_concept = (
                self._to_sec_concept(row.get("parent_concept"))
                if row.get("parent_concept")
                else None
            )
            abstract_concept = (
                self._to_sec_concept(row.get("parent_abstract_concept"))
                if row.get("parent_abstract_concept")
                else None
            )
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
            if not is_abstract and self._is_nullish(value):
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
            if not is_abstract and not self._is_nullish(value):
                try:
                    value_decimal = self._to_decimal(value)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid value for concept {concept}: {value}")
                    return None

            # Convert comparative value to Decimal
            comparative_value_decimal = None
            if not is_abstract and not self._is_nullish(comparative_value):
                try:
                    comparative_value_decimal = self._to_decimal(comparative_value)
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid comparative value for concept {concept}: {comparative_value}"
                    )

            weight_decimal = None
            if not is_abstract and not self._is_nullish(weight):
                try:
                    weight_decimal = self._to_decimal(weight)
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
            period = self._determine_period_type_from_column(
                period_col,
                statement_type,
                point_in_time=row.get("point_in_time"),
            )

            # Generate deterministic UUID from concept, statement_type, and period_end
            period_end_str = period_end.isoformat() if period_end else ""
            fact_key = str(
                uuid.uuid5(
                    uuid.NAMESPACE_OID, f"{concept}|{statement_type}|{period_end_str}"
                )
            )

            parent_key = None
            if parent_concept:
                parent_key = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_OID,
                        f"{parent_concept}|{statement_type}|{period_end_str}",
                    )
                )

            abstract_key = None
            if abstract_concept:
                abstract_key = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_OID,
                        f"{abstract_concept}|{statement_type}|{period_end_str}",
                    )
                )

            # Create the financial fact
            fact = FinancialFactCreate(
                key=fact_key,
                parent_key=parent_key,
                abstract_key=abstract_key,
                company_id=0,
                filing_id=0,
                form_type="",
                concept=concept,
                label=label,
                value=value_decimal,
                is_abstract=is_abstract,
                comparative_value=comparative_value_decimal,
                weight=weight_decimal,
                print_sign=self._print_sign(row),
                unit=unit,
                axis=None,
                member=None,
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
