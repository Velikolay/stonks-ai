"""Parse one 10-K, 10-Q or 20-F into financial facts with edgartools."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Optional

from edgar import Filing

from ...models import FinancialFactCreate
from .context import FilingContext, fiscal_focus
from .frame_cache import filing_frames
from .measures import segment_measures
from .segments import disclosure_notes, segment_disclosure_facts
from .statement import parse_statement, statement_frame

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StatementSpec:
    """A statement the parser reads, and how."""

    name: str
    #: The edgartools accessor on ``xbrl.statements``.
    accessor: str
    #: Parse dimension rows too. Only the income statement's splits are series.
    with_dimensions: bool = False


STATEMENTS = (
    StatementSpec("Income Statement", "income_statement", with_dimensions=True),
    StatementSpec("Balance Sheet", "balance_sheet"),
    StatementSpec("Cash Flow Statement", "cashflow_statement"),
    StatementSpec("Comprehensive Income", "comprehensive_income"),
    StatementSpec("Statement of Equity", "statement_of_equity"),
)


@dataclass
class ParsedFiling:
    """The facts of one filing and the fiscal period its cover declares."""

    facts: list[FinancialFactCreate] = field(default_factory=list)
    #: Fiscal year and quarter from the cover; either is None when absent.
    fiscal_focus: tuple[Optional[int], Optional[int]] = (None, None)


class SECXBRLParser:
    """Parser for SEC XBRL filings (10-K, 10-Q, 20-F) using edgartools."""

    def parse_filing(self, filing: Filing) -> ParsedFiling:
        """Every fact of the filing's financial statements and segment notes.

        Segment revenue a note adds sits right after the income statement;
        other segment measures (:mod:`.measures`) come last.

        A statement that fails to load or parse is logged and skipped, so one
        bad statement does not lose the others. A filing with no financial
        statements (a Part III 10-K/A) has no facts.
        """
        accession = getattr(filing, "accession_number", "")
        with filing_frames(accession):
            return self._parse_filing(filing, accession)

    def _parse_filing(self, filing: Filing, accession: str) -> ParsedFiling:
        try:
            xbrl = filing.xbrl()
        except Exception:
            logger.exception("Could not read XBRL for %s", accession)
            return ParsedFiling()
        if xbrl is None:
            logger.error("Error parsing filing %s: no XBRL", accession)
            return ParsedFiling()

        parsed = ParsedFiling(
            fiscal_focus=fiscal_focus(getattr(xbrl, "entity_info", None))
        )
        context = FilingContext.of(xbrl, accession)
        notes = disclosure_notes(xbrl)
        found = 0
        for spec in STATEMENTS:
            statement = self._statement(xbrl, spec, accession)
            if statement is None:
                continue
            found += 1
            facts = self._parse(statement, spec, context, accession)
            parsed.facts.extend(facts)
            if spec.with_dimensions:
                parsed.facts.extend(segment_disclosure_facts(notes, facts, context))
        if found:
            parsed.facts.extend(segment_measures(notes, parsed.facts, context))
            link_published_quarters(parsed.facts)

        if found == 0:
            logger.info("Filing %s has no financial statements", accession)
        else:
            logger.info(
                "Parsed %s financial facts from filing %s", len(parsed.facts), accession
            )
        return parsed

    @staticmethod
    def _statement(xbrl: object, spec: StatementSpec, accession: str) -> object:
        try:
            get: Callable[[], object] = getattr(xbrl.statements, spec.accessor)  # type: ignore[attr-defined]
            return get()
        except Exception:
            logger.exception("Could not read %s for %s", spec.name, accession)
            return None

    @staticmethod
    def _parse(
        statement: object, spec: StatementSpec, context: FilingContext, accession: str
    ) -> list[FinancialFactCreate]:
        try:
            frame = statement_frame(statement, with_dimensions=spec.with_dimensions)
        except Exception:
            logger.exception("Could not read %s for %s", spec.name, accession)
            return []
        return parse_statement(frame, spec.name, spec.with_dimensions, context)


def link_published_quarters(facts: list[FinancialFactCreate]) -> None:
    """Hang a printed quarter under its parent's printed quarter.

    A quarter beside a year-to-date total (``|q``) inherits the total's
    parent key, which names the parent's year-to-date fact. When the parent
    prints its quarter too, point at that instead: Alphabet's quarterly
    hedging gains belong under the quarter's revenue, not the half year's.
    """
    keys = {f.key for f in facts}
    for f in facts:
        if f.key.endswith("|q") and f.parent_key and f.parent_key + "|q" in keys:
            f.parent_key = f.parent_key + "|q"
