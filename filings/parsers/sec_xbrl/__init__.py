"""SEC XBRL filings as financial facts.

* :mod:`.parser`: the statements a filing is read for, and the result.
* :mod:`.statement`: one statement frame as face and dimension facts.
* :mod:`.facts`: one row and period column as a fact.
* :mod:`.columns`: which period columns a frame reports.
* :mod:`.segments`: segment members tagged only in a note.
* :mod:`.context`: the filing's fiscal focus, reporting currency and axis defaults.
* :mod:`.concepts`, :mod:`.values`: concept lists and cell conversion.
"""

from .context import FilingContext, fiscal_focus, reporting_currency_amounts
from .parser import STATEMENTS, ParsedFiling, SECXBRLParser, StatementSpec
from .values import filed_unit

__all__ = [
    "STATEMENTS",
    "FilingContext",
    "ParsedFiling",
    "SECXBRLParser",
    "StatementSpec",
    "filed_unit",
    "fiscal_focus",
    "reporting_currency_amounts",
]
