"""A whole filing through the parser, and the comparative-column rules."""

from types import SimpleNamespace

import pandas as pd

from filings.parsers.sec_xbrl import SECXBRLParser
from filings.parsers.sec_xbrl.columns import prior_column


class _Statement:
    def __init__(self, frame: pd.DataFrame):
        self.frame = frame

    def to_dataframe(self, **_):
        return self.frame

    def get_raw_data(self, **_):
        return []


def _income_statement() -> _Statement:
    return _Statement(
        pd.DataFrame(
            [
                {
                    "concept": "us-gaap_Revenues",
                    "label": "Revenue",
                    "2024-12-31 (FY)": 100,
                    "2023-12-31 (FY)": 90,
                    "abstract": False,
                    "dimension": False,
                }
            ]
        )
    )


def _filing(**statements) -> SimpleNamespace:
    accessors = {
        name: (lambda s=statements.get(name): s)
        for name in (
            "income_statement",
            "balance_sheet",
            "cashflow_statement",
            "comprehensive_income",
            "statement_of_equity",
        )
    }
    accessors["list_available"] = lambda: pd.DataFrame()
    xbrl = SimpleNamespace(
        entity_info={"fiscal_year": "2024", "fiscal_period": "FY"},
        statements=SimpleNamespace(**accessors),
        facts=None,
        axes={},
    )
    return SimpleNamespace(accession_number="0000000000-24-000001", xbrl=lambda: xbrl)


def test_a_missing_statement_keeps_the_others() -> None:
    """A filing whose balance sheet will not load still yields its income statement."""
    parsed = SECXBRLParser().parse_filing(_filing(income_statement=_income_statement()))
    assert [f.concept for f in parsed.facts] == ["us-gaap:Revenues"]
    assert parsed.fiscal_focus == (2024, 4)


def test_a_filing_without_statements_has_no_facts() -> None:
    """A Part III 10-K/A carries no financial statements."""
    assert SECXBRLParser().parse_filing(_filing()).facts == []


def test_an_instant_compares_with_the_year_end_the_filing_prints() -> None:
    """A 10-Q balance sheet prints the prior year end before last year's quarter."""
    columns = ["2026-07-31", "2026-01-31", "2025-07-31"]
    assert prior_column(columns, "2026-07-31") == "2026-01-31"


def test_a_duration_compares_with_the_same_period_a_year_earlier() -> None:
    """A 10-Q statement of equity also prints the year's earlier quarters."""
    columns = [
        "2026-05-03 (Q2)",
        "2026-08-02 (Q3)",
        "2025-08-03 (Q3)",
        "2026-02-01 (Q1)",
    ]
    assert prior_column(columns, "2026-08-02 (Q3)") == "2025-08-03 (Q3)"


def test_without_a_year_back_column_the_closest_earlier_one_is_used() -> None:
    columns = ["2024-12-31 (FY)", "2022-12-31 (FY)", "2021-12-31 (FY)"]
    assert prior_column(columns, "2024-12-31 (FY)") == "2022-12-31 (FY)"
    assert prior_column(columns, "2021-12-31 (FY)") is None


def test_printed_quarter_hangs_under_its_parents_quarter():
    """Alphabet's quarterly hedging gains sit under the quarter's revenue."""
    from filings.parsers.sec_xbrl.parser import link_published_quarters

    def fact(key, parent_key=None):
        return SimpleNamespace(key=key, parent_key=parent_key)

    revenue_ytd = fact("rev|IS|2025-06-30")
    revenue_q = fact("rev|IS|2025-06-30|q")
    hedge_q = fact("hedge|IS|2025-06-30|q", "rev|IS|2025-06-30")
    cost_q = fact("cost|IS|2025-06-30|q", "opex|IS|2025-06-30")
    link_published_quarters([revenue_ytd, revenue_q, hedge_q, cost_q])

    assert hedge_q.parent_key == "rev|IS|2025-06-30|q"
    # No printed quarter of the parent: the year-to-date parent stays.
    assert cost_q.parent_key == "opex|IS|2025-06-30"
