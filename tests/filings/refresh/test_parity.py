"""The snapshot diff used to check a refresh against a baseline."""

from __future__ import annotations

from decimal import Decimal

from filings.refresh.parity.compare import diff_table

from .builders import frame

D = Decimal


def test_diff_classifies_every_kind_of_difference() -> None:
    base = frame(
        "yearly_financials",
        [
            {"id": 1, "value": 1000, "normalized_label": "Revenue"},
            {"id": 2, "value": 50, "normalized_label": "Cost"},
            {"id": 3, "value": 7, "normalized_label": "Tax"},
        ],
    )
    new = frame(
        "yearly_financials",
        [
            {"id": 1, "value": D("1000.4"), "normalized_label": "Revenue"},
            {"id": 2, "value": 60, "normalized_label": "Costs"},
            {"id": 4, "value": 9, "normalized_label": "Other"},
        ],
    )
    found = {
        (r["key"], r["class"], r["column"])
        for r in diff_table("yearly_financials", base, new).iter_rows(named=True)
    }
    assert found == {
        ("1", "within_rounding", "value"),
        ("2", "value_differs", "value"),
        ("2", "column_differs", "normalized_label"),
        ("3", "missing", None),
        ("4", "extra", None),
    }
