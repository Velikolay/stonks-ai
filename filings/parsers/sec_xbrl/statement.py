"""One statement frame as facts.

Face rows keep the filing's latest column, with the closest earlier column
of the same shape as the comparative; the quarterly stage reduces a
year-to-date total. Income Statement lines and per-share amounts also keep
the printed quarter beside that total. Dimension rows keep every column of the
current period's shape, including prior years, and the year-to-date column
of the same date.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

import pandas as pd

from ...models import FinancialFactCreate, PeriodType
from . import columns as cols
from .concepts import is_per_share
from .context import FilingContext
from .facts import dimension_fact, face_fact
from .frame_cache import cached_frame
from .values import cell_text, flag, sec_member

logger = logging.getLogger(__name__)


def statement_frame(statement: object, with_dimensions: bool = True) -> pd.DataFrame:
    """The statement grid; with dimensions, each row's XBRL coordinates attached.

    Read from the filing's frame cache when one is open (see
    :mod:`.frame_cache`); a change to what this returns bumps its
    ``FRAME_VERSION``.
    """

    def build() -> pd.DataFrame:
        frame = statement.to_dataframe(  # type: ignore[attr-defined]
            include_unit=True,
            include_point_in_time=True,
            presentation=False,
        )
        return attach_dimension_metadata(frame, statement) if with_dimensions else frame

    variant = "dimensions" if with_dimensions else "plain"
    return cached_frame(statement, variant, build)


def attach_dimension_metadata(frame: pd.DataFrame, statement: object) -> pd.DataFrame:
    """Copy dimension coordinates from the raw statement rows onto the grid.

    The grid keeps one display dimension per row; the raw rows carry all of
    them. The grid drops an abstract the raw rows still contain, so the two
    are aligned by concept and label rather than by position.
    """
    if frame is None or frame.empty:
        return frame
    try:
        from edgar.xbrl.statements import StatementView

        raw = statement.get_raw_data(view=StatementView.DETAILED)  # type: ignore[attr-defined]
    except Exception:
        logger.exception("Could not read dimension metadata")
        return frame
    if not isinstance(raw, list) or not raw:
        return frame

    attached: list[object] = []
    cursor = 0
    for _, row in frame.iterrows():
        label = cell_text(row.get("label"))
        concept = cell_text(row.get("concept"))
        is_dimension = flag(row.get("dimension"))
        match = next(
            (
                index
                for index in range(cursor, len(raw))
                if cell_text(raw[index].get("label")) == label
                and cell_text(raw[index].get("concept")) == concept
                and flag(raw[index].get("is_dimension")) == is_dimension
            ),
            None,
        )
        if match is None:
            attached.append(None)
            continue
        cursor = match + 1
        attached.append(raw[match].get("dimension_metadata"))

    frame = frame.copy()
    frame["dimension_metadata"] = attached
    return frame


def keeps_published_quarter(fact: FinancialFactCreate) -> bool:
    """The face row also stores its printed quarter beside the year-to-date total.

    Every Income Statement line keeps it. Subtracting the previous 10-Q's
    year to date invents a false quarter when this 10-Q restated it: S&P
    Global's third-quarter 2026 10-Q took Mobility out of the first half, so
    its operating income came out at 1,813 against the 1,956 printed, which
    its segments add up to. Per-share amounts keep it on any statement
    because they are not additive: subtracting year-to-date totals invents a
    false quarter after a stock split.
    """
    return fact.statement == "Income Statement" or is_per_share(fact.concept, fact.unit)


def collapsed_dimension_members(dimensions: pd.DataFrame) -> tuple[set[str], set[str]]:
    """Member ids edgartools reused for several children, and ambiguous suffixes.

    A reused member's children are labeled "{member label} - {child}". A
    suffix keeps its parent when more than one reused member claims it, or
    when the statement has that name as its own member; otherwise the
    elimination of Subscription would land on Subscription.
    """
    suffixes: dict[str, set[str]] = {}
    own_labels: set[str] = set()
    for _, row in dimensions.iterrows():
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
        if suffix:
            suffixes.setdefault(sec_member(raw_member), set()).add(suffix)

    collapsed = {member for member, found in suffixes.items() if len(found) > 1}
    parents_by_suffix: dict[str, set[str]] = {}
    for member in collapsed:
        for suffix in suffixes[member]:
            parents_by_suffix.setdefault(suffix, set()).add(member)
    ambiguous = {
        suffix
        for suffix, parents in parents_by_suffix.items()
        if len(parents) > 1 or suffix in own_labels
    }
    return collapsed, ambiguous


class _Columns:
    """The period columns one statement frame reports, chosen once."""

    def __init__(self, frame: pd.DataFrame):
        self.all = cols.period_columns(frame)
        if not self.all:
            return
        self.quarter = cols.current_column(self.all)
        self.quarter_comparative = cols.prior_column(self.all, self.quarter)
        self.ytd = cols.same_date_ytd_column(self.all, self.quarter)
        # The year to date is the filing's period whatever edgartools names
        # it: Costco's second-quarter 10-Qs print "2025-02-16" beside
        # "2025-02-16 (Q2)", and the quarter sorted last, so the 24 weeks
        # were never stored. Without a same-date one, the latest column.
        self.latest = self.ytd or sorted(self.all)[-1]
        self.latest_comparative = cols.prior_column(self.all, self.latest)


def parse_statement(
    frame: pd.DataFrame,
    statement: str,
    include_dimensions: bool,
    context: FilingContext = FilingContext(),
    every_quarter: bool = False,
) -> list[FinancialFactCreate]:
    """Every fact of one statement frame.

    Args:
        frame: Statement grid from :func:`statement_frame`.
        statement: Statement name stored on the facts.
        include_dimensions: Also parse the frame's dimension rows.
        context: The filing's reporting currency and axis defaults.
        every_quarter: Keep the printed quarter of every undimensioned row,
            not only of revenue totals and per-share amounts. A note's rows
            only survive the segment rules, which match them by period.
    """
    facts: list[FinancialFactCreate] = []
    if frame is None or frame.empty:
        logger.warning("No data found for %s", statement)
        return facts
    try:
        columns = _Columns(frame)
        if not columns.all:
            logger.warning("No period columns found in %s", statement)
            return facts
        is_dimension = frame["dimension"] == True  # noqa: E712
        facts.extend(
            _face_facts(
                frame[~is_dimension], statement, columns, context, every_quarter
            )
        )
        dimensions = frame[is_dimension]
        if include_dimensions and not dimensions.empty:
            # A dimension row reuses the label of the face line it splits.
            labels: dict[str, str] = {}
            for fact in facts:
                if not fact.is_abstract:
                    labels.setdefault(fact.concept, fact.label)
            collapsed, ambiguous = collapsed_dimension_members(dimensions)
            for _, row in dimensions.iterrows():
                facts.extend(
                    _dimension_facts(
                        row, statement, columns, labels, collapsed, ambiguous, context
                    )
                )
    except Exception:
        logger.exception("Error parsing %s", statement)
    return facts


def _face_facts(
    rows: pd.DataFrame,
    statement: str,
    columns: _Columns,
    context: FilingContext,
    every_quarter: bool = False,
) -> list[FinancialFactCreate]:
    facts: list[FinancialFactCreate] = []
    position = 0
    for _, row in rows.iterrows():
        fact = face_fact(
            row,
            statement,
            columns.latest,
            columns.latest_comparative,
            position,
            context,
        )
        if fact is None:
            continue
        facts.append(fact)
        position += 1
        quarter = _published_quarter(
            row, statement, fact, columns, context, every_quarter
        )
        if quarter is not None:
            facts.append(quarter)
    return facts


def _published_quarter(
    row: Any,
    statement: str,
    ytd_fact: FinancialFactCreate,
    columns: _Columns,
    context: FilingContext,
    every_quarter: bool = False,
) -> Optional[FinancialFactCreate]:
    """The printed quarter beside a year-to-date face fact, when it keeps one.

    Otherwise the quarterly stage reconstructs the quarter by subtraction, and
    a restated comparative makes that residual miss the printed quarter.
    """
    if (
        ytd_fact.is_abstract
        or ytd_fact.axis
        or not (every_quarter or keeps_published_quarter(ytd_fact))
        or columns.quarter == columns.latest
    ):
        return None
    fact = face_fact(
        row,
        statement,
        columns.quarter,
        columns.quarter_comparative,
        ytd_fact.position or 0,
        context,
    )
    if fact is None or fact.period != PeriodType.Q:
        return None
    fact.key = f"{fact.key}|q"
    return fact


def _dimension_facts(
    row: Any,
    statement: str,
    columns: _Columns,
    labels: dict[str, str],
    collapsed: set[str],
    ambiguous: set[str],
    context: FilingContext,
) -> list[FinancialFactCreate]:
    """One fact per populated column of a dimension row.

    A 10-K prints two or three years and a 10-Q the prior-year quarter beside
    the current one; each column is its own fact, with the closest earlier
    column of the same shape as its comparative, for rename chaining. A
    year-to-date column beside a quarter of the same date keeps a ``|ytd``
    key suffix.
    """

    def build(column: str, comparative: Optional[str]) -> Optional[FinancialFactCreate]:
        return dimension_fact(
            row, statement, column, comparative, labels, context, collapsed, ambiguous
        )

    # Section headers are not period amounts. numpy bools compare equal to
    # True and are not the same object.
    if row.get("abstract") == True:  # noqa: E712
        fact = build(columns.quarter, None)
        return [fact] if fact else []

    current = cols.same_shape(columns.all, columns.quarter)
    ytd: list[str] = []
    if columns.ytd and columns.ytd not in current:
        ytd = cols.same_shape(columns.all, columns.ytd)

    facts: list[FinancialFactCreate] = []
    quarter_ends = set()
    for column in current:
        fact = build(column, cols.prior_column(columns.all, column))
        if fact is None:
            continue
        if fact.period == PeriodType.Q:
            quarter_ends.add(fact.period_end)
        facts.append(fact)
    for column in ytd:
        fact = build(column, cols.prior_column(columns.all, column))
        if fact is None or fact.period != PeriodType.YTD:
            continue
        if fact.period_end in quarter_ends:
            fact.key = f"{fact.key}|ytd"
        facts.append(fact)
    return facts
