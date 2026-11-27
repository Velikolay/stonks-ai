"""Polars schemas for every table the refresh reads or writes.

Column order matches the database, so a frame can be copied into its table
as is. ``numeric`` columns are ``Decimal(38, 10)``: the stored values have at
most five decimals and stay below 10^13, and the arithmetic must be exact.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

import polars as pl

DECIMAL = pl.Decimal(38, 10)
STR = pl.String()
INT = pl.Int32()
BIGINT = pl.Int64()
BOOL = pl.Boolean()
DATE = pl.Date()
TIMESTAMP = pl.Datetime("us")


@dataclass(frozen=True)
class Table:
    """A table's schema and how to scope it to one company.

    ``company_column`` filters directly; ``company_filter`` is a SQL
    condition on ``$1`` for a table without one. ``scope`` is a condition
    every load applies, with or without a company.
    """

    name: str
    schema: pl.Schema
    company_column: Optional[str] = "company_id"
    company_filter: Optional[str] = None
    scope: Optional[str] = None

    @property
    def columns(self) -> list[str]:
        return list(self.schema.names())

    @property
    def dtypes(self) -> Mapping[Any, pl.DataType]:
        return dtypes_of(self.schema)

    def empty(self) -> pl.DataFrame:
        return pl.DataFrame(schema=self.schema)

    def conform(self, frame: pl.DataFrame) -> pl.DataFrame:
        """``frame`` with exactly this table's columns, in order, and types."""
        return frame.select(self.columns).cast(self.dtypes)


def dtypes_of(schema: pl.Schema) -> Mapping[Any, pl.DataType]:
    """A schema as the column-to-type mapping ``DataFrame.cast`` accepts."""
    return dict(schema)


def _table(
    table_name: str,
    /,
    company_column: Optional[str] = "company_id",
    scope: Optional[str] = None,
    **cols: pl.DataType,
) -> Table:
    return Table(table_name, pl.Schema(cols), company_column, scope=scope)


#: Shell-company filings (a SPAC before its merger) are stored but never
#: read: they describe the trust, not the business that later files under
#: the same CIK. Every raw table keyed by filing drops them on load.
NOT_SHELL = "NOT is_shell"
NOT_SHELL_FACT = "filing_id NOT IN (SELECT id FROM filings WHERE is_shell)"


# --- Raw inputs -------------------------------------------------------------

COMPANIES = _table("companies", company_column="id", id=INT, name=STR, industry=STR)

FILINGS = _table(
    "filings",
    scope=NOT_SHELL,
    id=INT,
    company_id=INT,
    filing_entity_id=INT,
    registry=STR,
    number=STR,
    form_type=STR,
    filing_date=DATE,
    fiscal_period_end=DATE,
    fiscal_year=INT,
    fiscal_quarter=INT,
    public_url=STR,
    is_shell=BOOL,
)

FINANCIAL_FACTS = _table(
    "financial_facts",
    scope=NOT_SHELL_FACT,
    id=BIGINT,
    filing_id=INT,
    company_id=INT,
    form_type=STR,
    concept=STR,
    label=STR,
    is_abstract=BOOL,
    value=DECIMAL,
    comparative_value=DECIMAL,
    weight=DECIMAL,
    print_sign=DECIMAL,
    unit=STR,
    axis=STR,
    member=STR,
    member_label=STR,
    statement=STR,
    period_end=DATE,
    comparative_period_end=DATE,
    period=STR,
    position=INT,
    parent_id=BIGINT,
    abstract_id=BIGINT,
)

FINANCIAL_FACT_DIMENSIONS = Table(
    "financial_fact_dimensions",
    pl.Schema(
        {
            "fact_id": BIGINT,
            "ordinal": INT,
            "axis": STR,
            "member": STR,
            "member_label": STR,
            "is_default": BOOL,
        }
    ),
    company_column=None,
    company_filter="fact_id IN (SELECT id FROM financial_facts WHERE company_id = $1)",
    scope=("fact_id IN (SELECT id FROM financial_facts WHERE " + NOT_SHELL_FACT + ")"),
)

# --- Overrides (global and company rows; loaded once per run) ---------------

FINANCIAL_FACTS_OVERRIDES = _table(
    "financial_facts_overrides",
    company_column=None,
    id=INT,
    company_id=INT,
    concept=STR,
    statement=STR,
    axis=STR,
    member=STR,
    label=STR,
    form_type=STR,
    from_period=DATE,
    to_period=DATE,
    is_global=BOOL,
    to_concept=STR,
    to_axis=STR,
    to_member=STR,
    to_member_label=STR,
    to_weight=DECIMAL,
    created_at=TIMESTAMP,
    updated_at=TIMESTAMP,
    action=STR,
    from_filing_period=DATE,
    to_filing_period=DATE,
)

CONCEPT_NORMALIZATION_OVERRIDES = _table(
    "concept_normalization_overrides",
    company_column=None,
    company_id=INT,
    concept=STR,
    statement=STR,
    normalized_label=STR,
    is_abstract=BOOL,
    is_global=BOOL,
    description=STR,
    unit=STR,
    weight=DECIMAL,
    parent_concept=STR,
    abstract_concept=STR,
    created_at=TIMESTAMP,
    updated_at=TIMESTAMP,
)

DIMENSION_NORMALIZATION_OVERRIDES = _table(
    "dimension_normalization_overrides",
    company_column=None,
    id=INT,
    company_id=INT,
    axis=STR,
    member=STR,
    member_label=STR,
    is_global=BOOL,
    normalized_axis_label=STR,
    normalized_member_label=STR,
    tags=pl.List(STR),
    created_at=TIMESTAMP,
    updated_at=TIMESTAMP,
)

DIMENSION_BREAKDOWN_OVERRIDES = _table(
    "dimension_breakdown_overrides",
    company_column=None,
    id=INT,
    company_id=INT,
    normalized_label=STR,
    axis=STR,
    parent_member=STR,
    members=pl.List(STR),
)

FINANCIAL_REWRITES = _table(
    "financial_rewrites",
    company_column=None,
    id=INT,
    company_id=INT,
    statement=STR,
    normalized_label=STR,
    axis=STR,
    member=STR,
    period_end=DATE,
    granularity=STR,
    action=STR,
    value=DECIMAL,
    created_at=TIMESTAMP,
    updated_at=TIMESTAMP,
)

# --- Derived tables, in pipeline order --------------------------------------

FINANCIAL_FACTS_OVERRIDDEN = _table(
    "financial_facts_overridden",
    id=BIGINT,
    company_id=INT,
    statement=STR,
    concept=STR,
    axis=STR,
    member=STR,
    member_label=STR,
    weight=DECIMAL,
    fact_override_id=INT,
    source_fact_id=BIGINT,
)

CONCEPT_NORMALIZATION = _table(
    "concept_normalization",
    company_id=INT,
    statement=STR,
    concept=STR,
    normalized_label=STR,
    weight=DECIMAL,
    unit=STR,
    group_id=STR,
    source=STR,
    overridden=BOOL,
)

HIERARCHY_NORMALIZATION = _table(
    "hierarchy_normalization",
    company_id=INT,
    statement=STR,
    concept=STR,
    parent_concept=STR,
    concept_source=STR,
    parent_concept_source=STR,
)

DIMENSION_NORMALIZATION = _table(
    "dimension_normalization",
    company_id=INT,
    statement=STR,
    normalized_label=STR,
    axis=STR,
    member=STR,
    member_label=STR,
    normalized_axis_label=STR,
    normalized_member_label=STR,
    group_id=STR,
    source=STR,
    overridden=BOOL,
    override_priority=STR,
    override_level=STR,
)

FINANCIAL_FACTS_NORMALIZED = _table(
    "financial_facts_normalized",
    id=BIGINT,
    company_id=INT,
    filing_id=INT,
    form_type=STR,
    concept=STR,
    label=STR,
    normalized_label=STR,
    is_abstract=BOOL,
    value=DECIMAL,
    comparative_value=DECIMAL,
    weight=DECIMAL,
    print_sign=DECIMAL,
    unit=STR,
    axis=STR,
    member=STR,
    statement=STR,
    period_end=DATE,
    comparative_period_end=DATE,
    period=STR,
    position=INT,
    parent_id=BIGINT,
    abstract_id=BIGINT,
    is_synthetic=BOOL,
    is_duplicate=BOOL,
    fiscal_filing_id=INT,
)

DIMENSION_BREAKDOWNS = _table(
    "dimension_breakdowns",
    company_id=INT,
    statement=STR,
    normalized_label=STR,
    axis=STR,
    parent_member=STR,
    members=pl.List(STR),
    fiscal_years=pl.List(INT),
    source=STR,
)

QUARTERLY_FINANCIALS = _table(
    "quarterly_financials",
    id=BIGINT,
    parent_id=BIGINT,
    company_id=INT,
    filing_id=INT,
    concept=STR,
    label=STR,
    normalized_label=STR,
    value=DECIMAL,
    weight=DECIMAL,
    print_sign=DECIMAL,
    unit=STR,
    axis=STR,
    member=STR,
    statement=STR,
    abstract_id=BIGINT,
    period_end=DATE,
    fiscal_year=INT,
    fiscal_quarter=INT,
    position=INT,
    is_abstract=BOOL,
    is_synthetic=BOOL,
    source_type=STR,
)

YEARLY_FINANCIALS = _table(
    "yearly_financials",
    id=BIGINT,
    parent_id=BIGINT,
    company_id=INT,
    filing_id=INT,
    concept=STR,
    label=STR,
    normalized_label=STR,
    value=DECIMAL,
    unit=STR,
    weight=DECIMAL,
    print_sign=DECIMAL,
    axis=STR,
    member=STR,
    statement=STR,
    abstract_id=BIGINT,
    period_end=DATE,
    fiscal_year=INT,
    position=INT,
    is_abstract=BOOL,
    is_synthetic=BOOL,
    source_type=STR,
)

#: Override tables, loaded once per run and shared by every company.
OVERRIDE_TABLES: tuple[Table, ...] = (
    FINANCIAL_FACTS_OVERRIDES,
    CONCEPT_NORMALIZATION_OVERRIDES,
    DIMENSION_NORMALIZATION_OVERRIDES,
    DIMENSION_BREAKDOWN_OVERRIDES,
    FINANCIAL_REWRITES,
)

#: Tables the refresh writes, in pipeline order.
DERIVED_TABLES: tuple[Table, ...] = (
    FINANCIAL_FACTS_OVERRIDDEN,
    CONCEPT_NORMALIZATION,
    HIERARCHY_NORMALIZATION,
    DIMENSION_NORMALIZATION,
    FINANCIAL_FACTS_NORMALIZED,
    DIMENSION_BREAKDOWNS,
    QUARTERLY_FINANCIALS,
    YEARLY_FINANCIALS,
)

#: Primary key per derived table, used to diff two refreshes row by row.
PRIMARY_KEYS: dict[str, tuple[str, ...]] = {
    "financial_facts_overridden": ("id",),
    "concept_normalization": ("company_id", "statement", "concept"),
    "hierarchy_normalization": ("company_id", "statement", "concept", "parent_concept"),
    "dimension_normalization": (
        "company_id",
        "statement",
        "normalized_label",
        "axis",
        "member",
        "member_label",
    ),
    "financial_facts_normalized": ("id",),
    "dimension_breakdowns": (
        "company_id",
        "statement",
        "normalized_label",
        "axis",
        "parent_member",
        "members",
    ),
    "quarterly_financials": ("id",),
    "yearly_financials": ("id",),
}

TABLES: dict[str, Table] = {
    t.name: t
    for t in (
        COMPANIES,
        FILINGS,
        FINANCIAL_FACTS,
        FINANCIAL_FACT_DIMENSIONS,
        *OVERRIDE_TABLES,
        *DERIVED_TABLES,
    )
}
