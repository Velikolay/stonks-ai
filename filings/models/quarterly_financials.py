"""Quarterly financial metrics models."""

from datetime import date
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class QuarterlyFinancial(BaseModel):
    """Model for quarterly financial metrics from the view."""

    company_id: int
    filing_id: int
    # fiscal_year: int
    # fiscal_quarter: int
    label: str
    normalized_label: str
    value: Decimal
    weight: Optional[Decimal] = None
    unit: Optional[str] = None
    statement: Optional[str] = None
    axis: Optional[str] = None
    member: Optional[str] = None
    abstracts: Optional[List[str]] = None
    period_end: Optional[date] = None
    # Debug fields
    source_type: str  # '10-Q', or 'calculated'
    concept: Optional[str] = None
    abstract_concepts: Optional[List[str]] = None

    model_config = ConfigDict(from_attributes=True)


class QuarterlyFinancialsFilter(BaseModel):
    """Filter model for querying quarterly financials."""

    company_id: int
    fiscal_year_start: Optional[int] = None
    fiscal_year_end: Optional[int] = None
    fiscal_quarter_start: Optional[int] = None
    fiscal_quarter_end: Optional[int] = None
    labels: Optional[List[str]] = None
    normalized_labels: Optional[List[str]] = None
    statement: Optional[str] = None
    axis: Optional[str] = None
