"""Yearly financial metrics models."""

from datetime import date
from decimal import Decimal
from typing import List, Optional

from pydantic import BaseModel, ConfigDict


class YearlyFinancial(BaseModel):
    """Model for yearly financial metrics from the view."""

    id: int
    company_id: int
    filing_id: int
    label: str
    normalized_label: str
    value: Optional[Decimal] = None
    weight: Optional[Decimal] = None
    unit: Optional[str] = None
    statement: Optional[str] = None
    axis: Optional[str] = None
    member: Optional[str] = None
    abstract_id: Optional[int] = None
    is_abstract: bool
    is_synthetic: bool
    period_end: Optional[date] = None
    fiscal_year: int
    # Debug fields
    source_type: str  # '10-K'
    concept: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class YearlyFinancialsFilter(BaseModel):
    """Filter model for querying yearly financials."""

    company_id: int
    fiscal_year_start: Optional[int] = None
    fiscal_year_end: Optional[int] = None
    labels: Optional[List[str]] = None
    normalized_labels: Optional[List[str]] = None
    statement: Optional[str] = None
    axis: Optional[str] = None
