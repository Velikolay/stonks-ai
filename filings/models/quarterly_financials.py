"""Quarterly financial metrics models."""

from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict


class QuarterlyFinancial(BaseModel):
    """Model for quarterly financial metrics from the view."""

    company_id: int
    fiscal_year: int
    fiscal_quarter: int
    label: str
    value: Decimal
    unit: Optional[str] = None
    statement: Optional[str] = None
    period_end: Optional[date] = None
    period_start: Optional[date] = None
    source_type: str  # '10-Q', '10-K', or 'calculated'

    model_config = ConfigDict(from_attributes=True)


class QuarterlyFinancialsFilter(BaseModel):
    """Filter model for querying quarterly financials."""

    company_id: Optional[int] = None
    fiscal_year: Optional[int] = None
    fiscal_quarter: Optional[int] = None
    label: Optional[str] = None
    statement: Optional[str] = None
    source_type: Optional[str] = None
    limit: Optional[int] = 100
