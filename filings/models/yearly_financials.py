"""Yearly financial metrics models."""

from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict


class YearlyFinancial(BaseModel):
    """Model for yearly financial metrics from the view."""

    company_id: int
    label: str
    normalized_label: str
    value: Decimal
    unit: Optional[str] = None
    statement: Optional[str] = None
    period_end: Optional[date] = None
    period_start: Optional[date] = None
    fiscal_year: int
    fiscal_period_end: Optional[date] = None

    model_config = ConfigDict(from_attributes=True)


class YearlyFinancialsFilter(BaseModel):
    """Filter model for querying yearly financials."""

    company_id: int
    fiscal_year_start: Optional[int] = None
    fiscal_year_end: Optional[int] = None
    label: Optional[str] = None
    normalized_label: Optional[str] = None
    statement: Optional[str] = None
