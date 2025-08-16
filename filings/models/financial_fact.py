"""Financial fact pydantic models."""

from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class FinancialFactBase(BaseModel):
    """Base financial fact model."""

    filing_id: int
    metric: str
    value: Decimal = Field(..., decimal_places=2)
    unit: Optional[str] = None
    axis: Optional[str] = None
    member: Optional[str] = None
    statement: Optional[str] = None
    period_end: Optional[date] = None
    period_start: Optional[date] = None


class FinancialFactCreate(FinancialFactBase):
    """Model for creating a financial fact."""

    pass


class FinancialFact(FinancialFactBase):
    """Complete financial fact model with ID."""

    id: int

    class Config:
        from_attributes = True
