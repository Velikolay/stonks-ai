"""Base Pydantic models for database tables."""

from datetime import date
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class CompanyBase(BaseModel):
    """Base company model."""

    ticker: Optional[str] = None
    exchange: Optional[str] = None
    name: str


class CompanyCreate(CompanyBase):
    """Model for creating a company."""

    pass


class Company(CompanyBase):
    """Complete company model with ID."""

    id: int

    class Config:
        from_attributes = True


class FilingBase(BaseModel):
    """Base filing model."""

    company_id: int
    source: str
    filing_number: str
    form_type: str
    filing_date: date
    fiscal_period_end: date
    fiscal_year: int
    fiscal_quarter: int
    public_url: Optional[str] = None


class FilingCreate(FilingBase):
    """Model for creating a filing."""

    pass


class Filing(FilingBase):
    """Complete filing model with ID."""

    id: int

    class Config:
        from_attributes = True


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
