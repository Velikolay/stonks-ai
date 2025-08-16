"""Filing pydantic models."""

from datetime import date
from typing import Optional

from pydantic import BaseModel


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
