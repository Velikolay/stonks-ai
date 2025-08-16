"""Company pydantic models."""

from typing import Optional

from pydantic import BaseModel


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
