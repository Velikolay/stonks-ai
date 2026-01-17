"""Company pydantic models."""

from typing import Optional

from pydantic import BaseModel, ConfigDict


class CompanyBase(BaseModel):
    """Base company model."""

    name: str
    industry: Optional[str] = None


class CompanyCreate(CompanyBase):
    """Model for creating a company."""

    pass


class Company(CompanyBase):
    """Complete company model with ID."""

    id: int

    model_config = ConfigDict(from_attributes=True)
