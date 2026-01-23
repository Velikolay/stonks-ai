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


class CompanyUpdate(BaseModel):
    """Model for updating a company.

    All fields are optional; only provided fields will be updated.
    """

    name: Optional[str] = None
    industry: Optional[str] = None


class Company(CompanyBase):
    """Complete company model with ID."""

    id: int

    model_config = ConfigDict(from_attributes=True)


class CompanySearch(BaseModel):
    """Company search result model.

    Contains the company identity plus the first ticker that matches the search prefix
    (if any).
    """

    id: int
    name: str
    ticker: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)
