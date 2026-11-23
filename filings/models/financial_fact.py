"""Financial fact pydantic models."""

from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator


class PeriodType(str, Enum):
    """Period type enumeration for financial facts."""

    YTD = "YTD"  # Year to date
    Q = "Q"  # Quarter


class FactDimension(BaseModel):
    """One XBRL axis and member on a fact.

    A fact can carry several. The fact's own axis and member stay the single
    display coordinate the rest of the pipeline already uses.
    """

    axis: str
    member: str
    member_label: str
    is_default: bool = False


class FinancialFactBase(BaseModel):
    """Base financial fact model."""

    company_id: int
    filing_id: int
    form_type: str
    concept: str
    label: str
    period_end: date
    is_abstract: bool
    value: Optional[Decimal] = Field(None, decimal_places=5)
    comparative_value: Optional[Decimal] = Field(None, decimal_places=5)
    weight: Optional[Decimal] = None
    print_sign: Optional[Decimal] = None
    unit: Optional[str] = None
    axis: Optional[str] = None
    member: Optional[str] = None
    member_label: Optional[str] = None
    statement: Optional[str] = None
    comparative_period_end: Optional[date] = None
    period: Optional[PeriodType] = None
    position: Optional[int] = None

    @model_validator(mode="after")
    def validate_abstract_value(self) -> "FinancialFactBase":
        """Validate that non-abstract facts have a value."""
        if not self.is_abstract and self.value is None:
            raise ValueError("value cannot be None when is_abstract is False")
        return self


class FinancialFactCreate(FinancialFactBase):
    """Model for creating a financial fact."""

    # runtime key to keep track of the fact hierarchy
    # after db insert these are replaced with auto-generated ids
    key: str
    parent_key: Optional[str] = None
    abstract_key: Optional[str] = None
    dimensions: list[FactDimension] = Field(default_factory=list)


class FinancialFact(FinancialFactBase):
    """Complete financial fact model with ID."""

    id: int
    parent_id: Optional[int] = None
    abstract_id: Optional[int] = None

    model_config = ConfigDict(from_attributes=True)
