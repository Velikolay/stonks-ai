"""Financial fact pydantic models."""

from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PeriodType(str, Enum):
    """Period type enumeration for financial facts."""

    YTD = "YTD"  # Year to date
    Q = "Q"  # Quarter


class FinancialFactAbstract(BaseModel):
    """Financial fact abstract model with concept and label."""

    concept: str
    label: str


class FinancialFactBase(BaseModel):
    """Base financial fact model."""

    filing_id: int
    concept: str
    label: Optional[str] = None
    value: Decimal = Field(..., decimal_places=2)
    comparative_value: Optional[Decimal] = None
    weight: Optional[Decimal] = None
    unit: Optional[str] = None
    axis: Optional[str] = None
    member: Optional[str] = None
    parsed_axis: Optional[str] = None
    parsed_member: Optional[str] = None
    statement: Optional[str] = None
    abstracts: Optional[List[FinancialFactAbstract]] = None
    period_end: Optional[date] = None
    comparative_period_end: Optional[date] = None
    period_start: Optional[date] = None
    period: Optional[PeriodType] = None
    position: Optional[int] = None

    @field_validator("abstracts", mode="before")
    @classmethod
    def validate_abstracts(cls, v: Any) -> Optional[List[FinancialFactAbstract]]:
        """Convert raw JSON data to FinancialFactAbstract objects."""
        if v is None:
            return None
        if isinstance(v, list):
            if len(v) == 0:
                return []
            return [
                FinancialFactAbstract(**item) if isinstance(item, dict) else item
                for item in v
            ]
        return v


class FinancialFactCreate(FinancialFactBase):
    """Model for creating a financial fact."""

    pass


class FinancialFact(FinancialFactBase):
    """Complete financial fact model with ID."""

    id: int
    model_config = ConfigDict(from_attributes=True)
