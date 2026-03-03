"""Financial facts override models."""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, model_validator


class FinancialFactsOverrideBase(BaseModel):
    """Base financial facts override model."""

    company_id: int
    concept: str
    statement: str

    axis: Optional[str] = None
    member: Optional[str] = None
    label: Optional[str] = None
    form_type: Optional[str] = None
    from_period: Optional[date] = None
    to_period: Optional[date] = None

    to_concept: str
    to_axis: Optional[str] = None
    to_member: Optional[str] = None


class FinancialFactsOverrideCreate(FinancialFactsOverrideBase):
    """Model for creating a financial facts override."""

    pass


class FinancialFactsOverrideUpdate(BaseModel):
    """Model for updating a financial facts override."""

    axis: Optional[str] = None
    member: Optional[str] = None
    label: Optional[str] = None
    form_type: Optional[str] = None
    from_period: Optional[date] = None
    to_period: Optional[date] = None

    to_concept: Optional[str] = None
    to_axis: Optional[str] = None
    to_member: Optional[str] = None


class FinancialFactsOverride(FinancialFactsOverrideBase):
    """Complete financial facts override model."""

    id: int
    is_global: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="after")
    def _sync_is_global(self) -> "FinancialFactsOverride":
        self.is_global = self.company_id == 0
        return self
