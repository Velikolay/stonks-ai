"""Concept normalization override models."""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict, model_validator


class ConceptNormalizationOverrideBase(BaseModel):
    """Base concept normalization override model."""

    company_id: int
    concept: str
    statement: str
    normalized_label: str
    is_abstract: bool
    is_global: bool
    abstract_concept: Optional[str] = None
    parent_concept: Optional[str] = None
    description: Optional[str] = None
    unit: Optional[str] = None
    weight: Optional[Decimal] = None

    @model_validator(mode="after")
    def _sync_is_global(self) -> "ConceptNormalizationOverrideBase":
        self.is_global = self.company_id == 0
        return self


class ConceptNormalizationOverrideCreate(ConceptNormalizationOverrideBase):
    """Model for creating a concept normalization override."""

    pass


class ConceptNormalizationOverrideUpdate(BaseModel):
    """Model for updating a concept normalization override."""

    normalized_label: Optional[str] = None
    is_abstract: Optional[bool] = None
    abstract_concept: Optional[str] = None
    parent_concept: Optional[str] = None
    description: Optional[str] = None
    unit: Optional[str] = None
    weight: Optional[Decimal] = None


class ConceptNormalizationOverride(ConceptNormalizationOverrideBase):
    """Complete concept normalization override model."""

    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)
