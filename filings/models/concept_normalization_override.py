"""Concept normalization override models."""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, ConfigDict


class ConceptNormalizationOverrideBase(BaseModel):
    """Base concept normalization override model."""

    concept: str
    statement: str
    normalized_label: str
    is_abstract: bool
    abstract_concept: Optional[str] = None
    parent_concept: Optional[str] = None
    description: Optional[str] = None
    unit: Optional[str] = None
    weight: Optional[Decimal] = None


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
