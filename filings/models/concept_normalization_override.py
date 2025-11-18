"""Concept normalization override models."""

from typing import Optional

from pydantic import BaseModel, ConfigDict


class ConceptNormalizationOverrideBase(BaseModel):
    """Base concept normalization override model."""

    concept: str
    statement: str
    normalized_label: str
    is_abstract: bool
    parent_concept: Optional[str] = None
    description: Optional[str] = None


class ConceptNormalizationOverrideCreate(ConceptNormalizationOverrideBase):
    """Model for creating a concept normalization override."""

    pass


class ConceptNormalizationOverrideUpdate(BaseModel):
    """Model for updating a concept normalization override."""

    normalized_label: Optional[str] = None
    is_abstract: Optional[bool] = None
    parent_concept: Optional[str] = None
    description: Optional[str] = None


class ConceptNormalizationOverride(ConceptNormalizationOverrideBase):
    """Complete concept normalization override model."""

    model_config = ConfigDict(from_attributes=True)
