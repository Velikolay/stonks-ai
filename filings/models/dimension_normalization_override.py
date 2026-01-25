"""Dimension normalization override models."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, model_validator


class DimensionNormalizationOverrideBase(BaseModel):
    """Base dimension normalization override model."""

    company_id: int
    axis: str
    member: str
    member_label: str
    normalized_axis_label: str
    normalized_member_label: Optional[str] = None
    tags: Optional[List[str]] = None


class DimensionNormalizationOverrideCreate(DimensionNormalizationOverrideBase):
    """Model for creating a dimension normalization override."""

    pass


class DimensionNormalizationOverrideUpdate(BaseModel):
    """Model for updating a dimension normalization override."""

    normalized_axis_label: Optional[str] = None
    normalized_member_label: Optional[str] = None
    tags: Optional[List[str]] = None


class DimensionNormalizationOverride(DimensionNormalizationOverrideBase):
    """Complete dimension normalization override model."""

    is_global: bool

    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

    @model_validator(mode="after")
    def _sync_is_global(self) -> "DimensionNormalizationOverride":
        self.is_global = self.company_id == 0
        return self
