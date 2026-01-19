"""Filing entity models exposed by the API layer."""

from pydantic import BaseModel, ConfigDict


class FilingEntity(BaseModel):
    """Row model for a `filing_entities` record."""

    id: int
    registry: str
    number: str
    status: str
    company_id: int

    model_config = ConfigDict(from_attributes=True)


class FilingEntityCreate(BaseModel):
    """Model for creating a filing entity (e.g., SEC + CIK) for a company."""

    registry: str
    number: str
    status: str = "active"


class FilingEntityUpdate(BaseModel):
    """Model for updating a filing entity.

    All fields are optional; only provided fields will be updated.
    """

    registry: str | None = None
    number: str | None = None
    status: str | None = None
