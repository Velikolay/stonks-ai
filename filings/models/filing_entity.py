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
