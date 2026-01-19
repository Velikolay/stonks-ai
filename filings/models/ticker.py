"""Ticker models exposed by the API layer."""

from pydantic import BaseModel, ConfigDict


class Ticker(BaseModel):
    """Row model for a `tickers` record."""

    id: int
    ticker: str
    exchange: str
    status: str
    company_id: int

    model_config = ConfigDict(from_attributes=True)


class TickerCreate(BaseModel):
    """Model for creating a ticker mapping for a company."""

    ticker: str
    exchange: str
    status: str = "active"


class TickerUpdate(BaseModel):
    """Model for updating a ticker mapping.

    All fields are optional; only provided fields will be updated.
    """

    ticker: str | None = None
    exchange: str | None = None
    status: str | None = None
