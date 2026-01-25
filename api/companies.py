"""Company endpoints."""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from filings.db import FilingsDatabase

logger = logging.getLogger(__name__)

# Create router for company endpoints
router = APIRouter(prefix="/companies", tags=["companies"])

# Global database instance (will be set during app initialization)
filings_db: Optional[FilingsDatabase] = None


class CompanySearchResponse(BaseModel):
    """API response model for company search."""

    id: int
    name: str
    ticker: str


class CompanyResponse(BaseModel):
    """API response model for exact-match active ticker lookup."""

    id: int
    name: str
    ticker: str


def set_filings_db(db: FilingsDatabase) -> None:
    """Set the global filings database instance."""
    global filings_db
    filings_db = db


@router.get(
    "/search",
    response_model=List[CompanySearchResponse],
    response_model_exclude_none=True,
)
async def search_companies(
    prefix: str = Query(..., description="Company name or ticker prefix", min_length=1),
    limit: int = Query(20, description="Max results (1-20)", ge=1, le=20),
) -> List[CompanySearchResponse]:
    """Search companies by name/ticker prefix."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        rows = filings_db.companies.search_companies_by_prefix(
            prefix=prefix, limit=limit
        )
        return [
            CompanySearchResponse(id=r.id, name=r.name, ticker=r.ticker) for r in rows
        ]
    except Exception as e:
        logger.error("Error searching companies by prefix=%s: %s", prefix, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "",
    response_model=CompanyResponse,
    response_model_exclude_none=True,
)
async def get_company_by_ticker(
    ticker: str = Query(..., description="Exact-match ticker", min_length=1),
    exchange: Optional[str] = Query(None, description="Optional exchange filter"),
) -> CompanyResponse:
    """Retrieve a company via an active ticker (exact match)."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        company = filings_db.companies.get_company_by_ticker(
            ticker=ticker, exchange=exchange
        )
        if company is None:
            raise HTTPException(
                status_code=404,
                detail="Company not found for active ticker",
            )
        return CompanyResponse(
            id=company.id,
            name=company.name,
            ticker=ticker,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            "Error getting company by active ticker=%s exchange=%s: %s",
            ticker,
            exchange,
            e,
        )
        raise HTTPException(status_code=500, detail=str(e))
