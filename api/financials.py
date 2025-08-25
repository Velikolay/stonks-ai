"""Financial data endpoints."""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from filings.db import FilingsDatabase
from filings.models.quarterly_financials import QuarterlyFinancialsFilter
from filings.models.yearly_financials import YearlyFinancialsFilter

logger = logging.getLogger(__name__)

# Create router for financial endpoints
router = APIRouter(prefix="/financials", tags=["financials"])

# Global database instance (will be set during app initialization)
filings_db: Optional[FilingsDatabase] = None


def set_filings_db(db: FilingsDatabase) -> None:
    """Set the global filings database instance."""
    global filings_db
    filings_db = db


class FinancialMetricResponse(BaseModel):
    """Response model for financial metrics."""

    company_id: int
    ticker: str
    company_name: str
    fiscal_year: int
    fiscal_quarter: Optional[int] = None
    label: str
    normalized_label: str
    value: float
    unit: Optional[str] = None
    statement: Optional[str] = None
    period_end: Optional[str] = None
    period_start: Optional[str] = None
    source_type: Optional[str] = None


class NormalizedLabelResponse(BaseModel):
    """Response model for normalized labels."""

    normalized_label: str
    statement: Optional[str] = None
    count: int


@router.get("/", response_model=List[FinancialMetricResponse])
async def get_financials(
    ticker: str = Query(..., description="Company ticker symbol"),
    granularity: str = Query(
        ..., description="Data granularity: 'quarterly' or 'yearly'"
    ),
    fiscal_year_start: Optional[int] = Query(None, description="Start fiscal year"),
    fiscal_year_end: Optional[int] = Query(None, description="End fiscal year"),
    fiscal_quarter_start: Optional[int] = Query(
        None, description="Start fiscal quarter (1-4)"
    ),
    fiscal_quarter_end: Optional[int] = Query(
        None, description="End fiscal quarter (1-4)"
    ),
    label: Optional[str] = Query(None, description="Filter by metric label"),
    normalized_label: Optional[str] = Query(
        None, description="Filter by normalized label"
    ),
    statement: Optional[str] = Query(None, description="Filter by financial statement"),
) -> List[FinancialMetricResponse]:
    """Get quarterly or yearly financial metrics for a company by ticker."""

    # Validate granularity parameter
    if granularity not in ["quarterly", "yearly"]:
        raise HTTPException(
            status_code=400, detail="granularity must be either 'quarterly' or 'yearly'"
        )

    # Validate fiscal quarter parameters
    if fiscal_quarter_start is not None and not (1 <= fiscal_quarter_start <= 4):
        raise HTTPException(
            status_code=400, detail="fiscal_quarter_start must be between 1 and 4"
        )
    if fiscal_quarter_end is not None and not (1 <= fiscal_quarter_end <= 4):
        raise HTTPException(
            status_code=400, detail="fiscal_quarter_end must be between 1 and 4"
        )

    # Validate that quarterly parameters are only used with quarterly granularity
    if granularity == "yearly" and (
        fiscal_quarter_start is not None or fiscal_quarter_end is not None
    ):
        raise HTTPException(
            status_code=400,
            detail="fiscal_quarter parameters can only be used with quarterly granularity",
        )

    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        # Get company by ticker
        company = filings_db.companies.get_company_by_ticker(ticker)
        if not company:
            raise HTTPException(
                status_code=404, detail=f"Company with ticker '{ticker}' not found"
            )

        # Build filter parameters
        filter_kwargs = {"company_id": company.id}

        if fiscal_year_start is not None:
            filter_kwargs["fiscal_year_start"] = fiscal_year_start
        if fiscal_year_end is not None:
            filter_kwargs["fiscal_year_end"] = fiscal_year_end
        if fiscal_quarter_start is not None:
            filter_kwargs["fiscal_quarter_start"] = fiscal_quarter_start
        if fiscal_quarter_end is not None:
            filter_kwargs["fiscal_quarter_end"] = fiscal_quarter_end
        if label is not None:
            filter_kwargs["label"] = label
        if normalized_label is not None:
            filter_kwargs["normalized_label"] = normalized_label
        if statement is not None:
            filter_kwargs["statement"] = statement

        # Get financial data based on granularity
        if granularity == "quarterly":
            filter_params = QuarterlyFinancialsFilter(**filter_kwargs)
            metrics = filings_db.quarterly_financials.get_quarterly_financials(
                filter_params
            )
        else:  # yearly
            filter_params = YearlyFinancialsFilter(**filter_kwargs)
            metrics = filings_db.yearly_financials.get_yearly_financials(filter_params)

        # Convert to response format
        response_metrics = []
        for metric in metrics:
            response_metric = FinancialMetricResponse(
                company_id=metric.company_id,
                ticker=company.ticker,
                company_name=company.name,
                fiscal_year=metric.fiscal_year,
                fiscal_quarter=getattr(metric, "fiscal_quarter", None),
                label=metric.label,
                normalized_label=metric.normalized_label,
                value=float(metric.value),
                unit=metric.unit,
                statement=metric.statement,
                period_end=metric.period_end.isoformat() if metric.period_end else None,
                period_start=(
                    metric.period_start.isoformat() if metric.period_start else None
                ),
                source_type=getattr(metric, "source_type", None),
            )
            response_metrics.append(response_metric)

        return response_metrics

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving financial data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/normalized-labels", response_model=List[NormalizedLabelResponse])
async def get_normalized_labels(
    granularity: str = Query(
        ..., description="Data granularity: 'quarterly' or 'yearly'"
    ),
    statement: Optional[str] = Query(None, description="Filter by financial statement"),
) -> List[NormalizedLabelResponse]:
    """Get all normalized labels and their counts for quarterly or yearly financials."""

    # Validate granularity parameter
    if granularity not in ["quarterly", "yearly"]:
        raise HTTPException(
            status_code=400, detail="granularity must be either 'quarterly' or 'yearly'"
        )

    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        # Get normalized labels from the database
        if granularity == "quarterly":
            labels_data = filings_db.quarterly_financials.get_normalized_labels(
                statement
            )
        else:  # yearly
            labels_data = filings_db.yearly_financials.get_normalized_labels(statement)

        # Convert to response format
        response_labels = []
        for label_info in labels_data:
            response_label = NormalizedLabelResponse(
                normalized_label=label_info["normalized_label"],
                statement=label_info["statement"],
                count=label_info["count"],
            )
            response_labels.append(response_label)

        return response_labels

    except Exception as e:
        logger.error(f"Error retrieving normalized labels: {e}")
        raise HTTPException(status_code=500, detail=str(e))
