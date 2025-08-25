"""FastAPI application for the RAG system."""

import logging
import os
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel

from filings.db import FilingsDatabase
from filings.models.quarterly_financials import QuarterlyFinancialsFilter
from filings.models.yearly_financials import YearlyFinancialsFilter
from rag_system import RAGSystem

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize systems
rag_system: Optional[RAGSystem] = None
filings_db: Optional[FilingsDatabase] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for FastAPI app."""
    global rag_system, filings_db
    try:
        # Initialize RAG system
        rag_system = RAGSystem()

        # Initialize FilingsDatabase
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")

        filings_db = FilingsDatabase(database_url)
        logger.info("RAG system and FilingsDatabase initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize systems: {e}")
        rag_system = None
        filings_db = None

    yield

    # Cleanup on shutdown
    if rag_system:
        logger.info("Shutting down RAG system")
    if filings_db:
        filings_db.close()
        logger.info("FilingsDatabase connection closed")


# Initialize FastAPI app
app = FastAPI(
    title="RAG API with GPT-4o mini",
    description="A RAG system using OpenAI GPT-4o mini and PostgreSQL with pgvector",
    version="1.0.0",
    lifespan=lifespan,
)


class QueryRequest(BaseModel):
    """Request model for querying the RAG system."""

    query: str
    top_k: Optional[int] = 5


class QueryResponse(BaseModel):
    """Response model for RAG queries."""

    response: str
    document_count: int


class DocumentResponse(BaseModel):
    """Response model for document operations."""

    message: str
    document_count: int


class ProcessFilingRequest(BaseModel):
    """Request model for processing SEC filings - single endpoint for everything."""

    ticker: str
    company_name: str
    filing_date: str  # Format: YYYY-MM-DD
    form_type: str = "10-Q"
    save_csv: bool = False
    csv_filename: Optional[str] = None
    store_in_database: bool = True


class ProcessFilingResponse(BaseModel):
    """Response model for filing processing - comprehensive result."""

    success: bool
    message: str
    ticker: str
    company_name: str
    filing_date: str
    form_type: str

    # File operations
    filing_path: Optional[str] = None
    csv_saved: bool = False
    csv_filename: Optional[str] = None

    # Database operations
    database_stored: bool = False
    company_id: Optional[int] = None
    filing_id: Optional[int] = None
    facts_count: Optional[int] = None

    # Processing details
    processing_time_seconds: Optional[float] = None
    error: Optional[str] = None


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


@app.get("/")
async def health_check() -> dict:
    """Health check endpoint."""
    return {"message": "RAG API is running!"}


@app.post("/query", response_model=QueryResponse)
async def query_rag(request: QueryRequest) -> QueryResponse:
    """Query the RAG system."""
    if not rag_system:
        raise HTTPException(status_code=500, detail="RAG system not initialized")

    try:
        response = rag_system.query(request.query, top_k=request.top_k or 5)
        return QueryResponse(
            response=response, document_count=rag_system.get_document_count()
        )
    except Exception as e:
        logger.error(f"Error querying RAG system: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/add-document", response_model=DocumentResponse)
async def add_document(
    content: str = Form(...), filename: Optional[str] = Form(None)
) -> DocumentResponse:
    """Add a document to the RAG system."""
    if not rag_system:
        raise HTTPException(status_code=500, detail="RAG system not initialized")

    try:
        rag_system.add_document(content, filename)
        return DocumentResponse(
            message="Document added successfully",
            document_count=rag_system.get_document_count(),
        )
    except Exception as e:
        logger.error(f"Error adding document: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload-document", response_model=DocumentResponse)
async def upload_document(file: UploadFile = File(...)) -> DocumentResponse:
    """Upload and add a document to the RAG system."""
    if not rag_system:
        raise HTTPException(status_code=500, detail="RAG system not initialized")

    try:
        content = await file.read()
        content_str = content.decode("utf-8")
        rag_system.add_document(content_str, file.filename)
        return DocumentResponse(
            message=f"Document {file.filename} uploaded and added successfully",
            document_count=rag_system.get_document_count(),
        )
    except Exception as e:
        logger.error(f"Error uploading document: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/clear-documents", response_model=DocumentResponse)
async def clear_documents() -> DocumentResponse:
    """Clear all documents from the RAG system."""
    if not rag_system:
        raise HTTPException(status_code=500, detail="RAG system not initialized")

    try:
        rag_system.clear_documents()
        return DocumentResponse(
            message="All documents cleared successfully", document_count=0
        )
    except Exception as e:
        logger.error(f"Error clearing documents: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# EDGAR XBRL Extractor endpoints
@app.post("/ingest/load-filing", response_model=ProcessFilingResponse)
async def process_filing(request: ProcessFilingRequest) -> ProcessFilingResponse:
    """Download SEC filing, extract data, and import into database."""
    pass


# Financial Data endpoints
@app.get("/financials", response_model=List[FinancialMetricResponse])
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


@app.get("/financials/normalized-labels", response_model=List[NormalizedLabelResponse])
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
