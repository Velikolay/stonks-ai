"""FastAPI application for the RAG system."""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

from edgar_xbrl_extractor import MinimalEdgarExtractor
from rag_system import RAGSystem

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="RAG API with GPT-4o mini",
    description="A RAG system using OpenAI GPT-4o mini and PostgreSQL with pgvector",
    version="1.0.0",
)

# Initialize RAG system
rag_system: Optional[RAGSystem] = None

# Initialize EDGAR XBRL extractor
edgar_extractor: Optional[MinimalEdgarExtractor] = None


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


@app.on_event("startup")
async def startup_event() -> None:
    """Initialize the RAG system and EDGAR extractor on startup."""
    global rag_system, edgar_extractor
    try:
        rag_system = RAGSystem()

        # Initialize EDGAR extractor with database support
        database_url = os.getenv("DATABASE_URL")
        edgar_extractor = MinimalEdgarExtractor(
            download_path="edgar_downloads",
            company_name="RAG API System",
            email_address="admin@ragapi.com",
            database_url=database_url,
        )
        logger.info("RAG system and EDGAR extractor initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize systems: {e}")
        rag_system = None
        edgar_extractor = None


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
    if not edgar_extractor:
        raise HTTPException(status_code=500, detail="EDGAR extractor not initialized")

    try:
        # Determine CSV filename if requested
        csv_filename = None
        if request.save_csv:
            if request.csv_filename:
                csv_filename = request.csv_filename
            else:
                csv_filename = (
                    f"{request.ticker.lower()}_{request.form_type.lower()}.csv"
                )

        # Process the filing
        start_time = datetime.now()
        result = edgar_extractor.process_and_store_10q_data(
            ticker=request.ticker,
            company_name=request.company_name,
            output_file=csv_filename if request.save_csv else None,
            store_in_db=request.store_in_database,
        )
        end_time = datetime.now()

        if result:
            return ProcessFilingResponse(
                success=True,
                message=f"Successfully processed {request.form_type} filing for {request.ticker}",
                ticker=request.ticker,
                company_name=request.company_name,
                filing_date=request.filing_date,
                form_type=request.form_type,
                filing_path=result.get("filing_path"),
                csv_saved=result.get("csv_saved", False),
                csv_filename=csv_filename if result.get("csv_saved") else None,
                database_stored=result.get("database_stored", False),
                company_id=(
                    result.get("company", {}).get("id")
                    if result.get("company")
                    else None
                ),
                filing_id=(
                    result.get("filing", {}).get("id") if result.get("filing") else None
                ),
                facts_count=result.get("facts_count"),
                processing_time_seconds=(end_time - start_time).total_seconds(),
            )
        else:
            return ProcessFilingResponse(
                success=False,
                message=f"Failed to process {request.form_type} filing for {request.ticker}",
                ticker=request.ticker,
                company_name=request.company_name,
                filing_date=request.filing_date,
                form_type=request.form_type,
                error="Processing failed - no result returned",
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing filing: {e}")
        return ProcessFilingResponse(
            success=False,
            message=f"Error processing {request.form_type} filing for {request.ticker}",
            ticker=request.ticker,
            company_name=request.company_name,
            filing_date=request.filing_date,
            form_type=request.form_type,
            error=str(e),
        )


@app.get("/edgar/health")
async def edgar_health_check() -> dict:
    """Health check for EDGAR extractor."""
    return {
        "message": "EDGAR XBRL Extractor is available",
        "extractor_initialized": edgar_extractor is not None,
        "database_connected": (
            edgar_extractor.database_url is not None if edgar_extractor else False
        ),
    }


@app.get("/edgar/debug-download")
async def debug_download_filing(ticker: str = "AAPL") -> dict:
    """Debug endpoint to download filing and show file structure."""
    if not edgar_extractor:
        raise HTTPException(status_code=500, detail="EDGAR extractor not initialized")

    try:
        # Download the filing
        filing_path = edgar_extractor.download_10q_filing(ticker)

        if not filing_path:
            return {
                "success": False,
                "message": f"Failed to download filing for {ticker}",
                "ticker": ticker,
                "filing_path": None,
                "files": [],
            }

        # Check what files are in the directory
        filing_dir = Path(filing_path)
        files_info = []

        if filing_dir.exists():
            for item in filing_dir.iterdir():
                if item.is_file():
                    try:
                        size = item.stat().st_size
                        with open(item, "r", encoding="utf-8") as f:
                            content = f.read()

                        file_info = {
                            "name": item.name,
                            "size_bytes": size,
                            "size_chars": len(content),
                            "has_xml": "<?xml" in content,
                            "has_xbrl": "<xbrl" in content.lower()
                            or "<XBRL" in content,
                            "first_200_chars": content[:200],
                        }
                        files_info.append(file_info)
                    except Exception as e:
                        files_info.append({"name": item.name, "error": str(e)})

        return {
            "success": True,
            "message": f"Successfully downloaded filing for {ticker}",
            "ticker": ticker,
            "filing_path": filing_path,
            "files": files_info,
        }

    except Exception as e:
        logger.error(f"Error in debug download: {e}")
        return {
            "success": False,
            "message": f"Error downloading filing for {ticker}",
            "ticker": ticker,
            "error": str(e),
        }
