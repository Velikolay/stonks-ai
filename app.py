"""FastAPI application for the RAG system."""

import logging
from typing import Optional

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from pydantic import BaseModel

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
    global rag_system
    try:
        rag_system = RAGSystem()
        # Initialize EDGAR extractor with database support
        # database_url = os.getenv("DATABASE_URL")
        logger.info("RAG system and EDGAR extractor initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize systems: {e}")
        rag_system = None


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
