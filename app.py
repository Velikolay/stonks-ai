import logging
from typing import List, Optional

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from rag_system import RAGSystem

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="RAG API with GPT-4o mini",
    description="A RAG (Retrieval-Augmented Generation) API using LlamaIndex, FAISS, and OpenAI's GPT-4o mini",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize RAG system
rag_system = None


class QueryRequest(BaseModel):
    query: str
    top_k: Optional[int] = 3


class QueryResponse(BaseModel):
    answer: str
    sources: List[str]
    query: str


class DocumentUploadResponse(BaseModel):
    message: str
    document_count: int


@app.on_event("startup")
async def startup_event():
    """Initialize the RAG system on startup."""
    global rag_system
    try:
        rag_system = RAGSystem()
        logger.info("RAG system initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize RAG system: {e}")
        raise


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"message": "RAG API is running", "status": "healthy"}


@app.post("/query", response_model=QueryResponse)
async def query_rag(request: QueryRequest):
    """Query the RAG system."""
    if not rag_system:
        raise HTTPException(status_code=500, detail="RAG system not initialized")

    try:
        answer, sources = rag_system.query(request.query, top_k=request.top_k)
        return QueryResponse(answer=answer, sources=sources, query=request.query)
    except Exception as e:
        logger.error(f"Error querying RAG: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")


@app.post("/upload", response_model=DocumentUploadResponse)
async def upload_document(file: UploadFile = File(...)):
    """Upload a document to the RAG system."""
    if not rag_system:
        raise HTTPException(status_code=500, detail="RAG system not initialized")

    try:
        # Read file content
        content = await file.read()
        text_content = content.decode("utf-8")

        # Add document to RAG system
        rag_system.add_document(text_content, file.filename)

        return DocumentUploadResponse(
            message=f"Document '{file.filename}' uploaded successfully",
            document_count=rag_system.get_document_count(),
        )
    except Exception as e:
        logger.error(f"Error uploading document: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error uploading document: {str(e)}"
        )


@app.get("/documents/count")
async def get_document_count():
    """Get the number of documents in the RAG system."""
    if not rag_system:
        raise HTTPException(status_code=500, detail="RAG system not initialized")

    return {"document_count": rag_system.get_document_count()}


@app.delete("/documents/clear")
async def clear_documents():
    """Clear all documents from the RAG system."""
    if not rag_system:
        raise HTTPException(status_code=500, detail="RAG system not initialized")

    try:
        rag_system.clear_documents()
        return {"message": "All documents cleared successfully"}
    except Exception as e:
        logger.error(f"Error clearing documents: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error clearing documents: {str(e)}"
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
