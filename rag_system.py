"""RAG System using OpenAI GPT-4o mini and PostgreSQL with pgvector."""

import logging
import os
from typing import Optional

from llama_index.core import Document, VectorStoreIndex
from llama_index.core.node_parser import SentenceSplitter
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.llms.openai import OpenAI
from llama_index.vector_stores.postgres import PGVectorStore

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RAGSystem:
    """RAG System using OpenAI GPT-4o mini and PostgreSQL with pgvector."""

    def __init__(self) -> None:
        """Initialize the RAG system with OpenAI models and PostgreSQL vector store."""
        # Check for required environment variables
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            raise ValueError(
                "OPENAI_API_KEY environment variable is required. "
                "Please set it in your .env file."
            )

        # Initialize OpenAI LLM and embedding model
        self.llm = OpenAI(
            model="gpt-4o-mini",
            temperature=0.1,
            max_tokens=1000,
        )
        self.embed_model = OpenAIEmbedding(
            model="text-embedding-3-small",
            embed_batch_size=100,
        )

        # Initialize PostgreSQL vector store
        database_url = os.getenv(
            "DATABASE_URL", "postgresql://rag_user:rag_password@localhost:5432/rag_db"
        )
        self.vector_store = PGVectorStore.from_params(
            database=database_url,
            table_name="documents",
            embed_dim=1536,
        )

        # Initialize index
        self.index: Optional[VectorStoreIndex] = None
        self.document_count = 0
        self._load_existing_index()

    def _load_existing_index(self) -> None:
        """Load existing index from vector store."""
        try:
            # Check if we have documents in the vector store
            if self.document_count > 0:
                self.index = VectorStoreIndex.from_vector_store(
                    self.vector_store,
                    embed_model=self.embed_model,
                )
                logger.info(
                    f"Loaded existing index with {self.document_count} documents"
                )
            else:
                logger.info("No existing documents found, starting with empty index")
        except Exception as e:
            logger.warning(f"Could not load existing index: {e}")
            logger.info("Starting with empty index")

    def _get_document_count(self) -> int:
        """Get the number of documents in the vector store."""
        try:
            # This is a simplified count - in production you'd want a proper count query
            return 0  # For now, assume empty
        except Exception as e:
            logger.warning(f"Could not get document count: {e}")
            return 0

    def add_document(self, content: str, filename: Optional[str] = None) -> None:
        """Add a document to the RAG system."""
        try:
            # Create document
            document = Document(text=content, metadata={"filename": filename})

            # Split into nodes
            splitter = SentenceSplitter(chunk_size=1024, chunk_overlap=20)
            nodes = splitter.get_nodes_from_documents([document])

            # Add to vector store
            self.vector_store.add(nodes)

            # Update index
            self.index = VectorStoreIndex.from_vector_store(
                self.vector_store,
                embed_model=self.embed_model,
            )

            self.document_count += 1
            logger.info(f"Added document: {filename or 'unnamed'}")

        except Exception as e:
            logger.error(f"Error adding document: {e}")
            raise

    def query(self, query_text: str, top_k: int = 5) -> str:
        """Query the RAG system."""
        if not self.index:
            return (
                "No documents available for querying. Please add some documents first."
            )

        try:
            # Create query engine
            query_engine = self.index.as_query_engine(
                llm=self.llm,
                similarity_top_k=top_k,
            )

            # Get response
            response = query_engine.query(query_text)
            return str(response)

        except Exception as e:
            logger.error(f"Error querying RAG system: {e}")
            return f"Error processing query: {e}"

    def get_document_count(self) -> int:
        """Get the number of documents in the system."""
        return self.document_count

    def clear_documents(self) -> None:
        """Clear all documents from the system."""
        try:
            # Clear vector store
            self.vector_store.clear()

            # Reset index
            self.index = None
            self.document_count = 0

            logger.info("Cleared all documents from the system")

        except Exception as e:
            logger.error(f"Error clearing documents: {e}")
            raise
