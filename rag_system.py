import logging
import os
from pathlib import Path
from typing import List, Tuple

from llama_index.core import Document, Settings, SimpleDirectoryReader, VectorStoreIndex
from llama_index.core.query_engine import RetrieverQueryEngine
from llama_index.core.retrievers import VectorIndexRetriever
from llama_index.core.storage import StorageContext
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.llms.openai import OpenAI
from llama_index.vector_stores.postgres import PGVectorStore

logger = logging.getLogger(__name__)


class RAGSystem:
    """RAG system using LlamaIndex, PostgreSQL with pgvector, and OpenAI's GPT-4o mini."""

    def __init__(self, persist_dir: str = "./data"):
        """Initialize the RAG system."""
        self.persist_dir = Path(persist_dir)
        self.persist_dir.mkdir(exist_ok=True)

        # Initialize OpenAI API key
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")

        # Get model from environment or use default
        self.model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

        # Initialize LLM and embeddings
        self.llm = OpenAI(
            model=self.model_name, api_key=self.openai_api_key, temperature=0.1
        )

        self.embed_model = OpenAIEmbedding(
            model="text-embedding-3-small", api_key=self.openai_api_key
        )

        # Set global settings
        Settings.llm = self.llm
        Settings.embed_model = self.embed_model
        Settings.chunk_size = 1024
        Settings.chunk_overlap = 20

        # Initialize PostgreSQL vector store
        database_url = os.getenv(
            "DATABASE_URL", "postgresql://rag_user:rag_password@localhost:5432/rag_db"
        )
        self.vector_store = PGVectorStore.from_params(
            database_url=database_url,
            table_name="documents",
            embed_dim=1536,  # OpenAI text-embedding-3-small dimension
        )

        self.storage_context = StorageContext.from_defaults(
            vector_store=self.vector_store
        )

        # Initialize index (will be created when first document is added)
        self.index = None
        self.document_count = 0

        # Try to load existing index
        self._load_existing_index()

    def _load_existing_index(self):
        """Load existing index if available."""
        try:
            self.document_count = self._get_document_count()
            if self.document_count > 0:
                # Create index from existing vector store
                self.index = VectorStoreIndex.from_vector_store(
                    self.vector_store, storage_context=self.storage_context
                )
                logger.info(
                    f"Loaded existing index with {self.document_count} documents"
                )
        except Exception as e:
            logger.info(f"No existing documents found: {e}")

    def _get_document_count(self) -> int:
        """Get the number of documents in the database."""
        try:
            # This is a simplified count - in production you'd want proper connection handling
            from urllib.parse import urlparse

            import pg8000

            database_url = os.getenv(
                "DATABASE_URL",
                "postgresql://rag_user:rag_password@localhost:5432/rag_db",
            )
            parsed = urlparse(database_url)

            conn = pg8000.Connection(
                host=parsed.hostname,
                port=parsed.port or 5432,
                database=parsed.path[1:],
                user=parsed.username,
                password=parsed.password,
            )

            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM documents")
                count = cur.fetchone()[0]

            conn.close()
            return count
        except Exception as e:
            logger.error(f"Error getting document count: {e}")
            return 0

    def add_document(self, text: str, filename: str = None):
        """Add a document to the RAG system."""
        try:
            # Create document
            document = Document(text=text)
            if filename:
                document.metadata = {"filename": filename}

            # Create or update index
            if self.index is None:
                # First document - create new index
                self.index = VectorStoreIndex.from_documents(
                    [document], storage_context=self.storage_context, show_progress=True
                )
            else:
                # Add to existing index
                self.index.insert(document)

            self.document_count += 1
            logger.info(f"Added document: {filename or 'unnamed'}")

        except Exception as e:
            logger.error(f"Error adding document: {e}")
            raise

    def query(self, query: str, top_k: int = 3) -> Tuple[str, List[str]]:
        """Query the RAG system."""
        if self.index is None:
            raise ValueError("No documents have been added to the RAG system")

        try:
            # Create retriever
            retriever = VectorIndexRetriever(index=self.index, similarity_top_k=top_k)

            # Create query engine
            query_engine = RetrieverQueryEngine.from_args(
                retriever=retriever, llm=self.llm
            )

            # Execute query
            response = query_engine.query(query)

            # Extract sources
            sources = []
            for node in response.source_nodes:
                if hasattr(node, "metadata") and node.metadata:
                    source = node.metadata.get("filename", "Unknown source")
                else:
                    source = "Unknown source"
                sources.append(source)

            return str(response), sources

        except Exception as e:
            logger.error(f"Error querying RAG: {e}")
            raise

    def get_document_count(self) -> int:
        """Get the number of documents in the system."""
        return self._get_document_count()

    def clear_documents(self):
        """Clear all documents from the system."""
        try:
            from urllib.parse import urlparse

            import pg8000

            database_url = os.getenv(
                "DATABASE_URL",
                "postgresql://rag_user:rag_password@localhost:5432/rag_db",
            )
            parsed = urlparse(database_url)

            conn = pg8000.Connection(
                host=parsed.hostname,
                port=parsed.port or 5432,
                database=parsed.path[1:],
                user=parsed.username,
                password=parsed.password,
            )

            with conn.cursor() as cur:
                cur.execute("DELETE FROM documents")
                conn.commit()

            conn.close()

            self.index = None
            self.document_count = 0

            logger.info("All documents cleared")

        except Exception as e:
            logger.error(f"Error clearing documents: {e}")
            raise

    def add_documents_from_directory(self, directory_path: str):
        """Add all documents from a directory."""
        try:
            reader = SimpleDirectoryReader(input_dir=directory_path)
            documents = reader.load_data()

            for doc in documents:
                self.add_document(doc.text, doc.metadata.get("file_name", "Unknown"))

            logger.info(f"Added {len(documents)} documents from directory")

        except Exception as e:
            logger.error(f"Error adding documents from directory: {e}")
            raise
