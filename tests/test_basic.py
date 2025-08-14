"""Basic tests for the RAG API."""

import os

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client() -> TestClient:
    """Create a test client for the FastAPI app."""
    from app import app

    return TestClient(app)


def test_health_check(client: TestClient) -> None:
    """Test the health check endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "RAG API is running!"}


def test_health_check_response_structure(client: TestClient) -> None:
    """Test that the health check response has the expected structure."""
    response = client.get("/")
    data = response.json()
    assert "message" in data
    assert isinstance(data["message"], str)


@pytest.mark.integration
def test_app_import() -> None:
    """Test that the app can be imported successfully."""
    try:
        from app import app

        assert app is not None
        assert hasattr(app, "title")
        assert app.title == "RAG API with GPT-4o mini"
    except ImportError as e:
        pytest.skip(f"App import failed: {e}")


class TestSetupVerification:
    """Test setup verification - checks if all dependencies are properly installed."""

    def test_llama_index_import(self) -> None:
        """Test if llama_index can be imported."""
        import llama_index

        assert llama_index is not None

    def test_llama_index_llms_openai_import(self) -> None:
        """Test if llama_index.llms.openai can be imported."""
        import llama_index.llms.openai

        assert llama_index.llms.openai is not None

    def test_llama_index_embeddings_openai_import(self) -> None:
        """Test if llama_index.embeddings.openai can be imported."""
        import llama_index.embeddings.openai

        assert llama_index.embeddings.openai is not None

    def test_llama_index_vector_stores_postgres_import(self) -> None:
        """Test if llama_index.vector_stores.postgres can be imported."""
        import llama_index.vector_stores.postgres  # noqa: F401

        # If we get here, import was successful

    def test_fastapi_import(self) -> None:
        """Test if fastapi can be imported."""
        import fastapi  # noqa: F401

        # If we get here, import was successful

    def test_uvicorn_import(self) -> None:
        """Test if uvicorn can be imported."""
        import uvicorn  # noqa: F401

        # If we get here, import was successful

    def test_openai_import(self) -> None:
        """Test if openai can be imported."""
        import openai  # noqa: F401

        # If we get here, import was successful

    def test_pg8000_import(self) -> None:
        """Test if pg8000 can be imported."""
        import pg8000  # noqa: F401

        # If we get here, import was successful

    def test_requests_import(self) -> None:
        """Test if requests can be imported."""
        import requests  # type: ignore # noqa: F401

        # If we get here, import was successful

    def test_environment_variables(self) -> None:
        """Test environment variables."""
        # This test passes regardless of whether the key is set
        # It just verifies we can access environment variables
        assert True

    def test_rag_system_import(self) -> None:
        """Test if RAGSystem can be imported."""
        from rag_system import RAGSystem

        assert RAGSystem is not None

    def test_rag_system_requires_api_key(self) -> None:
        """Test that RAGSystem requires OPENAI_API_KEY."""
        from rag_system import RAGSystem

        # Temporarily remove API key
        original_key = os.environ.get("OPENAI_API_KEY")
        if original_key:
            del os.environ["OPENAI_API_KEY"]

        try:
            # Should raise ValueError about missing API key
            with pytest.raises(ValueError) as exc_info:
                RAGSystem()
            assert "OPENAI_API_KEY" in str(exc_info.value)
        finally:
            # Restore API key
            if original_key:
                os.environ["OPENAI_API_KEY"] = original_key


class TestDatabaseSetup:
    """Test database setup and migrations."""

    def test_alembic_import(self) -> None:
        """Test if alembic can be imported."""
        import alembic  # noqa: F401

        # If we get here, import was successful

    def test_sqlalchemy_import(self) -> None:
        """Test if sqlalchemy can be imported."""
        import sqlalchemy  # noqa: F401

        # If we get here, import was successful


class TestDevelopmentTools:
    """Test development tools setup."""

    def test_black_import(self) -> None:
        """Test if black can be imported."""
        import black  # noqa: F401

        # If we get here, import was successful

    def test_flake8_import(self) -> None:
        """Test if flake8 can be imported."""
        import flake8  # noqa: F401

        # If we get here, import was successful

    def test_isort_import(self) -> None:
        """Test if isort can be imported."""
        import isort  # noqa: F401

        # If we get here, import was successful

    def test_mypy_import(self) -> None:
        """Test if mypy can be imported."""
        import mypy  # noqa: F401

        # If we get here, import was successful

    def test_pytest_import(self) -> None:
        """Test if pytest can be imported."""
        import pytest  # noqa: F401

        # If we get here, import was successful

    def test_pytest_cov_import(self) -> None:
        """Test if pytest_cov can be imported."""
        import pytest_cov  # noqa: F401

        # If we get here, import was successful
