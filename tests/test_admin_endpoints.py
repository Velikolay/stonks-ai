"""Tests for admin API endpoints."""

import csv
import io
from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client() -> TestClient:
    """Create a test client for the FastAPI app."""
    from app import app

    return TestClient(app)


@pytest.fixture
def sample_override_data():
    """Sample override data for testing."""
    return {
        "concept": "us-gaap:TestConcept",
        "statement": "Income Statement",
        "normalized_label": "Test Label",
        "is_abstract": False,
        "parent_concept": None,
        "description": "Test description",
    }


@pytest.fixture
def mock_override():
    """Mock override object for testing."""
    mock = Mock()
    mock.concept = "us-gaap:TestConcept"
    mock.statement = "Income Statement"
    mock.normalized_label = "Test Label"
    mock.is_abstract = False
    mock.parent_concept = None
    mock.description = "Test description"
    mock.created_at = datetime(2024, 1, 1, 0, 0, 0)
    mock.updated_at = datetime(2024, 1, 1, 0, 0, 0)
    return mock


class TestAdminEndpoints:
    """Test admin API endpoints."""

    @patch("api.admin.filings_db")
    def test_list_overrides_empty(self, mock_filings_db, client):
        """Test listing overrides when database is empty."""
        mock_filings_db.concept_normalization_overrides.list_all.return_value = []

        response = client.get("/admin/concept-normalization-overrides")

        assert response.status_code == 200
        assert response.json() == []

    @patch("api.admin.filings_db")
    def test_list_overrides_with_data(self, mock_filings_db, client, mock_override):
        """Test listing overrides with data."""
        mock_filings_db.concept_normalization_overrides.list_all.return_value = [
            mock_override
        ]

        response = client.get("/admin/concept-normalization-overrides")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["concept"] == "us-gaap:TestConcept"
        assert data[0]["statement"] == "Income Statement"
        assert data[0]["normalized_label"] == "Test Label"

    @patch("api.admin.filings_db")
    def test_list_overrides_with_statement_filter(
        self, mock_filings_db, client, mock_override
    ):
        """Test listing overrides with statement filter."""
        mock_filings_db.concept_normalization_overrides.list_all.return_value = [
            mock_override
        ]

        response = client.get(
            "/admin/concept-normalization-overrides?statement=Income Statement"
        )

        assert response.status_code == 200
        mock_filings_db.concept_normalization_overrides.list_all.assert_called_once_with(
            "Income Statement"
        )

    @patch("api.admin.filings_db", None)
    def test_list_overrides_database_not_initialized(self, client):
        """Test listing overrides when database is not initialized."""
        response = client.get("/admin/concept-normalization-overrides")

        assert response.status_code == 500
        assert "not initialized" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_create_override_success(
        self, mock_filings_db, client, sample_override_data, mock_override
    ):
        """Test creating an override successfully."""
        mock_filings_db.concept_normalization_overrides.create.return_value = (
            mock_override
        )

        response = client.post(
            "/admin/concept-normalization-overrides", json=sample_override_data
        )

        assert response.status_code == 201
        data = response.json()
        assert data["concept"] == "us-gaap:TestConcept"
        assert data["normalized_label"] == "Test Label"

    @patch("api.admin.filings_db")
    def test_create_override_validation_error(
        self, mock_filings_db, client, sample_override_data
    ):
        """Test creating override with validation error."""
        mock_filings_db.concept_normalization_overrides.create.side_effect = ValueError(
            "Invalid data"
        )

        response = client.post(
            "/admin/concept-normalization-overrides", json=sample_override_data
        )

        assert response.status_code == 400
        assert "Invalid data" in response.json()["detail"]

    @patch("api.admin.filings_db")
    def test_update_override_success(self, mock_filings_db, client, mock_override):
        """Test updating an override successfully."""
        updated_mock = Mock()
        updated_mock.concept = "us-gaap:TestConcept"
        updated_mock.statement = "Income Statement"
        updated_mock.normalized_label = "Updated Label"
        updated_mock.is_abstract = False
        updated_mock.parent_concept = None
        updated_mock.description = "Updated description"
        updated_mock.created_at = datetime(2024, 1, 1, 0, 0, 0)
        updated_mock.updated_at = datetime(2024, 1, 2, 0, 0, 0)

        mock_filings_db.concept_normalization_overrides.update.return_value = (
            updated_mock
        )

        update_data = {
            "normalized_label": "Updated Label",
            "description": "Updated description",
        }

        response = client.put(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept",
            json=update_data,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["normalized_label"] == "Updated Label"
        assert data["description"] == "Updated description"

    @patch("api.admin.filings_db")
    def test_update_override_not_found(self, mock_filings_db, client):
        """Test updating non-existent override."""
        mock_filings_db.concept_normalization_overrides.update.return_value = None

        update_data = {"normalized_label": "Updated Label"}

        response = client.put(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ANonExistent",
            json=update_data,
        )

        assert response.status_code == 404

    @patch("api.admin.filings_db")
    def test_delete_override_success(self, mock_filings_db, client):
        """Test deleting an override successfully."""
        mock_filings_db.concept_normalization_overrides.delete.return_value = True

        response = client.delete(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept"
        )

        assert response.status_code == 204

    @patch("api.admin.filings_db")
    def test_delete_override_not_found(self, mock_filings_db, client):
        """Test deleting non-existent override."""
        mock_filings_db.concept_normalization_overrides.delete.return_value = False

        response = client.delete(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ANonExistent"
        )

        assert response.status_code == 404

    @patch("api.admin.filings_db")
    def test_delete_override_validation_error(self, mock_filings_db, client):
        """Test deleting override with validation error."""
        mock_filings_db.concept_normalization_overrides.delete.side_effect = ValueError(
            "Cannot delete: record is referenced"
        )

        response = client.delete(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept"
        )

        assert response.status_code == 400

    @patch("api.admin.filings_db")
    def test_export_to_csv(self, mock_filings_db, client, mock_override):
        """Test exporting overrides to CSV."""
        mock_filings_db.concept_normalization_overrides.list_all.return_value = [
            mock_override
        ]

        response = client.get("/admin/concept-normalization-overrides/export")

        assert response.status_code == 200
        assert response.headers["content-type"] == "text/csv; charset=utf-8"
        assert "attachment" in response.headers["content-disposition"]

        # Parse CSV content
        csv_content = response.text
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(csv_reader)

        assert len(rows) == 1
        assert rows[0]["concept"] == "us-gaap:TestConcept"
        assert rows[0]["statement"] == "Income Statement"
        assert rows[0]["normalized_label"] == "Test Label"
        assert rows[0]["is_abstract"] == "False"

    @patch("api.admin.filings_db")
    def test_export_to_csv_with_statement_filter(
        self, mock_filings_db, client, mock_override
    ):
        """Test exporting overrides to CSV with statement filter."""
        mock_filings_db.concept_normalization_overrides.list_all.return_value = [
            mock_override
        ]

        response = client.get(
            "/admin/concept-normalization-overrides/export?statement=Income Statement"
        )

        assert response.status_code == 200
        assert "Income_Statement" in response.headers["content-disposition"]

    @patch("api.admin.filings_db")
    def test_import_from_csv_success(self, mock_filings_db, client):
        """Test importing overrides from CSV."""
        # Setup mocks
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = None
        mock_created = Mock()
        mock_created.concept = "us-gaap:TestConcept"
        mock_created.statement = "Income Statement"
        mock_created.normalized_label = "Test Label"
        mock_created.is_abstract = False
        mock_created.parent_concept = None
        mock_created.description = "Test description"
        mock_created.created_at = datetime(2024, 1, 1, 0, 0, 0)
        mock_created.updated_at = datetime(2024, 1, 1, 0, 0, 0)
        mock_filings_db.concept_normalization_overrides.create.return_value = (
            mock_created
        )

        # Create CSV content
        csv_content = io.StringIO()
        writer = csv.DictWriter(
            csv_content,
            fieldnames=[
                "concept",
                "statement",
                "normalized_label",
                "is_abstract",
                "parent_concept",
                "description",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "false",
                "parent_concept": "",
                "description": "Test description",
            }
        )

        # Prepare file for upload
        files = {"file": ("test.csv", csv_content.getvalue(), "text/csv")}

        response = client.post(
            "/admin/concept-normalization-overrides/import",
            files=files,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["created"] == 1
        assert data["updated"] == 0
        assert len(data["errors"]) == 0

    @patch("api.admin.filings_db")
    def test_import_from_csv_with_update(self, mock_filings_db, client, mock_override):
        """Test importing CSV with update_existing=True."""
        # Mock existing override
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )
        mock_updated = Mock()
        mock_updated.concept = "us-gaap:TestConcept"
        mock_updated.statement = "Income Statement"
        mock_updated.normalized_label = "Updated Label"
        mock_updated.is_abstract = False
        mock_updated.parent_concept = None
        mock_updated.description = None
        mock_updated.created_at = datetime(2024, 1, 1, 0, 0, 0)
        mock_updated.updated_at = datetime(2024, 1, 2, 0, 0, 0)
        mock_filings_db.concept_normalization_overrides.update.return_value = (
            mock_updated
        )

        # Create CSV content
        csv_content = io.StringIO()
        writer = csv.DictWriter(
            csv_content,
            fieldnames=[
                "concept",
                "statement",
                "normalized_label",
                "is_abstract",
                "parent_concept",
                "description",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Updated Label",
                "is_abstract": "false",
                "parent_concept": "",
                "description": "",
            }
        )

        files = {"file": ("test.csv", csv_content.getvalue(), "text/csv")}

        response = client.post(
            "/admin/concept-normalization-overrides/import?update_existing=true",
            files=files,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["created"] == 0
        assert data["updated"] == 1
        assert len(data["errors"]) == 0

    @patch("api.admin.filings_db")
    def test_import_from_csv_without_update_existing(
        self, mock_filings_db, client, mock_override
    ):
        """Test importing CSV with existing records and update_existing=False."""
        # Mock existing override
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )

        # Create CSV content
        csv_content = io.StringIO()
        writer = csv.DictWriter(
            csv_content,
            fieldnames=[
                "concept",
                "statement",
                "normalized_label",
                "is_abstract",
                "parent_concept",
                "description",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "false",
                "parent_concept": "",
                "description": "",
            }
        )

        files = {"file": ("test.csv", csv_content.getvalue(), "text/csv")}

        response = client.post(
            "/admin/concept-normalization-overrides/import?update_existing=false",
            files=files,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["created"] == 0
        assert data["updated"] == 0
        assert len(data["errors"]) == 1
        assert "already exists" in data["errors"][0].lower()

    @patch("api.admin.filings_db")
    def test_import_from_csv_with_errors(self, mock_filings_db, client):
        """Test importing CSV with validation errors."""
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = None

        # Create CSV with missing required fields
        csv_content = io.StringIO()
        writer = csv.DictWriter(
            csv_content,
            fieldnames=[
                "concept",
                "statement",
                "normalized_label",
                "is_abstract",
                "parent_concept",
                "description",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "false",
                "parent_concept": "",
                "description": "",
            }
        )

        files = {"file": ("test.csv", csv_content.getvalue(), "text/csv")}

        response = client.post(
            "/admin/concept-normalization-overrides/import",
            files=files,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["created"] == 0
        assert data["updated"] == 0
        assert len(data["errors"]) > 0
        assert any("required fields" in error.lower() for error in data["errors"])

    @patch("api.admin.filings_db", None)
    def test_import_from_csv_database_not_initialized(self, client):
        """Test importing CSV when database is not initialized."""
        csv_content = io.StringIO()
        files = {"file": ("test.csv", csv_content.getvalue(), "text/csv")}

        response = client.post(
            "/admin/concept-normalization-overrides/import",
            files=files,
        )

        assert response.status_code == 500

    @patch("api.admin.filings_db")
    def test_import_from_csv_invalid_file_type(self, mock_filings_db, client):
        """Test importing non-CSV file."""
        # Mock database to pass initialization check
        mock_filings_db.concept_normalization_overrides = Mock()

        files = {"file": ("test.txt", "not a csv", "text/plain")}

        response = client.post(
            "/admin/concept-normalization-overrides/import",
            files=files,
        )

        assert response.status_code == 400
        assert "csv" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_import_from_csv_multiple_rows(self, mock_filings_db, client):
        """Test importing CSV with multiple rows."""
        # Mock no existing records
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = None

        # Create mock for created records
        def create_side_effect(override):
            mock = Mock()
            mock.concept = override.concept
            mock.statement = override.statement
            mock.normalized_label = override.normalized_label
            mock.is_abstract = override.is_abstract
            mock.parent_concept = override.parent_concept
            mock.description = override.description
            mock.created_at = datetime(2024, 1, 1, 0, 0, 0)
            mock.updated_at = datetime(2024, 1, 1, 0, 0, 0)
            return mock

        mock_filings_db.concept_normalization_overrides.create.side_effect = (
            create_side_effect
        )

        # Create CSV with multiple rows
        csv_content = io.StringIO()
        writer = csv.DictWriter(
            csv_content,
            fieldnames=[
                "concept",
                "statement",
                "normalized_label",
                "is_abstract",
                "parent_concept",
                "description",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:Concept1",
                "statement": "Income Statement",
                "normalized_label": "Label 1",
                "is_abstract": "false",
                "parent_concept": "",
                "description": "",
            }
        )
        writer.writerow(
            {
                "concept": "us-gaap:Concept2",
                "statement": "Balance Sheet",
                "normalized_label": "Label 2",
                "is_abstract": "true",
                "parent_concept": "",
                "description": "",
            }
        )

        files = {"file": ("test.csv", csv_content.getvalue(), "text/csv")}

        response = client.post(
            "/admin/concept-normalization-overrides/import",
            files=files,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["created"] == 2
        assert data["updated"] == 0
        assert len(data["errors"]) == 0
