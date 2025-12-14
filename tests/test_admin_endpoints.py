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
        "abstract_concept": None,
        "description": "Test description",
        "unit": "USD",
        "weight": None,
        "parent_concept": None,
    }


@pytest.fixture
def mock_override():
    """Mock override object for testing."""
    mock = Mock()
    mock.concept = "us-gaap:TestConcept"
    mock.statement = "Income Statement"
    mock.normalized_label = "Test Label"
    mock.is_abstract = False
    mock.abstract_concept = None
    mock.description = "Test description"
    mock.unit = "USD"  # Non-abstract records need unit
    mock.weight = None
    mock.parent_concept = None
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
        # Set existing record (non-abstract with unit for validation)
        mock_override.is_abstract = False
        mock_override.unit = "USD"
        mock_override.weight = None
        mock_override.parent_concept = None
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )

        updated_mock = Mock()
        updated_mock.concept = "us-gaap:TestConcept"
        updated_mock.statement = "Income Statement"
        updated_mock.normalized_label = "Updated Label"
        updated_mock.is_abstract = False
        updated_mock.abstract_concept = None
        updated_mock.description = "Updated description"
        updated_mock.unit = "USD"
        updated_mock.weight = None
        updated_mock.parent_concept = None
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
    def test_create_override_parent_concept_with_abstract_fails(
        self, mock_filings_db, client
    ):
        """Test creating override with parent_concept and is_abstract=True fails."""
        override_data = {
            "concept": "us-gaap:TestConcept",
            "statement": "Income Statement",
            "normalized_label": "Test Label",
            "is_abstract": True,
            "parent_concept": "us-gaap:ParentConcept",
            "unit": None,
            "weight": None,
        }

        response = client.post(
            "/admin/concept-normalization-overrides", json=override_data
        )

        assert response.status_code == 400
        assert "parent_concept" in response.json()["detail"].lower()
        assert "abstract" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_create_override_parent_concept_without_weight_fails(
        self, mock_filings_db, client
    ):
        """Test creating override with parent_concept but no weight fails."""
        override_data = {
            "concept": "us-gaap:TestConcept",
            "statement": "Income Statement",
            "normalized_label": "Test Label",
            "is_abstract": False,
            "parent_concept": "us-gaap:ParentConcept",
            "unit": "USD",
            "weight": None,
        }

        response = client.post(
            "/admin/concept-normalization-overrides", json=override_data
        )

        assert response.status_code == 400
        assert "parent_concept" in response.json()["detail"].lower()
        assert "weight" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_create_override_non_abstract_without_unit_fails(
        self, mock_filings_db, client
    ):
        """Test creating non-abstract override without unit fails."""
        override_data = {
            "concept": "us-gaap:TestConcept",
            "statement": "Income Statement",
            "normalized_label": "Test Label",
            "is_abstract": False,
            "parent_concept": None,
            "unit": None,
            "weight": None,
        }

        response = client.post(
            "/admin/concept-normalization-overrides", json=override_data
        )

        assert response.status_code == 400
        assert "unit" in response.json()["detail"].lower()
        assert (
            "non-abstract" in response.json()["detail"].lower()
            or "is_abstract=false" in response.json()["detail"].lower()
        )

    @patch("api.admin.filings_db")
    def test_create_override_abstract_with_parent_concept_fails(
        self, mock_filings_db, client
    ):
        """Test creating abstract override with parent_concept fails."""
        override_data = {
            "concept": "us-gaap:TestConcept",
            "statement": "Income Statement",
            "normalized_label": "Test Label",
            "is_abstract": True,
            "parent_concept": "us-gaap:ParentConcept",
            "unit": None,
            "weight": None,
        }

        response = client.post(
            "/admin/concept-normalization-overrides", json=override_data
        )

        assert response.status_code == 400
        assert "abstract" in response.json()["detail"].lower()
        assert "parent_concept" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_create_override_abstract_with_weight_fails(self, mock_filings_db, client):
        """Test creating abstract override with weight fails."""
        override_data = {
            "concept": "us-gaap:TestConcept",
            "statement": "Income Statement",
            "normalized_label": "Test Label",
            "is_abstract": True,
            "parent_concept": None,
            "unit": None,
            "weight": 1.0,
        }

        response = client.post(
            "/admin/concept-normalization-overrides", json=override_data
        )

        assert response.status_code == 400
        assert "abstract" in response.json()["detail"].lower()
        assert "weight" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_create_override_abstract_with_unit_fails(self, mock_filings_db, client):
        """Test creating abstract override with unit fails."""
        override_data = {
            "concept": "us-gaap:TestConcept",
            "statement": "Income Statement",
            "normalized_label": "Test Label",
            "is_abstract": True,
            "parent_concept": None,
            "unit": "USD",
            "weight": None,
        }

        response = client.post(
            "/admin/concept-normalization-overrides", json=override_data
        )

        assert response.status_code == 400
        assert "abstract" in response.json()["detail"].lower()
        assert "unit" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_delete_override_success(self, mock_filings_db, client):
        """Test deleting an override successfully."""
        mock_filings_db.concept_normalization_overrides.delete.return_value = True

        response = client.delete(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept"
        )

        assert response.status_code == 204

    @patch("api.admin.filings_db")
    def test_update_override_parent_concept_with_abstract_fails(
        self, mock_filings_db, client, mock_override
    ):
        """Test updating override to have parent_concept with is_abstract=True fails."""
        # Set existing to non-abstract with parent_concept
        mock_override.is_abstract = False
        mock_override.parent_concept = "us-gaap:ParentConcept"
        mock_override.unit = "USD"
        mock_override.weight = 1.0
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )

        update_data = {"is_abstract": True}

        response = client.put(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept",
            json=update_data,
        )

        assert response.status_code == 400
        assert "parent_concept" in response.json()["detail"].lower()
        assert "abstract" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_update_override_parent_concept_without_weight_fails(
        self, mock_filings_db, client, mock_override
    ):
        """Test updating override to have parent_concept without weight fails."""
        # Set existing to non-abstract
        mock_override.is_abstract = False
        mock_override.parent_concept = None
        mock_override.unit = "USD"
        mock_override.weight = None
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )

        update_data = {"parent_concept": "us-gaap:ParentConcept"}

        response = client.put(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept",
            json=update_data,
        )

        assert response.status_code == 400
        assert "parent_concept" in response.json()["detail"].lower()
        assert "weight" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_update_override_non_abstract_without_unit_fails(
        self, mock_filings_db, client, mock_override
    ):
        """Test updating override to be non-abstract without unit fails."""
        # Set existing to abstract
        mock_override.is_abstract = True
        mock_override.parent_concept = None
        mock_override.unit = None
        mock_override.weight = None
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )

        update_data = {"is_abstract": False}

        response = client.put(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept",
            json=update_data,
        )

        assert response.status_code == 400
        assert "unit" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_update_override_abstract_with_parent_concept_fails(
        self, mock_filings_db, client, mock_override
    ):
        """Test updating abstract override to have parent_concept fails."""
        # Set existing to abstract
        mock_override.is_abstract = True
        mock_override.parent_concept = None
        mock_override.unit = None
        mock_override.weight = None
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )

        update_data = {"parent_concept": "us-gaap:ParentConcept"}

        response = client.put(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept",
            json=update_data,
        )

        assert response.status_code == 400
        assert "abstract" in response.json()["detail"].lower()
        assert "parent_concept" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_update_override_abstract_with_weight_fails(
        self, mock_filings_db, client, mock_override
    ):
        """Test updating abstract override to have weight fails."""
        # Set existing to abstract
        mock_override.is_abstract = True
        mock_override.parent_concept = None
        mock_override.unit = None
        mock_override.weight = None
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )

        update_data = {"weight": 1.0}

        response = client.put(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept",
            json=update_data,
        )

        assert response.status_code == 400
        assert "abstract" in response.json()["detail"].lower()
        assert "weight" in response.json()["detail"].lower()

    @patch("api.admin.filings_db")
    def test_update_override_abstract_with_unit_fails(
        self, mock_filings_db, client, mock_override
    ):
        """Test updating abstract override to have unit fails."""
        # Set existing to abstract
        mock_override.is_abstract = True
        mock_override.parent_concept = None
        mock_override.unit = None
        mock_override.weight = None
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )

        update_data = {"unit": "USD"}

        response = client.put(
            "/admin/concept-normalization-overrides/Income%20Statement/us-gaap%3ATestConcept",
            json=update_data,
        )

        assert response.status_code == 400
        assert "abstract" in response.json()["detail"].lower()
        assert "unit" in response.json()["detail"].lower()

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
        mock_created.abstract_concept = None
        mock_created.description = "Test description"
        mock_created.unit = "USD"
        mock_created.weight = None
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
                "abstract_concept",
                "description",
                "unit",
                "weight",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "false",
                "abstract_concept": "",
                "description": "Test description",
                "unit": "USD",
                "weight": "",
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
        mock_updated.abstract_concept = None
        mock_updated.description = None
        mock_updated.unit = "USD"
        mock_updated.weight = None
        mock_updated.parent_concept = None
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
                "abstract_concept",
                "description",
                "unit",
                "weight",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Updated Label",
                "is_abstract": "false",
                "abstract_concept": "",
                "description": "",
                "unit": "USD",
                "weight": "",
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
                "abstract_concept",
                "description",
                "unit",
                "weight",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "false",
                "abstract_concept": "",
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
        assert len(data["errors"]) > 0
        assert any("already exists" in error.lower() for error in data["errors"])

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
                "abstract_concept",
                "description",
                "unit",
                "weight",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "false",
                "abstract_concept": "",
                "description": "",
                "unit": "USD",
                "weight": "",
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

    @patch("api.admin.filings_db")
    def test_import_from_csv_non_abstract_without_unit_fails(
        self, mock_filings_db, client
    ):
        """Test importing CSV with non-abstract record without unit fails."""
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = None

        csv_content = io.StringIO()
        writer = csv.DictWriter(
            csv_content,
            fieldnames=[
                "concept",
                "statement",
                "normalized_label",
                "is_abstract",
                "abstract_concept",
                "parent_concept",
                "description",
                "unit",
                "weight",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "false",
                "abstract_concept": "",
                "parent_concept": "",
                "description": "",
                "unit": "",
                "weight": "",
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
        assert len(data["errors"]) > 0
        assert any(
            "unit" in error.lower() and "non-abstract" in error.lower()
            for error in data["errors"]
        )

    @patch("api.admin.filings_db")
    def test_import_from_csv_abstract_with_unit_fails(self, mock_filings_db, client):
        """Test importing CSV with abstract record with unit fails."""
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = None

        csv_content = io.StringIO()
        writer = csv.DictWriter(
            csv_content,
            fieldnames=[
                "concept",
                "statement",
                "normalized_label",
                "is_abstract",
                "abstract_concept",
                "parent_concept",
                "description",
                "unit",
                "weight",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "true",
                "abstract_concept": "",
                "parent_concept": "",
                "description": "",
                "unit": "USD",
                "weight": "",
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
        assert len(data["errors"]) > 0
        assert any(
            "abstract" in error.lower() and "unit" in error.lower()
            for error in data["errors"]
        )

    @patch("api.admin.filings_db")
    def test_import_from_csv_parent_concept_without_weight_fails(
        self, mock_filings_db, client
    ):
        """Test importing CSV with parent_concept but no weight fails."""
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = None

        csv_content = io.StringIO()
        writer = csv.DictWriter(
            csv_content,
            fieldnames=[
                "concept",
                "statement",
                "normalized_label",
                "is_abstract",
                "abstract_concept",
                "parent_concept",
                "description",
                "unit",
                "weight",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "false",
                "abstract_concept": "",
                "parent_concept": "us-gaap:ParentConcept",
                "description": "",
                "unit": "USD",
                "weight": "",
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
        assert len(data["errors"]) > 0
        assert any(
            "parent_concept" in error.lower() and "weight" in error.lower()
            for error in data["errors"]
        )

    @patch("api.admin.filings_db")
    def test_import_from_csv_update_validation_fails(
        self, mock_filings_db, client, mock_override
    ):
        """Test importing CSV with update_existing that violates constraints fails."""
        # Set existing to non-abstract with unit
        mock_override.is_abstract = False
        mock_override.unit = "USD"
        mock_override.weight = None
        mock_override.parent_concept = None
        mock_filings_db.concept_normalization_overrides.get_by_key.return_value = (
            mock_override
        )

        # CSV tries to update to abstract but keep unit (should fail)
        csv_content = io.StringIO()
        writer = csv.DictWriter(
            csv_content,
            fieldnames=[
                "concept",
                "statement",
                "normalized_label",
                "is_abstract",
                "abstract_concept",
                "parent_concept",
                "description",
                "unit",
                "weight",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:TestConcept",
                "statement": "Income Statement",
                "normalized_label": "Test Label",
                "is_abstract": "true",
                "abstract_concept": "",
                "parent_concept": "",
                "description": "",
                "unit": "USD",  # This should cause validation error
                "weight": "",
            }
        )

        files = {"file": ("test.csv", csv_content.getvalue(), "text/csv")}

        response = client.post(
            "/admin/concept-normalization-overrides/import?update_existing=true",
            files=files,
        )

        assert response.status_code == 200
        data = response.json()
        assert data["updated"] == 0
        assert len(data["errors"]) > 0
        assert any(
            "abstract" in error.lower() and "unit" in error.lower()
            for error in data["errors"]
        )

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
            mock.abstract_concept = override.abstract_concept
            mock.description = override.description
            mock.unit = override.unit
            mock.weight = override.weight
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
                "abstract_concept",
                "description",
                "unit",
                "weight",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "concept": "us-gaap:Concept1",
                "statement": "Income Statement",
                "normalized_label": "Label 1",
                "is_abstract": "false",
                "abstract_concept": "",
                "description": "",
                "unit": "USD",
                "weight": "",
            }
        )
        writer.writerow(
            {
                "concept": "us-gaap:Concept2",
                "statement": "Balance Sheet",
                "normalized_label": "Label 2",
                "is_abstract": "true",
                "abstract_concept": "",
                "description": "",
                "unit": "",
                "weight": "",
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
