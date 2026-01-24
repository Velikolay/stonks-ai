"""Tests for concept normalization overrides database operations."""

import pytest

from filings.models.concept_normalization_override import (
    ConceptNormalizationOverrideCreate,
    ConceptNormalizationOverrideUpdate,
)


@pytest.fixture
def sample_override() -> ConceptNormalizationOverrideCreate:
    """Sample concept normalization override data for testing."""
    return ConceptNormalizationOverrideCreate(
        company_id=0,
        concept="us-gaap:TestConcept",
        statement="Income Statement",
        normalized_label="Test Label",
        is_abstract=False,
        is_global=True,
        abstract_concept=None,
        description="Test description",
        unit="USD",
    )


@pytest.fixture
def sample_override_with_parent() -> ConceptNormalizationOverrideCreate:
    """Sample concept normalization override with parent for testing."""
    return ConceptNormalizationOverrideCreate(
        company_id=0,
        concept="us-gaap:ChildConcept",
        statement="Balance Sheet",
        normalized_label="Child Label",
        is_abstract=False,
        is_global=True,
        abstract_concept="us-gaap:ParentConcept",
        description="Child description",
        unit="USD",
    )


class TestConceptNormalizationOverridesOperations:
    """Test concept normalization overrides database operations."""

    def test_create_override(self, db, sample_override):
        """Test creating a new concept normalization override."""
        created = db.concept_normalization_overrides.create(sample_override)

        assert created is not None
        assert created.concept == sample_override.concept
        assert created.statement == sample_override.statement
        assert created.normalized_label == sample_override.normalized_label
        assert created.is_abstract == sample_override.is_abstract
        assert created.abstract_concept == sample_override.abstract_concept
        assert created.description == sample_override.description

    def test_create_duplicate_override(self, db, sample_override):
        """Test creating duplicate override raises error."""
        db.concept_normalization_overrides.create(sample_override)

        with pytest.raises(ValueError, match="already exists"):
            db.concept_normalization_overrides.create(sample_override)

    def test_get_by_key(self, db, sample_override):
        """Test retrieving override by concept and statement."""
        created = db.concept_normalization_overrides.create(sample_override)
        retrieved = db.concept_normalization_overrides.get_by_key(
            concept=created.concept, statement=created.statement, company_id=0
        )

        assert retrieved is not None
        assert retrieved.concept == created.concept
        assert retrieved.statement == created.statement
        assert retrieved.normalized_label == created.normalized_label

    def test_get_by_key_not_found(self, db):
        """Test retrieving non-existent override returns None."""
        retrieved = db.concept_normalization_overrides.get_by_key(
            concept="us-gaap:NonExistent", statement="Income Statement", company_id=0
        )
        assert retrieved is None

    def test_list_all(self, db, sample_override):
        """Test listing all overrides."""
        # Create multiple overrides
        override1 = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:Concept1",
            statement="Income Statement",
            normalized_label="Label 1",
            is_abstract=False,
            is_global=True,
            unit="USD",
        )
        override2 = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:Concept2",
            statement="Balance Sheet",
            normalized_label="Label 2",
            is_abstract=True,
            is_global=True,
        )
        override3 = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:Concept3",
            statement="Income Statement",
            normalized_label="Label 3",
            is_abstract=False,
            is_global=True,
            unit="USD",
        )

        db.concept_normalization_overrides.create(override1)
        db.concept_normalization_overrides.create(override2)
        db.concept_normalization_overrides.create(override3)

        all_overrides = db.concept_normalization_overrides.list_all(company_id=0)

        assert len(all_overrides) >= 3
        concepts = [o.concept for o in all_overrides]
        assert "us-gaap:Concept1" in concepts
        assert "us-gaap:Concept2" in concepts
        assert "us-gaap:Concept3" in concepts

    def test_list_all_with_statement_filter(self, db):
        """Test listing overrides filtered by statement."""
        # Create overrides for different statements
        override1 = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:Concept1",
            statement="Income Statement",
            normalized_label="Label 1",
            is_abstract=False,
            is_global=True,
            unit="USD",
        )
        override2 = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:Concept2",
            statement="Balance Sheet",
            normalized_label="Label 2",
            is_abstract=False,
            is_global=True,
            unit="USD",
        )
        override3 = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:Concept3",
            statement="Income Statement",
            normalized_label="Label 3",
            is_abstract=False,
            is_global=True,
            unit="USD",
        )

        db.concept_normalization_overrides.create(override1)
        db.concept_normalization_overrides.create(override2)
        db.concept_normalization_overrides.create(override3)

        # Filter by Income Statement
        income_statement_overrides = db.concept_normalization_overrides.list_all(
            company_id=0, statement="Income Statement"
        )

        assert len(income_statement_overrides) >= 2
        assert all(
            o.statement == "Income Statement" for o in income_statement_overrides
        )

        # Filter by Balance Sheet
        balance_sheet_overrides = db.concept_normalization_overrides.list_all(
            company_id=0, statement="Balance Sheet"
        )

        assert len(balance_sheet_overrides) >= 1
        assert all(o.statement == "Balance Sheet" for o in balance_sheet_overrides)

    def test_update_override(self, db, sample_override):
        """Test updating an existing override."""
        created = db.concept_normalization_overrides.create(sample_override)

        update_data = ConceptNormalizationOverrideUpdate(
            normalized_label="Updated Label",
            description="Updated description",
        )

        updated = db.concept_normalization_overrides.update(
            0, created.concept, created.statement, update_data
        )

        assert updated is not None
        assert updated.normalized_label == "Updated Label"
        assert updated.description == "Updated description"
        assert updated.concept == created.concept
        assert updated.statement == created.statement
        assert updated.is_abstract == created.is_abstract

    def test_update_override_partial(self, db, sample_override):
        """Test partial update of an override."""
        created = db.concept_normalization_overrides.create(sample_override)

        update_data = ConceptNormalizationOverrideUpdate(
            normalized_label="Updated Label Only"
        )

        updated = db.concept_normalization_overrides.update(
            0, created.concept, created.statement, update_data
        )

        assert updated is not None
        assert updated.normalized_label == "Updated Label Only"
        assert updated.description == created.description
        assert updated.is_abstract == created.is_abstract

    def test_update_override_not_found(self, db):
        """Test updating non-existent override returns None."""
        update_data = ConceptNormalizationOverrideUpdate(
            normalized_label="Updated Label"
        )

        updated = db.concept_normalization_overrides.update(
            0, "us-gaap:NonExistent", "Income Statement", update_data
        )

        assert updated is None

    def test_update_override_no_changes(self, db, sample_override):
        """Test update with no changes returns existing record."""
        created = db.concept_normalization_overrides.create(sample_override)

        update_data = ConceptNormalizationOverrideUpdate()

        updated = db.concept_normalization_overrides.update(
            0, created.concept, created.statement, update_data
        )

        assert updated is not None
        assert updated.normalized_label == created.normalized_label
        assert updated.description == created.description

    def test_delete_override(self, db, sample_override):
        """Test deleting an override."""
        created = db.concept_normalization_overrides.create(sample_override)

        deleted = db.concept_normalization_overrides.delete(
            concept=created.concept, statement=created.statement, company_id=0
        )

        assert deleted is True

        # Verify it's deleted
        retrieved = db.concept_normalization_overrides.get_by_key(
            concept=created.concept, statement=created.statement, company_id=0
        )
        assert retrieved is None

    def test_delete_override_not_found(self, db):
        """Test deleting non-existent override returns False."""
        deleted = db.concept_normalization_overrides.delete(
            concept="us-gaap:NonExistent", statement="Income Statement", company_id=0
        )

        assert deleted is False

    def test_create_with_abstract_concept(
        self, db, sample_override, sample_override_with_parent
    ):
        """Test creating override with parent concept."""
        # First create the parent
        parent = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:ParentConcept",
            statement="Balance Sheet",
            normalized_label="Parent Label",
            is_abstract=True,
            is_global=True,
            abstract_concept=None,
        )
        db.concept_normalization_overrides.create(parent)

        # Then create the child
        child = db.concept_normalization_overrides.create(sample_override_with_parent)

        assert child is not None
        assert child.abstract_concept == "us-gaap:ParentConcept"

    def test_create_with_invalid_abstract_concept(self, db, sample_override):
        """Test creating override with invalid parent raises error."""
        override_with_invalid_parent = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:ChildConcept",
            statement="Balance Sheet",
            normalized_label="Child Label",
            is_abstract=False,
            is_global=True,
            abstract_concept="us-gaap:NonExistentParent",
            unit="USD",
        )

        with pytest.raises(ValueError, match="invalid abstract_concept"):
            db.concept_normalization_overrides.create(override_with_invalid_parent)

    def test_is_abstract_values(self, db):
        """Test creating overrides with different is_abstract values."""
        override_false = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:ConcreteConcept",
            statement="Income Statement",
            normalized_label="Concrete",
            is_abstract=False,
            is_global=True,
            unit="USD",
        )

        override_true = ConceptNormalizationOverrideCreate(
            company_id=0,
            concept="us-gaap:AbstractConcept",
            statement="Income Statement",
            normalized_label="Abstract",
            is_abstract=True,
            is_global=True,
        )

        created_false = db.concept_normalization_overrides.create(override_false)
        created_true = db.concept_normalization_overrides.create(override_true)

        assert created_false.is_abstract is False
        assert created_true.is_abstract is True
