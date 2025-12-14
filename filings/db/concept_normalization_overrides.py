"""Concept normalization overrides database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, Table, and_, delete, insert, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from ..models.concept_normalization_override import (
    ConceptNormalizationOverride,
    ConceptNormalizationOverrideCreate,
    ConceptNormalizationOverrideUpdate,
)

logger = logging.getLogger(__name__)


class ConceptNormalizationOverridesOperations:
    """Concept normalization overrides database operations."""

    def __init__(self, engine: Engine):
        """Initialize with database engine."""
        self.engine = engine
        # Create table metadata
        metadata = MetaData()
        self.overrides_table = Table(
            "concept_normalization_overrides", metadata, autoload_with=engine
        )

    def list_all(
        self, statement: Optional[str] = None
    ) -> List[ConceptNormalizationOverride]:
        """Get all concept normalization overrides, optionally filtered by statement."""
        try:
            with self.engine.connect() as conn:
                stmt = select(self.overrides_table)
                if statement is not None:
                    stmt = stmt.where(self.overrides_table.c.statement == statement)
                result = conn.execute(stmt)
                rows = result.fetchall()

                overrides = []
                for row in rows:
                    override = ConceptNormalizationOverride(
                        concept=row.concept,
                        statement=row.statement,
                        normalized_label=row.normalized_label,
                        is_abstract=row.is_abstract,
                        abstract_concept=row.abstract_concept,
                        parent_concept=row.parent_concept,
                        description=row.description,
                        unit=row.unit,
                        weight=row.weight,
                        created_at=row.created_at,
                        updated_at=row.updated_at,
                    )
                    overrides.append(override)

                logger.info(
                    f"Retrieved {len(overrides)} concept normalization overrides"
                    + (f" for statement: {statement}" if statement else "")
                )
                return overrides

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving concept normalization overrides: {e}")
            raise

    def get_by_key(
        self, concept: str, statement: str
    ) -> Optional[ConceptNormalizationOverride]:
        """Get a concept normalization override by concept and statement."""
        try:
            with self.engine.connect() as conn:
                stmt = select(self.overrides_table).where(
                    and_(
                        self.overrides_table.c.concept == concept,
                        self.overrides_table.c.statement == statement,
                    )
                )
                result = conn.execute(stmt)
                row = result.fetchone()

                if row:
                    return ConceptNormalizationOverride(
                        concept=row.concept,
                        statement=row.statement,
                        normalized_label=row.normalized_label,
                        is_abstract=row.is_abstract,
                        abstract_concept=row.abstract_concept,
                        parent_concept=row.parent_concept,
                        description=row.description,
                        unit=row.unit,
                        weight=row.weight,
                        created_at=row.created_at,
                        updated_at=row.updated_at,
                    )
                return None

        except SQLAlchemyError as e:
            logger.error(
                f"Error getting concept normalization override ({concept}, {statement}): {e}"
            )
            raise

    def create(
        self, override: ConceptNormalizationOverrideCreate
    ) -> ConceptNormalizationOverride:
        """Create a new concept normalization override."""
        try:
            with self.engine.connect() as conn:
                stmt = (
                    insert(self.overrides_table)
                    .values(
                        concept=override.concept,
                        statement=override.statement,
                        normalized_label=override.normalized_label,
                        is_abstract=override.is_abstract,
                        abstract_concept=override.abstract_concept,
                        parent_concept=override.parent_concept,
                        description=override.description,
                        unit=override.unit,
                        weight=override.weight,
                    )
                    .returning(self.overrides_table)
                )

                result = conn.execute(stmt)
                row = result.fetchone()
                conn.commit()

                logger.info(
                    f"Created concept normalization override: ({override.concept}, {override.statement})"
                )

                return ConceptNormalizationOverride(
                    concept=row.concept,
                    statement=row.statement,
                    normalized_label=row.normalized_label,
                    is_abstract=row.is_abstract,
                    abstract_concept=row.abstract_concept,
                    parent_concept=row.parent_concept,
                    description=row.description,
                    unit=row.unit,
                    weight=row.weight,
                    created_at=row.created_at,
                    updated_at=row.updated_at,
                )

        except IntegrityError as e:
            logger.error(
                f"Integrity error creating concept normalization override: {e}"
            )
            conn.rollback()
            raise ValueError(
                f"Concept normalization override already exists or invalid abstract_concept/parent_concept: {e}"
            )
        except SQLAlchemyError as e:
            logger.error(f"Error creating concept normalization override: {e}")
            conn.rollback()
            raise

    def update(
        self,
        concept: str,
        statement: str,
        override_update: ConceptNormalizationOverrideUpdate,
    ) -> Optional[ConceptNormalizationOverride]:
        """Update an existing concept normalization override."""
        try:
            with self.engine.connect() as conn:
                # Build update values from non-None fields
                update_values = {}
                if override_update.normalized_label is not None:
                    update_values["normalized_label"] = override_update.normalized_label
                if override_update.is_abstract is not None:
                    update_values["is_abstract"] = override_update.is_abstract
                if override_update.abstract_concept is not None:
                    update_values["abstract_concept"] = override_update.abstract_concept
                if override_update.parent_concept is not None:
                    update_values["parent_concept"] = override_update.parent_concept
                if override_update.description is not None:
                    update_values["description"] = override_update.description
                if override_update.unit is not None:
                    update_values["unit"] = override_update.unit
                if override_update.weight is not None:
                    update_values["weight"] = override_update.weight

                if not update_values:
                    # No fields to update, return existing record
                    return self.get_by_key(concept, statement)

                stmt = (
                    update(self.overrides_table)
                    .where(
                        and_(
                            self.overrides_table.c.concept == concept,
                            self.overrides_table.c.statement == statement,
                        )
                    )
                    .values(**update_values)
                    .returning(self.overrides_table)
                )

                result = conn.execute(stmt)
                row = result.fetchone()
                conn.commit()

                if row:
                    logger.info(
                        f"Updated concept normalization override: ({concept}, {statement})"
                    )
                    return ConceptNormalizationOverride(
                        concept=row.concept,
                        statement=row.statement,
                        normalized_label=row.normalized_label,
                        is_abstract=row.is_abstract,
                        abstract_concept=row.abstract_concept,
                        parent_concept=row.parent_concept,
                        description=row.description,
                        unit=row.unit,
                        weight=row.weight,
                        created_at=row.created_at,
                        updated_at=row.updated_at,
                    )
                return None

        except IntegrityError as e:
            logger.error(
                f"Integrity error updating concept normalization override: {e}"
            )
            conn.rollback()
            raise ValueError(
                f"Invalid abstract_concept/parent_concept or constraint violation: {e}"
            )
        except SQLAlchemyError as e:
            logger.error(f"Error updating concept normalization override: {e}")
            conn.rollback()
            raise

    def delete(self, concept: str, statement: str) -> bool:
        """Delete a concept normalization override."""
        try:
            with self.engine.connect() as conn:
                stmt = delete(self.overrides_table).where(
                    and_(
                        self.overrides_table.c.concept == concept,
                        self.overrides_table.c.statement == statement,
                    )
                )
                result = conn.execute(stmt)
                conn.commit()

                deleted = result.rowcount > 0
                if deleted:
                    logger.info(
                        f"Deleted concept normalization override: ({concept}, {statement})"
                    )
                else:
                    logger.warning(
                        f"Concept normalization override not found for deletion: ({concept}, {statement})"
                    )

                return deleted

        except IntegrityError as e:
            logger.error(
                f"Integrity error deleting concept normalization override: {e}"
            )
            conn.rollback()
            raise ValueError(
                f"Cannot delete: record is referenced by other records: {e}"
            )
        except SQLAlchemyError as e:
            logger.error(f"Error deleting concept normalization override: {e}")
            conn.rollback()
            raise
