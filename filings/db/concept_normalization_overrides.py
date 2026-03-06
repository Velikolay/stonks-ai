"""Concept normalization overrides async database operations."""

import logging
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import MetaData, and_, delete, insert, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from filings.models.concept_normalization_override import (
    ConceptNormalizationOverride,
    ConceptNormalizationOverrideCreate,
    ConceptNormalizationOverrideUpdate,
)

logger = logging.getLogger(__name__)


def validate_override_constraints(
    is_abstract: bool,
    parent_concept: Optional[str],
    unit: Optional[Decimal],
    weight: Optional[Decimal],
) -> None:
    """Validate override constraints.

    Raises:
        ValueError: If constraints are violated.
    """
    if parent_concept is not None:
        if is_abstract:
            raise ValueError(
                "Records with parent_concept cannot be abstract (is_abstract must be False)"
            )
        if weight is None:
            raise ValueError("Records with parent_concept must have a weight specified")

    if not is_abstract:
        if unit is None:
            raise ValueError(
                "Non-abstract records (is_abstract=False) must have a unit specified"
            )

    if is_abstract:
        if parent_concept is not None:
            raise ValueError(
                "Abstract records (is_abstract=True) cannot have a parent_concept"
            )
        if weight is not None:
            raise ValueError("Abstract records (is_abstract=True) cannot have a weight")
        if unit is not None:
            raise ValueError("Abstract records (is_abstract=True) cannot have a unit")


class ConceptNormalizationOverridesOperationsAsync:
    """Concept normalization overrides async database operations."""

    def __init__(self, engine: AsyncEngine, metadata: MetaData):
        """Initialize with async engine and metadata."""
        self.engine = engine
        self.overrides_table = metadata.tables["concept_normalization_overrides"]

    async def list_all(
        self, *, company_id: Optional[int] = None, statement: Optional[str] = None
    ) -> List[ConceptNormalizationOverride]:
        """Get concept normalization overrides, optionally filtered."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.overrides_table)
                if statement is not None:
                    stmt = stmt.where(self.overrides_table.c.statement == statement)
                if company_id is not None:
                    stmt = stmt.where(self.overrides_table.c.company_id == company_id)
                stmt = stmt.order_by(self.overrides_table.c.updated_at.desc())
                result = await conn.execute(stmt)
                rows = result.fetchall()

                overrides = []
                for row in rows:
                    override = ConceptNormalizationOverride(
                        company_id=row.company_id,
                        is_global=row.is_global,
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
                    + f" for company_id: {company_id}"
                )
                return overrides

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving concept normalization overrides: {e}")
            raise

    async def get_by_key(
        self, *, concept: str, statement: str, company_id: int
    ) -> Optional[ConceptNormalizationOverride]:
        """Get a concept normalization override by (concept, statement, company_id)."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.overrides_table).where(
                    and_(
                        self.overrides_table.c.concept == concept,
                        self.overrides_table.c.statement == statement,
                        self.overrides_table.c.company_id == company_id,
                    )
                )
                result = await conn.execute(stmt)
                row = result.fetchone()

                if row:
                    return ConceptNormalizationOverride(
                        company_id=row.company_id,
                        is_global=row.is_global,
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
                "Error getting concept normalization override (%s, %s, %s): %s",
                concept,
                statement,
                company_id,
                e,
            )
            raise

    async def create(
        self, override: ConceptNormalizationOverrideCreate
    ) -> ConceptNormalizationOverride:
        """Create a new concept normalization override."""
        validate_override_constraints(
            is_abstract=override.is_abstract,
            parent_concept=override.parent_concept,
            unit=override.unit,
            weight=override.weight,
        )

        try:
            async with self.engine.connect() as conn:
                stmt = (
                    insert(self.overrides_table)
                    .values(
                        company_id=override.company_id,
                        concept=override.concept,
                        statement=override.statement,
                        normalized_label=override.normalized_label,
                        is_abstract=override.is_abstract,
                        is_global=override.company_id == 0,
                        abstract_concept=override.abstract_concept,
                        parent_concept=override.parent_concept,
                        description=override.description,
                        unit=override.unit,
                        weight=override.weight,
                    )
                    .returning(self.overrides_table)
                )

                result = await conn.execute(stmt)
                row = result.fetchone()
                await conn.commit()

                logger.info(
                    "Created concept normalization override: (%s, %s, %s)",
                    override.concept,
                    override.statement,
                    override.company_id,
                )

                return ConceptNormalizationOverride(
                    company_id=row.company_id,
                    is_global=row.is_global,
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
            raise ValueError(
                f"Concept normalization override already exists or invalid abstract_concept/parent_concept: {e}"
            )
        except SQLAlchemyError as e:
            logger.error(f"Error creating concept normalization override: {e}")
            raise

    async def update(
        self,
        company_id: int,
        concept: str,
        statement: str,
        override_update: ConceptNormalizationOverrideUpdate,
    ) -> Optional[ConceptNormalizationOverride]:
        """Update an existing concept normalization override."""
        existing = await self.get_by_key(
            concept=concept, statement=statement, company_id=company_id
        )
        if not existing:
            return None

        final_is_abstract = (
            override_update.is_abstract
            if override_update.is_abstract is not None
            else existing.is_abstract
        )
        final_parent_concept = (
            override_update.parent_concept
            if override_update.parent_concept is not None
            else existing.parent_concept
        )
        final_unit = (
            override_update.unit if override_update.unit is not None else existing.unit
        )
        final_weight = (
            override_update.weight
            if override_update.weight is not None
            else existing.weight
        )

        validate_override_constraints(
            is_abstract=final_is_abstract,
            parent_concept=final_parent_concept,
            unit=final_unit,
            weight=final_weight,
        )

        try:
            async with self.engine.connect() as conn:
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
                    return existing

                stmt = (
                    update(self.overrides_table)
                    .where(
                        and_(
                            self.overrides_table.c.concept == concept,
                            self.overrides_table.c.statement == statement,
                            self.overrides_table.c.company_id == company_id,
                        )
                    )
                    .values(**update_values)
                    .returning(self.overrides_table)
                )

                result = await conn.execute(stmt)
                row = result.fetchone()
                await conn.commit()

                if row:
                    logger.info(
                        "Updated concept normalization override: (%s, %s, %s)",
                        concept,
                        statement,
                        company_id,
                    )
                    return ConceptNormalizationOverride(
                        company_id=row.company_id,
                        is_global=row.is_global,
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
            raise ValueError(
                f"Invalid abstract_concept/parent_concept or constraint violation: {e}"
            )
        except SQLAlchemyError as e:
            logger.error(f"Error updating concept normalization override: {e}")
            raise

    async def delete(self, *, company_id: int, concept: str, statement: str) -> bool:
        """Delete a concept normalization override."""
        try:
            async with self.engine.connect() as conn:
                stmt = delete(self.overrides_table).where(
                    and_(
                        self.overrides_table.c.concept == concept,
                        self.overrides_table.c.statement == statement,
                        self.overrides_table.c.company_id == company_id,
                    )
                )
                result = await conn.execute(stmt)
                await conn.commit()

                deleted = result.rowcount > 0
                if deleted:
                    logger.info(
                        "Deleted concept normalization override: (%s, %s, %s)",
                        concept,
                        statement,
                        company_id,
                    )
                else:
                    logger.warning(
                        "Concept normalization override not found for deletion: (%s, %s, %s)",
                        concept,
                        statement,
                        company_id,
                    )

                return deleted

        except IntegrityError as e:
            logger.error(
                f"Integrity error deleting concept normalization override: {e}"
            )
            raise ValueError(
                f"Cannot delete: record is referenced by other records: {e}"
            )
        except SQLAlchemyError as e:
            logger.error(f"Error deleting concept normalization override: {e}")
            raise
