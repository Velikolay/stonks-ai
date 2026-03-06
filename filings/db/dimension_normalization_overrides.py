"""Dimension normalization overrides async database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, and_, delete, insert, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from filings.models.dimension_normalization_override import (
    DimensionNormalizationOverride,
    DimensionNormalizationOverrideCreate,
    DimensionNormalizationOverrideUpdate,
)

logger = logging.getLogger(__name__)


class DimensionNormalizationOverridesOperationsAsync:
    """Dimension normalization overrides async database operations."""

    def __init__(self, engine: AsyncEngine, metadata: MetaData):
        """Initialize with async engine and metadata."""
        self.engine = engine
        self.overrides_table = metadata.tables["dimension_normalization_overrides"]

    async def list_all(
        self, *, company_id: Optional[int] = None, axis: Optional[str] = None
    ) -> List[DimensionNormalizationOverride]:
        """Get dimension normalization overrides filtered by company and axis."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.overrides_table)
                if axis is not None:
                    stmt = stmt.where(self.overrides_table.c.axis == axis)
                if company_id is not None:
                    stmt = stmt.where(self.overrides_table.c.company_id == company_id)
                stmt = stmt.order_by(self.overrides_table.c.updated_at.desc())
                result = await conn.execute(stmt)
                rows = result.fetchall()

                overrides = []
                for row in rows:
                    override = DimensionNormalizationOverride(
                        company_id=row.company_id,
                        axis=row.axis,
                        member=row.member,
                        member_label=row.member_label,
                        normalized_axis_label=row.normalized_axis_label,
                        normalized_member_label=row.normalized_member_label,
                        is_global=row.is_global,
                        tags=list(row.tags) if row.tags else None,
                        created_at=row.created_at,
                        updated_at=row.updated_at,
                    )
                    overrides.append(override)

                logger.info(
                    f"Retrieved {len(overrides)} dimension normalization overrides"
                    + (f" for axis: {axis}" if axis else "")
                    + f" for company_id: {company_id}"
                )
                return overrides

        except SQLAlchemyError as e:
            logger.error(f"Error retrieving dimension normalization overrides: {e}")
            raise

    async def get_by_key(
        self, *, company_id: int, axis: str, member: str, member_label: str
    ) -> Optional[DimensionNormalizationOverride]:
        """Get a dimension normalization override by key."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.overrides_table).where(
                    and_(
                        self.overrides_table.c.company_id == company_id,
                        self.overrides_table.c.axis == axis,
                        self.overrides_table.c.member == member,
                        self.overrides_table.c.member_label == member_label,
                    )
                )
                result = await conn.execute(stmt)
                row = result.fetchone()

                if row:
                    return DimensionNormalizationOverride(
                        company_id=row.company_id,
                        axis=row.axis,
                        member=row.member,
                        member_label=row.member_label,
                        normalized_axis_label=row.normalized_axis_label,
                        normalized_member_label=row.normalized_member_label,
                        is_global=row.is_global,
                        tags=list(row.tags) if row.tags else None,
                        created_at=row.created_at,
                        updated_at=row.updated_at,
                    )
                return None

        except SQLAlchemyError as e:
            logger.error(
                "Error getting dimension normalization override (%s, %s, %s, %s): %s",
                axis,
                member,
                member_label,
                company_id,
                e,
            )
            raise

    async def create(
        self, override: DimensionNormalizationOverrideCreate
    ) -> DimensionNormalizationOverride:
        """Create a new dimension normalization override."""
        try:
            async with self.engine.connect() as conn:
                stmt = (
                    insert(self.overrides_table)
                    .values(
                        company_id=override.company_id,
                        axis=override.axis,
                        member=override.member,
                        member_label=override.member_label,
                        normalized_axis_label=override.normalized_axis_label,
                        normalized_member_label=override.normalized_member_label,
                        is_global=override.company_id == 0,
                        tags=override.tags,
                    )
                    .returning(self.overrides_table)
                )

                result = await conn.execute(stmt)
                row = result.fetchone()
                await conn.commit()

                logger.info(
                    "Created dimension normalization override: (%s, %s, %s, %s)",
                    override.axis,
                    override.member,
                    override.member_label,
                    override.company_id,
                )

                return DimensionNormalizationOverride(
                    company_id=row.company_id,
                    axis=row.axis,
                    member=row.member,
                    member_label=row.member_label,
                    normalized_axis_label=row.normalized_axis_label,
                    normalized_member_label=row.normalized_member_label,
                    is_global=row.is_global,
                    tags=list(row.tags) if row.tags else None,
                    created_at=row.created_at,
                    updated_at=row.updated_at,
                )

        except IntegrityError as e:
            logger.error(
                f"Integrity error creating dimension normalization override: {e}"
            )
            raise ValueError(f"Dimension normalization override already exists: {e}")
        except SQLAlchemyError as e:
            logger.error(f"Error creating dimension normalization override: {e}")
            raise

    async def update(
        self,
        company_id: int,
        axis: str,
        member: str,
        member_label: str,
        override_update: DimensionNormalizationOverrideUpdate,
    ) -> Optional[DimensionNormalizationOverride]:
        """Update an existing dimension normalization override."""
        try:
            async with self.engine.connect() as conn:
                update_values = {}
                if override_update.normalized_axis_label is not None:
                    update_values["normalized_axis_label"] = (
                        override_update.normalized_axis_label
                    )
                if override_update.normalized_member_label is not None:
                    update_values["normalized_member_label"] = (
                        override_update.normalized_member_label
                    )
                if override_update.tags is not None:
                    update_values["tags"] = override_update.tags

                if not update_values:
                    return await self.get_by_key(
                        company_id=company_id,
                        axis=axis,
                        member=member,
                        member_label=member_label,
                    )

                stmt = (
                    update(self.overrides_table)
                    .where(
                        and_(
                            self.overrides_table.c.company_id == company_id,
                            self.overrides_table.c.axis == axis,
                            self.overrides_table.c.member == member,
                            self.overrides_table.c.member_label == member_label,
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
                        "Updated dimension normalization override: (%s, %s, %s, %s)",
                        company_id,
                        axis,
                        member,
                        member_label,
                    )
                    return DimensionNormalizationOverride(
                        company_id=row.company_id,
                        axis=row.axis,
                        member=row.member,
                        member_label=row.member_label,
                        normalized_axis_label=row.normalized_axis_label,
                        normalized_member_label=row.normalized_member_label,
                        is_global=row.is_global,
                        tags=list(row.tags) if row.tags else None,
                        created_at=row.created_at,
                        updated_at=row.updated_at,
                    )
                return None

        except IntegrityError as e:
            logger.error(
                f"Integrity error updating dimension normalization override: {e}"
            )
            raise ValueError(f"Constraint violation: {e}")
        except SQLAlchemyError as e:
            logger.error(f"Error updating dimension normalization override: {e}")
            raise

    async def delete(
        self,
        company_id: int,
        axis: str,
        member: str,
        member_label: str,
    ) -> bool:
        """Delete a dimension normalization override."""
        try:
            async with self.engine.connect() as conn:
                stmt = delete(self.overrides_table).where(
                    and_(
                        self.overrides_table.c.company_id == company_id,
                        self.overrides_table.c.axis == axis,
                        self.overrides_table.c.member == member,
                        self.overrides_table.c.member_label == member_label,
                    )
                )
                result = await conn.execute(stmt)
                await conn.commit()

                deleted = result.rowcount > 0
                if deleted:
                    logger.info(
                        "Deleted dimension normalization override: (%s, %s, %s, %s)",
                        company_id,
                        axis,
                        member,
                        member_label,
                    )
                else:
                    logger.warning(
                        "Dimension normalization override not found for deletion: (%s, %s, %s, %s)",
                        company_id,
                        axis,
                        member,
                        member_label,
                    )

                return deleted

        except IntegrityError as e:
            logger.error(
                f"Integrity error deleting dimension normalization override: {e}"
            )
            raise ValueError(
                f"Cannot delete: record is referenced by other records: {e}"
            )
        except SQLAlchemyError as e:
            logger.error(f"Error deleting dimension normalization override: {e}")
            raise
