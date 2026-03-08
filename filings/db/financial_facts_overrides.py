"""Financial facts overrides async database operations."""

import logging
from typing import List, Optional

from sqlalchemy import MetaData, delete, insert, select, update
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from filings.models.financial_facts_override import (
    FinancialFactsOverride,
    FinancialFactsOverrideCreate,
    FinancialFactsOverrideUpdate,
)

logger = logging.getLogger(__name__)


class FinancialFactsOverridesOperationsAsync:
    """Financial facts overrides async database operations."""

    def __init__(self, engine: AsyncEngine, metadata: MetaData):
        """Initialize with async engine and metadata."""
        self.engine = engine
        self.overrides_table = metadata.tables["financial_facts_overrides"]

    async def list_all(
        self,
        *,
        company_id: Optional[int] = None,
        statement: Optional[str] = None,
        concept: Optional[str] = None,
    ) -> List[FinancialFactsOverride]:
        """List overrides optionally filtered by company/statement/concept."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.overrides_table)
                if company_id is not None:
                    stmt = stmt.where(self.overrides_table.c.company_id == company_id)
                if statement is not None:
                    stmt = stmt.where(self.overrides_table.c.statement == statement)
                if concept is not None:
                    stmt = stmt.where(self.overrides_table.c.concept == concept)
                stmt = stmt.order_by(self.overrides_table.c.updated_at.desc())
                result = await conn.execute(stmt)
                rows = result.fetchall()

                return [
                    FinancialFactsOverride(
                        id=row.id,
                        company_id=row.company_id,
                        concept=row.concept,
                        statement=row.statement,
                        axis=row.axis,
                        member=row.member,
                        label=row.label,
                        form_type=row.form_type,
                        from_period=row.from_period,
                        to_period=row.to_period,
                        to_concept=row.to_concept,
                        to_axis=row.to_axis,
                        to_member=row.to_member,
                        to_weight=row.to_weight,
                        is_global=row.is_global,
                        created_at=row.created_at,
                        updated_at=row.updated_at,
                    )
                    for row in rows
                ]

        except SQLAlchemyError as e:
            logger.error("Error listing financial facts overrides: %s", e)
            raise

    async def create(
        self, override: FinancialFactsOverrideCreate
    ) -> FinancialFactsOverride:
        """Create a new override."""
        try:
            async with self.engine.connect() as conn:
                stmt = (
                    insert(self.overrides_table)
                    .values(
                        company_id=override.company_id,
                        concept=override.concept,
                        statement=override.statement,
                        axis=override.axis,
                        member=override.member,
                        label=override.label,
                        form_type=override.form_type,
                        from_period=override.from_period,
                        to_period=override.to_period,
                        is_global=override.company_id == 0,
                        to_concept=override.to_concept,
                        to_axis=override.to_axis,
                        to_member=override.to_member,
                        to_weight=override.to_weight,
                    )
                    .returning(self.overrides_table)
                )
                result = await conn.execute(stmt)
                row = result.fetchone()
                await conn.commit()

                return FinancialFactsOverride(
                    id=row.id,
                    company_id=row.company_id,
                    concept=row.concept,
                    statement=row.statement,
                    axis=row.axis,
                    member=row.member,
                    label=row.label,
                    form_type=row.form_type,
                    from_period=row.from_period,
                    to_period=row.to_period,
                    to_concept=row.to_concept,
                    to_axis=row.to_axis,
                    to_member=row.to_member,
                    to_weight=row.to_weight,
                    is_global=row.is_global,
                    created_at=row.created_at,
                    updated_at=row.updated_at,
                )

        except IntegrityError as e:
            logger.error("Integrity error creating financial facts override: %s", e)
            raise ValueError(f"Constraint violation: {e}")
        except SQLAlchemyError as e:
            logger.error("Error creating financial facts override: %s", e)
            raise

    async def update(
        self, override_id: int, override_update: FinancialFactsOverrideUpdate
    ) -> Optional[FinancialFactsOverride]:
        """Update an override by id."""
        try:
            async with self.engine.connect() as conn:
                update_values = {}
                for field in [
                    "axis",
                    "member",
                    "label",
                    "form_type",
                    "from_period",
                    "to_period",
                    "to_concept",
                    "to_axis",
                    "to_member",
                    "to_weight",
                ]:
                    value = getattr(override_update, field)
                    if value is not None:
                        update_values[field] = value

                if not update_values:
                    return await self.get_by_id(override_id=override_id)

                stmt = (
                    update(self.overrides_table)
                    .where(self.overrides_table.c.id == override_id)
                    .values(**update_values)
                    .returning(self.overrides_table)
                )
                result = await conn.execute(stmt)
                row = result.fetchone()
                await conn.commit()
                if not row:
                    return None

                return FinancialFactsOverride(
                    id=row.id,
                    company_id=row.company_id,
                    concept=row.concept,
                    statement=row.statement,
                    axis=row.axis,
                    member=row.member,
                    label=row.label,
                    form_type=row.form_type,
                    from_period=row.from_period,
                    to_period=row.to_period,
                    to_concept=row.to_concept,
                    to_axis=row.to_axis,
                    to_member=row.to_member,
                    to_weight=row.to_weight,
                    is_global=row.is_global,
                    created_at=row.created_at,
                    updated_at=row.updated_at,
                )

        except IntegrityError as e:
            logger.error("Integrity error updating financial facts override: %s", e)
            raise ValueError(f"Constraint violation: {e}")
        except SQLAlchemyError as e:
            logger.error(
                "Error updating financial facts override id=%s: %s", override_id, e
            )
            raise

    async def delete(self, *, override_id: int) -> bool:
        """Delete an override by id."""
        try:
            async with self.engine.connect() as conn:
                stmt = delete(self.overrides_table).where(
                    self.overrides_table.c.id == override_id
                )
                result = await conn.execute(stmt)
                await conn.commit()
                return result.rowcount > 0

        except IntegrityError as e:
            logger.error("Integrity error deleting financial facts override: %s", e)
            raise ValueError(
                f"Cannot delete: record is referenced by other records: {e}"
            )
        except SQLAlchemyError as e:
            logger.error(
                "Error deleting financial facts override id=%s: %s", override_id, e
            )
            raise

    async def get_by_id(self, *, override_id: int) -> Optional[FinancialFactsOverride]:
        """Get an override by primary key id."""
        try:
            async with self.engine.connect() as conn:
                stmt = select(self.overrides_table).where(
                    self.overrides_table.c.id == override_id
                )
                result = await conn.execute(stmt)
                row = result.fetchone()
                if not row:
                    return None
                return FinancialFactsOverride(
                    id=row.id,
                    company_id=row.company_id,
                    concept=row.concept,
                    statement=row.statement,
                    axis=row.axis,
                    member=row.member,
                    label=row.label,
                    form_type=row.form_type,
                    from_period=row.from_period,
                    to_period=row.to_period,
                    to_concept=row.to_concept,
                    to_axis=row.to_axis,
                    to_member=row.to_member,
                    to_weight=row.to_weight,
                    is_global=row.is_global,
                    created_at=row.created_at,
                    updated_at=row.updated_at,
                )
        except SQLAlchemyError as e:
            logger.error(
                "Error getting financial facts override id=%s: %s", override_id, e
            )
            raise
