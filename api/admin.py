"""Admin API endpoints."""

import logging
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Path
from pydantic import BaseModel

from filings.db import FilingsDatabase
from filings.models.concept_normalization_override import (
    ConceptNormalizationOverrideCreate,
    ConceptNormalizationOverrideUpdate,
)

logger = logging.getLogger(__name__)

# Create router for admin endpoints
router = APIRouter(prefix="/admin", tags=["admin"])

# Global database instance (will be set during app initialization)
filings_db: Optional[FilingsDatabase] = None


def set_filings_db(db: FilingsDatabase) -> None:
    """Set the global filings database instance."""
    global filings_db
    filings_db = db


class ConceptNormalizationOverrideResponse(BaseModel):
    """Response model for concept normalization override."""

    concept: str
    statement: str
    normalized_label: str
    is_abstract: bool
    parent_concept: Optional[str] = None
    description: Optional[str] = None


@router.get(
    "/concept-normalization-overrides",
    response_model=List[ConceptNormalizationOverrideResponse],
)
async def list_concept_normalization_overrides() -> (
    List[ConceptNormalizationOverrideResponse]
):
    """List all concept normalization overrides."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        overrides = filings_db.concept_normalization_overrides.list_all()
        return [
            ConceptNormalizationOverrideResponse(
                concept=override.concept,
                statement=override.statement,
                normalized_label=override.normalized_label,
                is_abstract=override.is_abstract,
                parent_concept=override.parent_concept,
                description=override.description,
            )
            for override in overrides
        ]
    except Exception as e:
        logger.error(f"Error listing concept normalization overrides: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/concept-normalization-overrides",
    response_model=ConceptNormalizationOverrideResponse,
    status_code=201,
)
async def create_concept_normalization_override(
    override: ConceptNormalizationOverrideCreate,
) -> ConceptNormalizationOverrideResponse:
    """Create a new concept normalization override."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        created_override = filings_db.concept_normalization_overrides.create(override)
        return ConceptNormalizationOverrideResponse(
            concept=created_override.concept,
            statement=created_override.statement,
            normalized_label=created_override.normalized_label,
            is_abstract=created_override.is_abstract,
            parent_concept=created_override.parent_concept,
            description=created_override.description,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating concept normalization override: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put(
    "/concept-normalization-overrides/{statement}/{concept}",
    response_model=ConceptNormalizationOverrideResponse,
)
async def update_concept_normalization_override(
    statement: str = Path(..., description="Statement type"),
    concept: str = Path(..., description="Concept identifier"),
    override_update: ConceptNormalizationOverrideUpdate = ...,
) -> ConceptNormalizationOverrideResponse:
    """Update an existing concept normalization override."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        updated_override = filings_db.concept_normalization_overrides.update(
            concept, statement, override_update
        )
        if not updated_override:
            raise HTTPException(
                status_code=404,
                detail=f"Concept normalization override not found: ({concept}, {statement})",
            )
        return ConceptNormalizationOverrideResponse(
            concept=updated_override.concept,
            statement=updated_override.statement,
            normalized_label=updated_override.normalized_label,
            is_abstract=updated_override.is_abstract,
            parent_concept=updated_override.parent_concept,
            description=updated_override.description,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating concept normalization override: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/concept-normalization-overrides/{statement}/{concept}",
    status_code=204,
)
async def delete_concept_normalization_override(
    statement: str = Path(..., description="Statement type"),
    concept: str = Path(..., description="Concept identifier"),
) -> None:
    """Delete a concept normalization override."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        deleted = filings_db.concept_normalization_overrides.delete(concept, statement)
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"Concept normalization override not found: ({concept}, {statement})",
            )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error deleting concept normalization override: {e}")
        raise HTTPException(status_code=500, detail=str(e))
