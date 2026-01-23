"""Admin API endpoints."""

import csv
import io
import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, File, HTTPException, Path, Query, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from filings.db import FilingsDatabase
from filings.models import (
    CompanyUpdate,
    FilingEntityCreate,
    FilingEntityUpdate,
    TickerCreate,
    TickerUpdate,
)
from filings.models.concept_normalization_override import (
    ConceptNormalizationOverrideCreate,
    ConceptNormalizationOverrideUpdate,
)
from filings.models.dimension_normalization_override import (
    DimensionNormalizationOverrideCreate,
    DimensionNormalizationOverrideUpdate,
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


class TickerResponse(BaseModel):
    """Ticker response model for admin endpoints (no company_id)."""

    id: int
    ticker: str
    exchange: str
    status: str


class FilingEntityResponse(BaseModel):
    """Filing entity response model for admin endpoints (no company_id)."""

    id: int
    registry: str
    number: str
    status: str


class CompanyResponse(BaseModel):
    """Response model for a company with managed relationships."""

    id: int
    name: str
    industry: Optional[str] = None
    tickers: List[TickerResponse]
    filing_entities: List[FilingEntityResponse]


class ConceptNormalizationOverrideResponse(BaseModel):
    """Response model for concept normalization override."""

    company_id: Optional[int] = None
    concept: str
    statement: str
    normalized_label: str
    is_abstract: bool
    abstract_concept: Optional[str] = None
    parent_concept: Optional[str] = None
    description: Optional[str] = None
    unit: Optional[str] = None
    weight: Optional[float] = None
    created_at: datetime
    updated_at: datetime


class DimensionNormalizationOverrideResponse(BaseModel):
    """Response model for dimension normalization override."""

    company_id: Optional[int] = None
    axis: str
    member: str
    member_label: str
    normalized_axis_label: str
    normalized_member_label: Optional[str] = None
    tags: Optional[List[str]] = None
    created_at: datetime
    updated_at: datetime


@router.get(
    "/concept-normalization-overrides",
    response_model=List[ConceptNormalizationOverrideResponse],
)
async def list_concept_normalization_overrides(
    statement: Optional[str] = Query(None, description="Filter by statement type"),
    company_id: Optional[int] = Query(
        None, description="Filter by company_id (company-specific overrides only)"
    ),
) -> List[ConceptNormalizationOverrideResponse]:
    """List all concept normalization overrides, optionally filtered by statement/company."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        overrides = filings_db.concept_normalization_overrides.list_all(
            statement=statement, company_id=company_id
        )
        return [
            ConceptNormalizationOverrideResponse(
                company_id=override.company_id,
                concept=override.concept,
                statement=override.statement,
                normalized_label=override.normalized_label,
                is_abstract=override.is_abstract,
                abstract_concept=override.abstract_concept,
                parent_concept=override.parent_concept,
                description=override.description,
                unit=override.unit,
                weight=float(override.weight) if override.weight is not None else None,
                created_at=override.created_at,
                updated_at=override.updated_at,
            )
            for override in overrides
        ]
    except Exception as e:
        logger.error(f"Error listing concept normalization overrides: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/companies", response_model=List[CompanyResponse])
async def list_companies() -> List[CompanyResponse]:
    """List companies including tickers and filing entities."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        companies = filings_db.companies.get_all_companies()
        if not companies:
            return []

        company_ids = [c.id for c in companies]
        tickers_by_company_id = filings_db.companies.get_tickers_by_company_ids(
            company_ids=company_ids
        )
        filing_entities_by_company_id = (
            filings_db.companies.get_filing_entities_by_company_ids(
                company_ids=company_ids
            )
        )

        result: List[CompanyResponse] = []
        for company in companies:
            tickers = [
                TickerResponse(
                    id=t.id,
                    ticker=t.ticker,
                    exchange=t.exchange,
                    status=t.status,
                )
                for t in tickers_by_company_id.get(company.id, [])
            ]
            filing_entities = [
                FilingEntityResponse(
                    id=fe.id,
                    registry=fe.registry,
                    number=fe.number,
                    status=fe.status,
                )
                for fe in filing_entities_by_company_id.get(company.id, [])
            ]
            result.append(
                CompanyResponse(
                    id=company.id,
                    name=company.name,
                    industry=company.industry,
                    tickers=tickers,
                    filing_entities=filing_entities,
                )
            )
        return result
    except Exception as e:
        logger.error("Error listing companies: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/companies/{company_id}", response_model=CompanyResponse)
async def update_company(
    company_id: int = Path(..., description="Company ID"),
    company_update: CompanyUpdate = ...,
) -> CompanyResponse:
    """Update company fields and return the updated company with relationships."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        updated = filings_db.companies.update_company(
            company_id=company_id, company=company_update
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="Company not found")

        tickers = filings_db.companies.get_tickers_by_company_id(company_id)
        filing_entities = filings_db.companies.get_filing_entities_by_company_id(
            company_id
        )
        return CompanyResponse(
            id=updated.id,
            name=updated.name,
            industry=updated.industry,
            tickers=[
                TickerResponse(
                    id=t.id,
                    ticker=t.ticker,
                    exchange=t.exchange,
                    status=t.status,
                )
                for t in tickers
            ],
            filing_entities=[
                FilingEntityResponse(
                    id=fe.id,
                    registry=fe.registry,
                    number=fe.number,
                    status=fe.status,
                )
                for fe in filing_entities
            ],
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error updating company_id=%s: %s", company_id, e)
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/companies/{company_id}/tickers",
    response_model=TickerResponse,
    status_code=201,
)
async def add_company_ticker(
    company_id: int = Path(..., description="Company ID"),
    ticker: TickerCreate = ...,
) -> TickerResponse:
    """Add a ticker mapping to a company."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    company = filings_db.companies.get_company_by_id(company_id)
    if company is None:
        raise HTTPException(status_code=404, detail="Company not found")

    created = filings_db.companies.create_ticker(company_id=company_id, ticker=ticker)
    if created is None:
        raise HTTPException(status_code=400, detail="Failed to create ticker")
    return TickerResponse(
        id=created.id,
        ticker=created.ticker,
        exchange=created.exchange,
        status=created.status,
    )


@router.put(
    "/companies/{company_id}/tickers/{ticker_id}",
    response_model=TickerResponse,
)
async def update_company_ticker(
    company_id: int = Path(..., description="Company ID"),
    ticker_id: int = Path(..., description="Ticker ID"),
    ticker_update: TickerUpdate = ...,
) -> TickerResponse:
    """Update a ticker mapping for a company."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    updated = filings_db.companies.update_ticker(
        company_id=company_id, ticker_id=ticker_id, ticker=ticker_update
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Ticker not found")
    return TickerResponse(
        id=updated.id,
        ticker=updated.ticker,
        exchange=updated.exchange,
        status=updated.status,
    )


@router.delete(
    "/companies/{company_id}/tickers/{ticker_id}",
    status_code=204,
)
async def delete_company_ticker(
    company_id: int = Path(..., description="Company ID"),
    ticker_id: int = Path(..., description="Ticker ID"),
) -> None:
    """Delete a ticker mapping for a company."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    deleted = filings_db.companies.delete_ticker(
        company_id=company_id, ticker_id=ticker_id
    )
    if not deleted:
        raise HTTPException(status_code=404, detail="Ticker not found")


@router.post(
    "/companies/{company_id}/filing-entities",
    response_model=FilingEntityResponse,
    status_code=201,
)
async def add_company_filing_entity(
    company_id: int = Path(..., description="Company ID"),
    filing_entity: FilingEntityCreate = ...,
) -> FilingEntityResponse:
    """Add a filing entity (e.g., SEC + CIK) to a company."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    company = filings_db.companies.get_company_by_id(company_id)
    if company is None:
        raise HTTPException(status_code=404, detail="Company not found")

    created = filings_db.companies.create_filing_entity(
        company_id=company_id, filing_entity=filing_entity
    )
    if created is None:
        raise HTTPException(status_code=400, detail="Failed to create filing entity")
    return FilingEntityResponse(
        id=created.id,
        registry=created.registry,
        number=created.number,
        status=created.status,
    )


@router.put(
    "/companies/{company_id}/filing-entities/{filing_entity_id}",
    response_model=FilingEntityResponse,
)
async def update_company_filing_entity(
    company_id: int = Path(..., description="Company ID"),
    filing_entity_id: int = Path(..., description="Filing entity ID"),
    filing_entity_update: FilingEntityUpdate = ...,
) -> FilingEntityResponse:
    """Update a filing entity for a company."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    updated = filings_db.companies.update_filing_entity(
        company_id=company_id,
        filing_entity_id=filing_entity_id,
        filing_entity=filing_entity_update,
    )
    if updated is None:
        raise HTTPException(status_code=404, detail="Filing entity not found")
    return FilingEntityResponse(
        id=updated.id,
        registry=updated.registry,
        number=updated.number,
        status=updated.status,
    )


@router.delete(
    "/companies/{company_id}/filing-entities/{filing_entity_id}",
    status_code=204,
)
async def delete_company_filing_entity(
    company_id: int = Path(..., description="Company ID"),
    filing_entity_id: int = Path(..., description="Filing entity ID"),
) -> None:
    """Delete a filing entity for a company."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    deleted = filings_db.companies.delete_filing_entity(
        company_id=company_id, filing_entity_id=filing_entity_id
    )
    if deleted:
        return

    existing_ids = {
        fe.id
        for fe in filings_db.companies.get_filing_entities_by_company_id(company_id)
    }
    if filing_entity_id in existing_ids:
        raise HTTPException(
            status_code=409,
            detail="Cannot delete filing entity: it may be referenced by filings",
        )
    raise HTTPException(status_code=404, detail="Filing entity not found")


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
            company_id=created_override.company_id,
            concept=created_override.concept,
            statement=created_override.statement,
            normalized_label=created_override.normalized_label,
            is_abstract=created_override.is_abstract,
            abstract_concept=created_override.abstract_concept,
            parent_concept=created_override.parent_concept,
            description=created_override.description,
            unit=created_override.unit,
            weight=(
                float(created_override.weight)
                if created_override.weight is not None
                else None
            ),
            created_at=created_override.created_at,
            updated_at=created_override.updated_at,
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
    company_id: Optional[int] = Query(
        None, description="Company ID for company-specific override (omit for global)"
    ),
) -> ConceptNormalizationOverrideResponse:
    """Update an existing concept normalization override."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        updated_override = filings_db.concept_normalization_overrides.update(
            concept, statement, override_update, company_id
        )
        if not updated_override:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Concept normalization override not found: "
                    f"({concept}, {statement}, {company_id})"
                ),
            )
        return ConceptNormalizationOverrideResponse(
            company_id=updated_override.company_id,
            concept=updated_override.concept,
            statement=updated_override.statement,
            normalized_label=updated_override.normalized_label,
            is_abstract=updated_override.is_abstract,
            abstract_concept=updated_override.abstract_concept,
            parent_concept=updated_override.parent_concept,
            description=updated_override.description,
            unit=updated_override.unit,
            weight=(
                float(updated_override.weight)
                if updated_override.weight is not None
                else None
            ),
            created_at=updated_override.created_at,
            updated_at=updated_override.updated_at,
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
    company_id: Optional[int] = Query(
        None, description="Company ID for company-specific override (omit for global)"
    ),
) -> None:
    """Delete a concept normalization override."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        deleted = filings_db.concept_normalization_overrides.delete(
            concept, statement, company_id
        )
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Concept normalization override not found: "
                    f"({concept}, {statement}, {company_id})"
                ),
            )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error deleting concept normalization override: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class ImportResponse(BaseModel):
    """Response model for CSV import operation."""

    message: str
    created: int
    updated: int
    errors: List[str]


@router.get("/concept-normalization-overrides/export")
async def export_concept_normalization_overrides_to_csv(
    statement: Optional[str] = Query(None, description="Filter by statement type"),
    company_id: Optional[int] = Query(
        None, description="Filter by company_id (company-specific overrides only)"
    ),
) -> StreamingResponse:
    """Export concept normalization overrides to CSV."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        overrides = filings_db.concept_normalization_overrides.list_all(
            statement=statement, company_id=company_id
        )

        # Create CSV content
        output = io.StringIO()
        fieldnames = [
            "company_id",
            "concept",
            "statement",
            "normalized_label",
            "is_abstract",
            "abstract_concept",
            "parent_concept",
            "description",
            "unit",
            "weight",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for override in overrides:
            writer.writerow(
                {
                    "company_id": (
                        str(override.company_id)
                        if override.company_id is not None
                        else ""
                    ),
                    "concept": override.concept,
                    "statement": override.statement,
                    "normalized_label": override.normalized_label,
                    "is_abstract": str(override.is_abstract),
                    "abstract_concept": override.abstract_concept or "",
                    "parent_concept": override.parent_concept or "",
                    "description": override.description or "",
                    "unit": override.unit or "",
                    "weight": (
                        str(override.weight) if override.weight is not None else ""
                    ),
                }
            )

        output.seek(0)
        filename = "concept_normalization_overrides.csv"
        if statement:
            filename = (
                f"concept_normalization_overrides_{statement.replace(' ', '_')}.csv"
            )
        if company_id is not None:
            filename = filename.replace(".csv", f"_company_{company_id}.csv")

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        logger.error(f"Error exporting concept normalization overrides: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/concept-normalization-overrides/import",
    response_model=ImportResponse,
)
async def import_concept_normalization_overrides_from_csv(
    file: UploadFile = File(..., description="CSV file to import"),
    update_existing: bool = Query(
        False, description="Update existing records if they exist"
    ),
) -> ImportResponse:
    """Import concept normalization overrides from CSV file."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV file")

    created = 0
    updated = 0
    errors = []

    try:
        # Read file content
        content = await file.read()
        content_str = content.decode("utf-8")
        csv_file = io.StringIO(content_str)

        # Parse CSV
        reader = csv.DictReader(csv_file)
        required_fields = ["concept", "statement", "normalized_label", "is_abstract"]

        for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 is header)
            try:
                # Validate required fields
                missing_fields = [f for f in required_fields if not row.get(f)]
                if missing_fields:
                    errors.append(
                        f"Row {row_num}: Missing required fields: {', '.join(missing_fields)}"
                    )
                    continue

                # Parse boolean
                is_abstract_str = row["is_abstract"].strip().lower()
                if is_abstract_str in ("true", "1", "yes", "t"):
                    is_abstract = True
                elif is_abstract_str in ("false", "0", "no", "f", ""):
                    is_abstract = False
                else:
                    errors.append(
                        f"Row {row_num}: Invalid is_abstract value: {row['is_abstract']}"
                    )
                    continue

                # Parse weight (convert to Decimal if present)
                weight_str = row.get("weight", "").strip()
                weight = None
                if weight_str:
                    try:
                        weight = Decimal(weight_str)
                    except (ValueError, Exception):
                        errors.append(
                            f"Row {row_num}: Invalid weight value: {weight_str}"
                        )
                        continue

                row_company_id_str = (row.get("company_id") or "").strip()
                if not row_company_id_str:
                    row_company_id = None
                else:
                    try:
                        row_company_id = int(row_company_id_str)
                    except (ValueError, Exception):
                        errors.append(
                            f"Row {row_num}: Invalid company_id value: {row_company_id_str}"
                        )
                        continue

                # Create override object
                override_create = ConceptNormalizationOverrideCreate(
                    company_id=row_company_id,
                    concept=row["concept"].strip(),
                    statement=row["statement"].strip(),
                    normalized_label=row["normalized_label"].strip(),
                    is_abstract=is_abstract,
                    abstract_concept=row.get("abstract_concept", "").strip() or None,
                    parent_concept=row.get("parent_concept", "").strip() or None,
                    description=row.get("description", "").strip() or None,
                    unit=row.get("unit", "").strip() or None,
                    weight=weight,
                )

                # Check if record exists
                existing = filings_db.concept_normalization_overrides.get_by_key(
                    override_create.concept,
                    override_create.statement,
                    override_create.company_id,
                )

                if existing:
                    if update_existing:
                        # Update existing record (validation happens in DB layer)
                        override_update = ConceptNormalizationOverrideUpdate(
                            normalized_label=override_create.normalized_label,
                            is_abstract=override_create.is_abstract,
                            abstract_concept=override_create.abstract_concept,
                            parent_concept=override_create.parent_concept,
                            description=override_create.description,
                            unit=override_create.unit,
                            weight=override_create.weight,
                        )
                        filings_db.concept_normalization_overrides.update(
                            override_create.concept,
                            override_create.statement,
                            override_update,
                            override_create.company_id,
                        )
                        updated += 1
                    else:
                        errors.append(
                            f"Row {row_num}: Record already exists: "
                            f"({override_create.concept}, {override_create.statement}, "
                            f"{override_create.company_id})"
                        )
                else:
                    # Create new record (validation happens in DB layer)
                    filings_db.concept_normalization_overrides.create(override_create)
                    created += 1

            except ValueError as e:
                errors.append(f"Row {row_num}: {str(e)}")
            except Exception as e:
                errors.append(f"Row {row_num}: Unexpected error: {str(e)}")
                logger.error(f"Error processing row {row_num}: {e}")

        message = f"Import completed: {created} created, {updated} updated"
        if errors:
            message += f", {len(errors)} errors"

        return ImportResponse(
            message=message, created=created, updated=updated, errors=errors
        )

    except Exception as e:
        logger.error(f"Error importing concept normalization overrides: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class FinancialsRefreshResponse(BaseModel):
    """Response model for financials refresh operation."""

    message: str
    view_name: str
    mode: str
    success: bool


@router.post(
    "/financials/refresh",
    response_model=List[FinancialsRefreshResponse],
)
async def refresh_financials(
    concurrent: bool = Query(
        False, description="Use CONCURRENTLY mode to prevent blocking reads"
    ),
) -> List[FinancialsRefreshResponse]:
    """Refresh both quarterly_financials and yearly_financials materialized views.

    Args:
        concurrent: If True, uses REFRESH MATERIALIZED VIEW CONCURRENTLY (async). Else, uses standard REFRESH (sync, blocks until complete).

    Returns:
        List of FinancialsRefreshResponse with refresh status for each view
    """
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    mode = "CONCURRENTLY" if concurrent else "SYNC"
    results = []

    # Refresh quarterly financials
    try:
        filings_db.quarterly_financials.refresh_materialized_view(concurrent=concurrent)
        results.append(
            FinancialsRefreshResponse(
                message="Successfully refreshed quarterly_financials",
                view_name="quarterly_financials",
                mode=mode,
                success=True,
            )
        )
    except Exception as e:
        logger.error(f"Unexpected error refreshing quarterly_financials: {e}")
        results.append(
            FinancialsRefreshResponse(
                message=f"Unexpected error: {str(e)}",
                view_name="quarterly_financials",
                mode=mode,
                success=False,
            )
        )

    # Refresh yearly financials
    try:
        filings_db.yearly_financials.refresh_materialized_view(concurrent=concurrent)
        results.append(
            FinancialsRefreshResponse(
                message="Successfully refreshed yearly_financials",
                view_name="yearly_financials",
                mode=mode,
                success=True,
            )
        )
    except Exception as e:
        logger.error(f"Unexpected error refreshing yearly_financials: {e}")
        results.append(
            FinancialsRefreshResponse(
                message=f"Unexpected error: {str(e)}",
                view_name="yearly_financials",
                mode=mode,
                success=False,
            )
        )

    return results


@router.get(
    "/dimension-normalization-overrides",
    response_model=List[DimensionNormalizationOverrideResponse],
)
async def list_dimension_normalization_overrides(
    axis: Optional[str] = Query(None, description="Filter by axis"),
    company_id: Optional[int] = Query(
        None, description="Filter by company_id (company-specific overrides only)"
    ),
) -> List[DimensionNormalizationOverrideResponse]:
    """List all dimension normalization overrides, optionally filtered by axis/company."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        overrides = filings_db.dimension_normalization_overrides.list_all(
            axis=axis, company_id=company_id
        )
        return [
            DimensionNormalizationOverrideResponse(
                company_id=override.company_id,
                axis=override.axis,
                member=override.member,
                member_label=override.member_label,
                normalized_axis_label=override.normalized_axis_label,
                normalized_member_label=override.normalized_member_label,
                tags=override.tags,
                created_at=override.created_at,
                updated_at=override.updated_at,
            )
            for override in overrides
        ]
    except Exception as e:
        logger.error(f"Error listing dimension normalization overrides: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/dimension-normalization-overrides",
    response_model=DimensionNormalizationOverrideResponse,
    status_code=201,
)
async def create_dimension_normalization_override(
    override: DimensionNormalizationOverrideCreate,
) -> DimensionNormalizationOverrideResponse:
    """Create a new dimension normalization override."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        created_override = filings_db.dimension_normalization_overrides.create(override)
        return DimensionNormalizationOverrideResponse(
            company_id=created_override.company_id,
            axis=created_override.axis,
            member=created_override.member,
            member_label=created_override.member_label,
            normalized_axis_label=created_override.normalized_axis_label,
            normalized_member_label=created_override.normalized_member_label,
            tags=created_override.tags,
            created_at=created_override.created_at,
            updated_at=created_override.updated_at,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating dimension normalization override: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put(
    "/dimension-normalization-overrides/{axis}/{member}/{member_label}",
    response_model=DimensionNormalizationOverrideResponse,
)
async def update_dimension_normalization_override(
    axis: str = Path(..., description="Axis identifier"),
    member: str = Path(..., description="Member identifier"),
    member_label: str = Path(..., description="Member label identifier"),
    override_update: DimensionNormalizationOverrideUpdate = ...,
    company_id: Optional[int] = Query(
        None, description="Company ID for company-specific override (omit for global)"
    ),
) -> DimensionNormalizationOverrideResponse:
    """Update an existing dimension normalization override."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        updated_override = filings_db.dimension_normalization_overrides.update(
            axis, member, member_label, override_update, company_id
        )
        if not updated_override:
            raise HTTPException(
                status_code=404,
                detail=f"Dimension normalization override not found: ({axis}, {member}, {member_label})",
            )
        return DimensionNormalizationOverrideResponse(
            company_id=updated_override.company_id,
            axis=updated_override.axis,
            member=updated_override.member,
            member_label=updated_override.member_label,
            normalized_axis_label=updated_override.normalized_axis_label,
            normalized_member_label=updated_override.normalized_member_label,
            tags=updated_override.tags,
            created_at=updated_override.created_at,
            updated_at=updated_override.updated_at,
        )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating dimension normalization override: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/dimension-normalization-overrides/{axis}/{member}/{member_label}",
    status_code=204,
)
async def delete_dimension_normalization_override(
    axis: str = Path(..., description="Axis identifier"),
    member: str = Path(..., description="Member identifier"),
    member_label: str = Path(..., description="Member label identifier"),
    company_id: Optional[int] = Query(
        None, description="Company ID for company-specific override (omit for global)"
    ),
) -> None:
    """Delete a dimension normalization override."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        deleted = filings_db.dimension_normalization_overrides.delete(
            axis, member, member_label, company_id
        )
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"Dimension normalization override not found: ({axis}, {member}, {member_label})",
            )
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error deleting dimension normalization override: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/dimension-normalization-overrides/export")
async def export_dimension_normalization_overrides_to_csv(
    axis: Optional[str] = Query(None, description="Filter by axis"),
    company_id: Optional[int] = Query(
        None, description="Filter by company_id (company-specific overrides only)"
    ),
) -> StreamingResponse:
    """Export dimension normalization overrides to CSV."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    try:
        overrides = filings_db.dimension_normalization_overrides.list_all(
            axis=axis, company_id=company_id
        )

        # Create CSV content
        output = io.StringIO()
        fieldnames = [
            "company_id",
            "axis",
            "member",
            "member_label",
            "normalized_axis_label",
            "normalized_member_label",
            "tags",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for override in overrides:
            writer.writerow(
                {
                    "company_id": (
                        str(override.company_id)
                        if override.company_id is not None
                        else ""
                    ),
                    "axis": override.axis,
                    "member": override.member,
                    "member_label": override.member_label,
                    "normalized_axis_label": override.normalized_axis_label,
                    "normalized_member_label": override.normalized_member_label or "",
                    "tags": ",".join(override.tags) if override.tags else "",
                }
            )

        output.seek(0)
        filename = "dimension_normalization_overrides.csv"
        if axis:
            filename = f"dimension_normalization_overrides_{axis.replace(' ', '_')}.csv"
        if company_id is not None:
            filename = filename.replace(".csv", f"_company_{company_id}.csv")

        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        logger.error(f"Error exporting dimension normalization overrides: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/dimension-normalization-overrides/import",
    response_model=ImportResponse,
)
async def import_dimension_normalization_overrides_from_csv(
    file: UploadFile = File(..., description="CSV file to import"),
    update_existing: bool = Query(
        False, description="Update existing records if they exist"
    ),
) -> ImportResponse:
    """Import dimension normalization overrides from CSV file."""
    if not filings_db:
        raise HTTPException(status_code=500, detail="FilingsDatabase not initialized")

    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV file")

    created = 0
    updated = 0
    errors = []

    try:
        # Read file content
        content = await file.read()
        content_str = content.decode("utf-8")
        csv_file = io.StringIO(content_str)

        # Parse CSV
        reader = csv.DictReader(csv_file)
        required_fields = ["axis", "member", "member_label", "normalized_axis_label"]

        for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 is header)
            try:
                # Validate required fields
                missing_fields = [f for f in required_fields if not row.get(f)]
                if missing_fields:
                    errors.append(
                        f"Row {row_num}: Missing required fields: {', '.join(missing_fields)}"
                    )
                    continue

                # Parse tags (comma-separated)
                tags_str = row.get("tags", "").strip()
                tags = None
                if tags_str:
                    tags = [tag.strip() for tag in tags_str.split(",") if tag.strip()]

                # Parse optional company_id from CSV (empty/missing => None)
                row_company_id_str = (row.get("company_id") or "").strip()
                if row_company_id_str:
                    try:
                        row_company_id = int(row_company_id_str)
                    except (ValueError, Exception):
                        errors.append(
                            f"Row {row_num}: Invalid company_id value: {row_company_id_str}"
                        )
                        continue
                else:
                    row_company_id = None

                # Create override object
                override_create = DimensionNormalizationOverrideCreate(
                    company_id=row_company_id,
                    axis=row["axis"].strip(),
                    member=row["member"].strip(),
                    member_label=row["member_label"].strip(),
                    normalized_axis_label=row["normalized_axis_label"].strip(),
                    normalized_member_label=row.get(
                        "normalized_member_label", ""
                    ).strip()
                    or None,
                    tags=tags,
                )

                # Check if record exists
                existing = filings_db.dimension_normalization_overrides.get_by_key(
                    override_create.axis,
                    override_create.member,
                    override_create.member_label,
                    override_create.company_id,
                )

                if existing:
                    if update_existing:
                        # Update existing record
                        override_update = DimensionNormalizationOverrideUpdate(
                            normalized_axis_label=override_create.normalized_axis_label,
                            normalized_member_label=override_create.normalized_member_label,
                            tags=override_create.tags,
                        )
                        filings_db.dimension_normalization_overrides.update(
                            override_create.axis,
                            override_create.member,
                            override_create.member_label,
                            override_update,
                            override_create.company_id,
                        )
                        updated += 1
                    else:
                        errors.append(
                            f"Row {row_num}: Record already exists: "
                            f"({override_create.axis}, {override_create.member}, "
                            f"{override_create.member_label}, {override_create.company_id})"
                        )
                else:
                    # Create new record
                    filings_db.dimension_normalization_overrides.create(override_create)
                    created += 1

            except ValueError as e:
                errors.append(f"Row {row_num}: {str(e)}")
            except Exception as e:
                errors.append(f"Row {row_num}: Unexpected error: {str(e)}")
                logger.error(f"Error processing row {row_num}: {e}")

        message = f"Import completed: {created} created, {updated} updated"
        if errors:
            message += f", {len(errors)} errors"

        return ImportResponse(
            message=message, created=created, updated=updated, errors=errors
        )

    except Exception as e:
        logger.error(f"Error importing dimension normalization overrides: {e}")
        raise HTTPException(status_code=500, detail=str(e))
