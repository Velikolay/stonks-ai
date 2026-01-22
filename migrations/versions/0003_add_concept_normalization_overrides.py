"""Add concept normalization overrides table

Revision ID: 0003
Revises: 0002
Create Date: 2025-01-27 10:00:00.000000

"""

import csv
import logging
from pathlib import Path

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create concept normalization mapping table
    table = op.create_table(
        "concept_normalization_overrides",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column("is_abstract", sa.Boolean(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("weight", sa.Numeric(), nullable=True),
        sa.Column("parent_concept", sa.String(), nullable=True),
        sa.Column("abstract_concept", sa.String(), nullable=True),
        # Support company-specific overrides
        sa.Column("company_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "concept",
            "statement",
            "company_id",
            name="uq_concept_statement_company",
            postgresql_nulls_not_distinct=True,
        ),
        sa.ForeignKeyConstraint(
            ["abstract_concept", "statement", "company_id"],
            [
                "concept_normalization_overrides.concept",
                "concept_normalization_overrides.statement",
                "concept_normalization_overrides.company_id",
            ],
            name="fk_concept_normalization_overrides_abstract_concept",
        ),
        sa.ForeignKeyConstraint(
            ["parent_concept", "statement", "company_id"],
            [
                "concept_normalization_overrides.concept",
                "concept_normalization_overrides.statement",
                "concept_normalization_overrides.company_id",
            ],
            name="fk_concept_normalization_overrides_parent_concept",
        ),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_concept_normalization_overrides_company_id",
        ),
    )

    # Create indexes for parent_concept and abstract_concept
    op.execute(
        """
        CREATE INDEX idx_concept_normalization_overrides_parent_concept ON concept_normalization_overrides (parent_concept, statement, company_id);
    """
    )

    op.execute(
        """
        CREATE INDEX idx_concept_normalization_overrides_abstract_concept ON concept_normalization_overrides (abstract_concept, statement, company_id);
    """
    )

    # Create trigger to update updated_at timestamp
    op.execute(
        """
        CREATE TRIGGER update_concept_normalization_overrides_updated_at
        BEFORE UPDATE ON concept_normalization_overrides
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    )

    # Insert initial concept mappings from CSV file
    migration_dir = Path(__file__).parent
    csv_path = migration_dir.parent / "data" / "concept-normalization-overrides.csv"

    connection = op.get_bind()

    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Skip empty rows
                if not row.get("concept") or not row.get("statement"):
                    continue

                # Convert boolean string to boolean
                is_abstract = row.get("is_abstract", "").lower() == "true"

                # Convert empty strings to None for nullable fields
                abstract_concept = row.get("abstract_concept") or None
                parent_concept = row.get("parent_concept") or None
                description = row.get("description") or None
                unit = row.get("unit") or None
                weight = row.get("weight") or None

                rows.append(
                    {
                        "concept": row["concept"],
                        "statement": row["statement"],
                        "normalized_label": row["normalized_label"],
                        "is_abstract": is_abstract,
                        "description": description,
                        "unit": unit,
                        "weight": weight,
                        "parent_concept": parent_concept,
                        "abstract_concept": abstract_concept,
                    }
                )
            except Exception as e:
                logger.exception(f"Error processing row {row}")
                raise e

    if rows:
        connection.execute(table.insert(), rows)


def downgrade() -> None:
    # Drop triggers
    op.execute(
        "DROP TRIGGER IF EXISTS update_concept_normalization_overrides_updated_at ON concept_normalization_overrides"
    )

    # Drop indexes for parent_concept and abstract_concept
    op.execute(
        "DROP INDEX IF EXISTS idx_concept_normalization_overrides_parent_concept"
    )
    op.execute(
        "DROP INDEX IF EXISTS idx_concept_normalization_overrides_abstract_concept"
    )

    # Drop concept_normalization_overrides table
    op.drop_table("concept_normalization_overrides")
