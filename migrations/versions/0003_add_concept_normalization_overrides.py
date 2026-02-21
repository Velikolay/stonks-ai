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
    # Create concept graph / hierarchy table
    hierarchy_table = op.create_table(
        "concept_hierarchy",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("is_global", sa.Boolean(), nullable=False),
        sa.Column("weight", sa.Numeric(), nullable=True),
        sa.Column("parent_concept", sa.String(), nullable=True),
        sa.Column("abstract_concept", sa.String(), nullable=True),
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
        sa.PrimaryKeyConstraint("company_id", "concept", "statement"),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_concept_hierarchy_company_id",
        ),
    )

    # Create normalization rules table
    rules_table = op.create_table(
        "concept_normalization_overrides",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("label", sa.String(), nullable=False, server_default=sa.text("'*'")),
        sa.Column(
            "form_type", sa.String(), nullable=False, server_default=sa.text("'*'")
        ),
        sa.Column(
            "from_period", sa.String(), nullable=False, server_default=sa.text("'*'")
        ),
        sa.Column(
            "to_period", sa.String(), nullable=False, server_default=sa.text("'*'")
        ),
        sa.Column("is_global", sa.Boolean(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column("normalized_concept", sa.String(), nullable=True),
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
        sa.PrimaryKeyConstraint(
            "company_id",
            "concept",
            "statement",
            "label",
            "form_type",
            "from_period",
            "to_period",
        ),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_concept_normalization_overrides_company_id",
        ),
    )

    # Indexes for hierarchy traversal
    op.execute(
        """
        CREATE INDEX idx_concept_hierarchy_parent_concept
        ON concept_hierarchy (company_id, parent_concept, statement);
    """
    )
    op.execute(
        """
        CREATE INDEX idx_concept_hierarchy_abstract_concept
        ON concept_hierarchy (company_id, abstract_concept, statement);
    """
    )

    # Index for normalized concept lookups
    op.execute(
        """
        CREATE INDEX idx_concept_normalization_overrides_normalized_concept
        ON concept_normalization_overrides (
            company_id,
            COALESCE(normalized_concept, concept),
            statement
        );
    """
    )

    # Triggers to update updated_at timestamp
    op.execute(
        """
        CREATE TRIGGER update_concept_hierarchy_updated_at
        BEFORE UPDATE ON concept_hierarchy
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    )
    op.execute(
        """
        CREATE TRIGGER update_concept_normalization_overrides_updated_at
        BEFORE UPDATE ON concept_normalization_overrides
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    )

    # Insert initial concept mappings from CSV files
    migration_dir = Path(__file__).parent
    hierarchy_csv_path = migration_dir.parent / "data" / "concept-hierarchy.csv"
    rules_csv_path = (
        migration_dir.parent / "data" / "concept-normalization-overrides.csv"
    )

    connection = op.get_bind()

    hierarchy_rows = []
    with open(hierarchy_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Skip empty rows
                if (
                    not row.get("company_id")
                    or not row.get("concept")
                    or not row.get("statement")
                ):
                    continue

                company_id = int(row["company_id"])
                concept = row["concept"]
                statement = row["statement"]
                is_global = row.get("is_global", "").lower() == "true"

                # Convert empty strings to None for nullable fields
                abstract_concept = row.get("abstract_concept") or None
                parent_concept = row.get("parent_concept") or None
                weight = row.get("weight") or None

                hierarchy_rows.append(
                    {
                        "company_id": company_id,
                        "concept": concept,
                        "statement": statement,
                        "is_global": is_global,
                        "weight": weight,
                        "parent_concept": parent_concept,
                        "abstract_concept": abstract_concept,
                    }
                )
            except Exception as e:
                logger.exception(f"Error processing row {row}")
                raise e

    rules_rows = []
    with open(rules_csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Skip empty rows
                if (
                    not row.get("company_id")
                    or not row.get("concept")
                    or not row.get("statement")
                ):
                    continue

                company_id = int(row["company_id"])
                concept = row["concept"]
                statement = row["statement"]
                is_global = row.get("is_global", "").lower() == "true"

                def _star_default(value: object) -> str:
                    if value is None:
                        return "*"
                    stripped = str(value).strip()
                    return stripped if stripped else "*"

                rules_rows.append(
                    {
                        "company_id": company_id,
                        "concept": concept,
                        "statement": statement,
                        "label": _star_default(row.get("label")),
                        "form_type": _star_default(row.get("form_type")),
                        "from_period": _star_default(row.get("from_period")),
                        "to_period": _star_default(row.get("to_period")),
                        "is_global": is_global,
                        "description": (row.get("description") or "").strip() or None,
                        "unit": (row.get("unit") or "").strip() or None,
                        "normalized_label": row["normalized_label"],
                        "normalized_concept": (
                            (row.get("normalized_concept") or "").strip() or None
                        ),
                    }
                )
            except Exception as e:
                logger.exception(f"Error processing row {row}")
                raise e

    if hierarchy_rows:
        connection.execute(hierarchy_table.insert(), hierarchy_rows)
    if rules_rows:
        connection.execute(rules_table.insert(), rules_rows)


def downgrade() -> None:
    # Drop triggers
    op.execute(
        "DROP TRIGGER IF EXISTS update_concept_normalization_overrides_updated_at ON concept_normalization_overrides"
    )
    op.execute(
        "DROP TRIGGER IF EXISTS update_concept_hierarchy_updated_at ON concept_hierarchy"
    )

    op.execute(
        "DROP INDEX IF EXISTS idx_concept_normalization_overrides_normalized_concept"
    )

    # Drop indexes for parent_concept and abstract_concept
    op.execute("DROP INDEX IF EXISTS idx_concept_hierarchy_parent_concept")
    op.execute("DROP INDEX IF EXISTS idx_concept_hierarchy_abstract_concept")

    # Drop tables
    op.drop_table("concept_normalization_overrides")
    op.drop_table("concept_hierarchy")
