"""Add dimension normalization overrides table

Revision ID: 0004
Revises: 0003
Create Date: 2025-01-27 10:00:00.000000

"""

import csv
import logging
from pathlib import Path

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create dimension normalization mapping table
    table = op.create_table(
        "dimension_normalization_overrides",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("axis", sa.String(), nullable=False),
        sa.Column("member", sa.String(), nullable=False),
        sa.Column("member_label", sa.String(), nullable=False),
        sa.Column("is_global", sa.Boolean(), nullable=False),
        sa.Column("normalized_axis_label", sa.String(), nullable=False),
        sa.Column("normalized_member_label", sa.String(), nullable=True),
        sa.Column("tags", sa.ARRAY(sa.String()), nullable=True),
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
        sa.PrimaryKeyConstraint("company_id", "axis", "member", "member_label"),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_dimension_normalization_overrides_company_id",
        ),
    )

    # Create trigger to update updated_at timestamp
    op.execute(
        """
        CREATE TRIGGER update_dimension_normalization_overrides_updated_at
        BEFORE UPDATE ON dimension_normalization_overrides
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    )

    # Insert initial dimension mappings from CSV file
    migration_dir = Path(__file__).parent
    csv_path = migration_dir.parent / "data" / "dimension-normalization-overrides.csv"

    connection = op.get_bind()

    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                # Skip empty rows
                if (
                    not row.get("company_id")
                    or not row.get("axis")
                    or not row.get("member")
                    or not row.get("member_label")
                    or not row.get("normalized_axis_label")
                ):
                    continue

                is_global = row.get("is_global", "").lower() == "true"

                rows.append(
                    {
                        "company_id": row["company_id"],
                        "axis": row["axis"],
                        "member": row["member"],
                        "member_label": row["member_label"],
                        "is_global": is_global,
                        "normalized_axis_label": row["normalized_axis_label"],
                        "normalized_member_label": (
                            row["normalized_member_label"]
                            if row["normalized_member_label"] != ""
                            else None
                        ),
                        "tags": row["tags"].split(";") if row["tags"] != "" else None,
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
        "DROP TRIGGER IF EXISTS update_dimension_normalization_overrides_updated_at ON dimension_normalization_overrides"
    )

    # Drop dimension_normalization_overrides table
    op.drop_table("dimension_normalization_overrides")
