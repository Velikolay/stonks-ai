"""Add fact overrides table

Revision ID: 0005
Revises: 0004
Create Date: 2026-03-01 10:00:00.000000

"""

import csv
import logging
from pathlib import Path

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create rewrite rules table
    table = op.create_table(
        "fact_overrides",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("axis", sa.String(), nullable=True),
        sa.Column("member", sa.String(), nullable=True),
        sa.Column("label", sa.String(), nullable=True),
        sa.Column("form_type", sa.String(), nullable=True),
        sa.Column("from_period", sa.String(), nullable=True),
        sa.Column("to_period", sa.String(), nullable=True),
        sa.Column("is_global", sa.Boolean(), nullable=False),
        # override target
        sa.Column("to_concept", sa.String(), nullable=False),
        sa.Column("to_axis", sa.String(), nullable=True),
        sa.Column("to_member", sa.String(), nullable=True),
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
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
            name="fk_fact_overrides_company_id",
        ),
    )

    op.create_index(
        "ix_fact_overrides_match",
        "fact_overrides",
        [
            "company_id",
            "concept",
            "statement",
            "axis",
            "member",
            "label",
            "form_type",
            "from_period",
            "to_period",
        ],
    )

    # Create trigger to update updated_at timestamp
    op.execute(
        """
        CREATE TRIGGER update_fact_overrides_updated_at
        BEFORE UPDATE ON fact_overrides
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    """
    )

    # Insert initial rewrite rules from CSV file
    migration_dir = Path(__file__).parent
    csv_path = migration_dir.parent / "data" / "fact-overrides.csv"

    connection = op.get_bind()

    rows: list[dict[str, object]] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                required_fields = [
                    "company_id",
                    "concept",
                    "statement",
                    "to_concept",
                    "is_global",
                ]
                if not any(row.get(field) for field in required_fields):
                    continue
                missing_required_fields = [
                    field for field in required_fields if not row.get(field)
                ]
                if missing_required_fields:
                    raise ValueError(
                        "Missing required column value(s): "
                        + ", ".join(missing_required_fields)
                    )

                rows.append(
                    {
                        "company_id": row["company_id"],
                        "concept": row["concept"],
                        "statement": row["statement"],
                        "axis": row.get("axis") or None,
                        "member": row.get("member") or None,
                        "label": row.get("label") or None,
                        "form_type": row.get("form_type") or None,
                        "from_period": row.get("from_period") or None,
                        "to_period": row.get("to_period") or None,
                        "is_global": row["is_global"].lower() == "true",
                        "to_concept": row["to_concept"],
                        "to_axis": row.get("to_axis") or None,
                        "to_member": row.get("to_member") or None,
                    }
                )
            except Exception as e:
                logger.exception(f"Error processing row {row}")
                raise e

    if rows:
        connection.execute(table.insert(), rows)


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS update_fact_overrides_updated_at ON fact_overrides"
    )
    op.drop_index("ix_fact_overrides_match", table_name="fact_overrides")
    op.drop_table("fact_overrides")
