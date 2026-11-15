"""Add financial rewrites for quarterly and yearly values.

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-15 00:00:00.000000

"""

import csv
import logging
from pathlib import Path

import sqlalchemy as sa
from alembic import op

logger = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None

_ACTIONS = ("delete", "set", "add")
_GRANULARITIES = ("quarterly", "yearly")


def upgrade() -> None:
    """Create financial_rewrites and load migrations/data/financial-rewrites.csv."""
    table = op.create_table(
        "financial_rewrites",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column("axis", sa.String(), nullable=False, server_default=""),
        sa.Column("member", sa.String(), nullable=False, server_default=""),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("granularity", sa.String(), nullable=False),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
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
            name="fk_financial_rewrites_company_id",
        ),
        sa.UniqueConstraint(
            "company_id",
            "statement",
            "normalized_label",
            "axis",
            "member",
            "period_end",
            "granularity",
            name="uq_financial_rewrites",
        ),
        sa.CheckConstraint(
            "granularity IN ('quarterly', 'yearly')",
            name="ck_financial_rewrites_granularity",
        ),
        sa.CheckConstraint(
            "action IN ('delete', 'set', 'add')",
            name="ck_financial_rewrites_action",
        ),
        sa.CheckConstraint(
            "(action = 'delete' AND value IS NULL) "
            "OR (action IN ('set', 'add') AND value IS NOT NULL)",
            name="ck_financial_rewrites_value",
        ),
    )

    op.execute(
        """
        CREATE TRIGGER update_financial_rewrites_updated_at
        BEFORE UPDATE ON financial_rewrites
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
        """
    )

    csv_path = Path(__file__).parent.parent / "data" / "financial-rewrites.csv"
    rows = []
    with open(csv_path, "r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if not any(value for value in row.values()):
                continue
            action = (row.get("action") or "").strip().lower()
            granularity = (row.get("granularity") or "").strip().lower()
            if action not in _ACTIONS:
                raise ValueError(f"action must be delete, set, or add, not {action!r}")
            if granularity not in _GRANULARITIES:
                raise ValueError(
                    "granularity must be quarterly or yearly, " f"not {granularity!r}"
                )
            amount = (row.get("value") or "").strip() or None
            if action == "delete" and amount is not None:
                raise ValueError("delete must not set a value")
            if action != "delete" and amount is None:
                raise ValueError(f"{action} requires a value")
            rows.append(
                {
                    "company_id": int(row["company_id"]),
                    "statement": row["statement"].strip(),
                    "normalized_label": row["normalized_label"].strip(),
                    "axis": (row.get("axis") or "").strip(),
                    "member": (row.get("member") or "").strip(),
                    "period_end": row["period_end"].strip(),
                    "granularity": granularity,
                    "action": action,
                    "value": amount,
                }
            )

    if rows:
        op.get_bind().execute(table.insert(), rows)
        logger.info("Loaded %s financial rewrite(s)", len(rows))


def downgrade() -> None:
    """Drop financial_rewrites."""
    op.execute(
        "DROP TRIGGER IF EXISTS update_financial_rewrites_updated_at "
        "ON financial_rewrites"
    )
    op.drop_table("financial_rewrites")
