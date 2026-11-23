"""Store every XBRL dimension of a fact.

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-22 00:00:00.000000
"""

from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def _read_sql(relative_path: str) -> str:
    """Read a SQL file relative to the repository root."""
    repo_root = Path(__file__).resolve().parents[2]
    return (repo_root / relative_path).read_text(encoding="utf-8")


def upgrade() -> None:
    """Add fact dimensions and the breakdown procedure that reads them."""
    op.create_table(
        "financial_fact_dimensions",
        sa.Column("fact_id", sa.BigInteger(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("axis", sa.String(), nullable=False),
        sa.Column("member", sa.String(), nullable=False),
        sa.Column("member_label", sa.String(), nullable=False),
        sa.Column(
            "is_default",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.ForeignKeyConstraint(
            ["fact_id"],
            ["financial_facts.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("fact_id", "ordinal"),
    )
    op.execute(_read_sql("sql/procedures/refresh_dimension_breakdowns.sql"))
    op.execute(_read_sql("sql/procedures/refresh_financials.sql"))


def downgrade() -> None:
    """Drop fact dimensions. The procedure body is the SQL file at this revision."""
    op.execute("DROP TABLE IF EXISTS financial_fact_dimensions")
