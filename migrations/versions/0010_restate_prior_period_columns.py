"""Keep prior-period columns and prefer a later filing's restatement.

Revision ID: 0010
Revises: 0009
Create Date: 2026-11-17 00:00:00.000000
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def _read_sql(relative_path: str) -> str:
    """Read a SQL file relative to the repository root."""
    repo_root = Path(__file__).resolve().parents[2]
    return (repo_root / relative_path).read_text(encoding="utf-8")


def upgrade() -> None:
    """Install fiscal placement and the procedures that use it."""
    op.execute(_read_sql("sql/procedures/filing_for_period.sql"))
    op.execute(_read_sql("sql/procedures/refresh_financial_facts_normalized.sql"))
    op.execute(_read_sql("sql/procedures/refresh_dimension_breakdowns.sql"))
    op.execute(_read_sql("sql/procedures/refresh_quarterly_financials.sql"))
    op.execute(_read_sql("sql/procedures/refresh_yearly_financials.sql"))


def downgrade() -> None:
    """Leave the procedures in place.

    The previous procedure bodies are not stored separately. Dropping
    filing_for_period would break the procedures that call it.
    """
