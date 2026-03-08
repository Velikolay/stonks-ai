"""Install financials refresh function/procedures.

Revision ID: 0007
Revises: 0006
Create Date: 2026-03-04 00:00:00.000000
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

# revision identifiers, used by Alembic.
revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def _read_sql(relative_path: str) -> str:
    """Read a SQL file relative to repository root."""
    # migrations/versions/<file>.py -> migrations/versions -> migrations -> repo root
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / relative_path
    return path.read_text(encoding="utf-8")


def upgrade() -> None:
    op.execute(_read_sql("sql/procedures/refresh_financial_facts_overridden.sql"))
    op.execute(_read_sql("sql/procedures/refresh_concept_normalization.sql"))
    op.execute(_read_sql("sql/procedures/refresh_hierarchy_normalization.sql"))
    op.execute(_read_sql("sql/procedures/refresh_dimension_normalization.sql"))
    op.execute(_read_sql("sql/procedures/refresh_financial_facts_normalized.sql"))
    op.execute(_read_sql("sql/procedures/refresh_quarterly_financials.sql"))
    op.execute(_read_sql("sql/procedures/refresh_yearly_financials.sql"))
    op.execute(_read_sql("sql/procedures/refresh_financials.sql"))


def downgrade() -> None:
    op.execute("DROP PROCEDURE IF EXISTS refresh_financials(int[]);")
    op.execute("DROP PROCEDURE IF EXISTS refresh_yearly_financials(int[]);")
    op.execute("DROP PROCEDURE IF EXISTS refresh_quarterly_financials(int[]);")
    op.execute("DROP PROCEDURE IF EXISTS refresh_financial_facts_normalized(int[]);")
    op.execute("DROP PROCEDURE IF EXISTS refresh_dimension_normalization(int[]);")
    op.execute("DROP PROCEDURE IF EXISTS refresh_hierarchy_normalization(int[]);")
    op.execute("DROP PROCEDURE IF EXISTS refresh_concept_normalization(int[]);")
    op.execute("DROP PROCEDURE IF EXISTS refresh_financial_facts_overridden(int[]);")
