"""Keep statement headers out of quarterly year-to-date subtraction.

Revision ID: 0010
Revises: 0009
Create Date: 2026-11-23 00:00:00.000000
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

# revision identifiers, used by Alembic.
revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def _read_sql(relative_path: str) -> str:
    """Read a SQL file relative to the repository root."""
    repo_root = Path(__file__).resolve().parents[2]
    return (repo_root / relative_path).read_text(encoding="utf-8")


def upgrade() -> None:
    """Reinstall quarterly derivation so a header is not the prior quarter."""
    op.execute(_read_sql("sql/procedures/refresh_quarterly_financials.sql"))


def downgrade() -> None:
    """The previous procedure body is the SQL file at the parent revision."""
