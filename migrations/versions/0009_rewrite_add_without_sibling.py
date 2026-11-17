"""Let a financial rewrite add the first member of an axis.

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-17 00:00:00.000000
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def _read_sql(relative_path: str) -> str:
    """Read a SQL file relative to the repository root."""
    repo_root = Path(__file__).resolve().parents[2]
    return (repo_root / relative_path).read_text(encoding="utf-8")


def upgrade() -> None:
    """Reinstall the presentation procedures that apply financial rewrites."""
    op.execute(_read_sql("sql/procedures/refresh_quarterly_financials.sql"))
    op.execute(_read_sql("sql/procedures/refresh_yearly_financials.sql"))


def downgrade() -> None:
    """Leave the procedures in place.

    The previous procedure body is not stored separately. The add fallback
    still copies a same-axis sibling when one exists, so older rewrite rows
    keep their behavior.
    """
