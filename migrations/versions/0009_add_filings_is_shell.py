"""Flag filings whose cover declares a shell company.

A SPAC files as a shell until its merger closes, and the operating company
then reports under the SPAC's CIK. The refresh leaves shell filings out.
Filings stored before this revision read false; re-ingest a de-SPAC with
``--override`` to set it.

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-27 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "filings",
        sa.Column("is_shell", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("filings", "is_shell")
