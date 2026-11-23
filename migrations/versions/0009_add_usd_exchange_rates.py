"""Store daily foreign-currency rates for USD translation.

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-23 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "usd_exchange_rates",
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("rate_date", sa.Date(), nullable=False),
        sa.Column("units_per_usd", sa.Numeric(precision=20, scale=8), nullable=False),
        sa.PrimaryKeyConstraint("currency", "rate_date"),
    )


def downgrade() -> None:
    op.drop_table("usd_exchange_rates")
