"""Create derived financial tables (replace legacy views).

Revision ID: 0006
Revises: 0005
Create Date: 2026-03-04 00:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    period_type = postgresql.ENUM("YTD", "Q", name="period_type", create_type=False)

    op.create_table(
        "financial_facts_overridden",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("axis", sa.String(), nullable=False),
        sa.Column("member", sa.String(), nullable=False),
        sa.Column("member_label", sa.String(), nullable=False),
        sa.Column("weight", sa.Numeric(), nullable=True),
        sa.Column("fact_override_id", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_financial_facts_overridden_company_id",
        "financial_facts_overridden",
        ["company_id"],
    )

    op.create_table(
        "concept_normalization",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column("weight", sa.Numeric(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("group_id", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("overridden", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("company_id", "statement", "concept"),
    )
    op.create_index(
        "ix_concept_normalization_company_statement_group_id",
        "concept_normalization",
        ["company_id", "statement", "group_id"],
    )

    op.create_table(
        "hierarchy_normalization",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("parent_concept", sa.String(), nullable=False),
        sa.Column("concept_source", sa.String(), nullable=False),
        sa.Column("parent_concept_source", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("company_id", "statement", "concept", "parent_concept"),
    )

    op.create_table(
        "dimension_normalization",
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column("axis", sa.String(), nullable=False),
        sa.Column("member", sa.String(), nullable=False),
        sa.Column("member_label", sa.String(), nullable=False),
        sa.Column("normalized_axis_label", sa.String(), nullable=False),
        sa.Column("normalized_member_label", sa.String(), nullable=True),
        sa.Column("group_id", sa.String(), nullable=False),
        sa.Column("group_max_period_end", sa.Date(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("overridden", sa.Boolean(), nullable=False),
        sa.Column("override_priority", sa.String(), nullable=True),
        sa.Column("override_level", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint(
            "company_id",
            "statement",
            "normalized_label",
            "axis",
            "member",
            "member_label",
        ),
    )

    op.create_table(
        "financial_facts_normalized",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("filing_id", sa.Integer(), nullable=False),
        sa.Column("form_type", sa.String(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column("is_abstract", sa.Boolean(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("comparative_value", sa.Numeric(), nullable=True),
        sa.Column("weight", sa.Numeric(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("axis", sa.String(), nullable=False),
        sa.Column("member", sa.String(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("comparative_period_end", sa.Date(), nullable=True),
        sa.Column("period", period_type, nullable=True),
        sa.Column("position", sa.Integer(), nullable=True),
        sa.Column("parent_id", sa.BigInteger(), nullable=True),
        sa.Column("abstract_id", sa.BigInteger(), nullable=True),
        sa.Column("is_synthetic", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ff_normalized_co_stmt_lbl_ax_mb_per",
        "financial_facts_normalized",
        ["company_id", "statement", "normalized_label", "axis", "member", "period_end"],
    )
    op.create_index(
        "ix_financial_facts_normalized_company_filing_id",
        "financial_facts_normalized",
        ["company_id", "filing_id"],
    )

    op.create_table(
        "quarterly_financials",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("parent_id", sa.BigInteger(), nullable=True),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("filing_id", sa.Integer(), nullable=False),
        sa.Column("concept", sa.String(), nullable=True),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("weight", sa.Numeric(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("axis", sa.String(), nullable=False),
        sa.Column("member", sa.String(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("abstract_id", sa.BigInteger(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("fiscal_year", sa.Integer(), nullable=False),
        sa.Column("fiscal_quarter", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=True),
        sa.Column("is_abstract", sa.Boolean(), nullable=False),
        sa.Column("is_synthetic", sa.Boolean(), nullable=False),
        sa.Column("source_type", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_quarterly_financials_unique_composite
        ON quarterly_financials (
            company_id,
            statement,
            concept,
            normalized_label,
            period_end,
            axis,
            member
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_quarterly_financials_order
        ON quarterly_financials (
            company_id,
            statement,
            position,
            period_end DESC
        );
        """
    )

    op.create_table(
        "yearly_financials",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("parent_id", sa.BigInteger(), nullable=True),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("filing_id", sa.Integer(), nullable=False),
        sa.Column("concept", sa.String(), nullable=True),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("normalized_label", sa.String(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("weight", sa.Numeric(), nullable=True),
        sa.Column("axis", sa.String(), nullable=False),
        sa.Column("member", sa.String(), nullable=False),
        sa.Column("statement", sa.String(), nullable=False),
        sa.Column("abstract_id", sa.BigInteger(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=False),
        sa.Column("fiscal_year", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=True),
        sa.Column("is_abstract", sa.Boolean(), nullable=False),
        sa.Column("is_synthetic", sa.Boolean(), nullable=False),
        sa.Column("source_type", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_yearly_financials_unique_composite
        ON yearly_financials (
            company_id,
            statement,
            concept,
            normalized_label,
            period_end,
            axis,
            member
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_yearly_financials_order
        ON yearly_financials (
            company_id,
            statement,
            position,
            period_end DESC
        );
        """
    )


def downgrade() -> None:
    op.drop_table("yearly_financials")
    op.drop_table("quarterly_financials")
    op.drop_table("financial_facts_normalized")
    op.drop_table("dimension_normalization")
    op.drop_table("hierarchy_normalization")
    op.drop_table("concept_normalization")
    op.drop_index(
        "ix_financial_facts_overridden_company_id",
        table_name="financial_facts_overridden",
    )
    op.drop_table("financial_facts_overridden")
