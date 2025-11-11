"""Add companies, filings, and financial_facts tables

Revision ID: 0002
Revises: 0001
Create Date: 2024-12-19 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create companies table
    op.create_table(
        "companies",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ticker", sa.String(), nullable=True),
        sa.Column("exchange", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("ticker", "exchange", name="uq_companies_ticker_exchange"),
    )

    # Create filings table
    op.create_table(
        "filings",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("filing_number", sa.String(), nullable=False),
        sa.Column("form_type", sa.String(), nullable=False),
        sa.Column("filing_date", sa.Date(), nullable=False),
        sa.Column("fiscal_period_end", sa.Date(), nullable=False),
        sa.Column("fiscal_year", sa.Integer(), nullable=False),
        sa.Column("fiscal_quarter", sa.Integer(), nullable=False),
        sa.Column("public_url", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["company_id"],
            ["companies.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source", "filing_number", name="uq_filings_source_filing_number"
        ),
    )
    op.create_index(
        "ix_filings_form_type",
        "filings",
        ["form_type"],
    )

    # Create financial_facts table
    op.create_table(
        "financial_facts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("filing_id", sa.Integer(), nullable=False),
        sa.Column("concept", sa.String(), nullable=False),
        sa.Column("label", sa.String(), nullable=False),
        sa.Column("is_abstract", sa.Boolean(), nullable=False),
        sa.Column("value", sa.Numeric(), nullable=True),
        sa.Column("comparative_value", sa.Numeric(), nullable=True),
        sa.Column("weight", sa.Numeric(), nullable=True),
        sa.Column("unit", sa.String(), nullable=True),
        sa.Column("axis", sa.String(), nullable=True),
        sa.Column("member", sa.String(), nullable=True),
        sa.Column("parsed_axis", sa.String(), nullable=True),
        sa.Column("parsed_member", sa.String(), nullable=True),
        sa.Column("statement", sa.String(), nullable=True),
        sa.Column("period_end", sa.Date(), nullable=True),
        sa.Column("comparative_period_end", sa.Date(), nullable=True),
        sa.Column("period", sa.Enum("YTD", "Q", name="period_type"), nullable=True),
        sa.Column("position", sa.Integer(), nullable=True),
        sa.Column("parent_id", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(
            ["filing_id"],
            ["filings.id"],
        ),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["financial_facts.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_financial_facts_statement_concept",
        "financial_facts",
        ["statement", "concept"],
    )


def downgrade() -> None:
    # Drop financial_facts table
    op.drop_index("ix_financial_facts_statement_concept", table_name="financial_facts")
    op.drop_table("financial_facts")

    # Drop the enum type
    op.execute("DROP TYPE IF EXISTS period_type")

    # Drop filings table
    op.drop_index("ix_filings_form_type", table_name="filings")
    op.drop_table("filings")

    # Drop companies table
    op.drop_table("companies")
