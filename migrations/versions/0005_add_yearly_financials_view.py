"""add_yearly_financials_view

Revision ID: 0005
Revises: 0004
Create Date: 2025-01-27 10:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create yearly financial metrics view based on 10-K filings only
    op.execute(
        """
        CREATE MATERIALIZED VIEW yearly_financials AS
        SELECT
            f.company_id,
            ff.label,
            COALESCE(cn.normalized_label, ff.label) as normalized_label,
            ff.value,
            ff.unit,
            ff.statement,
            ff.period_end,
            ff.period_start,
            f.fiscal_year,
            f.fiscal_period_end
        FROM financial_facts ff
        JOIN filings f ON ff.filing_id = f.id
        LEFT JOIN concept_normalizations cn ON ff.concept = cn.concept
            AND (cn.statement IS NULL OR ff.statement = cn.statement)
        WHERE f.form_type = '10-K'
        ORDER BY f.company_id, f.fiscal_year, ff.label;
    """
    )


def downgrade() -> None:
    # Drop the yearly financials view
    op.execute("DROP VIEW IF EXISTS yearly_financials;")
