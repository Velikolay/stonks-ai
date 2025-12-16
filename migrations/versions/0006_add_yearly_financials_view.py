"""add_yearly_financials_view

Revision ID: 0006
Revises: 0005
Create Date: 2025-01-27 10:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create yearly financial metrics view based on 10-K filings only

    op.execute(
        """
        CREATE MATERIALIZED VIEW yearly_financials AS

        WITH all_filings_data AS (
            -- Get all filing data with latest abstracts for each unique metric combination
            SELECT
                ff.company_id,
                ff.filing_id,
                ff.id,
                ff.parent_id,
                ff.concept,
                ff.label,
                ff.normalized_label,
                CASE
                    -- opposite weights means we have to flip the value
                    WHEN ff.weight * FIRST_VALUE(ff.weight) OVER w < 0 THEN -1 * ff.value
                    ELSE ff.value
                END as value,
                ff.unit,
                ff.parsed_axis as axis,
                ff.parsed_member as member,
                ff.statement,
                ff.period_end,
                ff.is_abstract,
                ff.is_synthetic,
                f.fiscal_year,
                -- Get the latest abstracts for this metric combination
                FIRST_VALUE(ff.abstract_id) OVER w AS latest_abstract_id,
                FIRST_VALUE(ff.position) OVER w AS latest_position,
                FIRST_VALUE(ff.weight) OVER w AS latest_weight
            FROM normalized_financial_facts ff
            JOIN filings f
            ON
                ff.company_id = f.company_id
                AND ff.filing_id = f.id
            WHERE
                ff.form_type = '10-K'
            WINDOW w AS (
                PARTITION BY ff.company_id, ff.filing_id, ff.statement, ff.normalized_label, ff.axis, ff.member
                ORDER BY ff.period_end DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        )

        SELECT
            id,
            parent_id,
            company_id,
            filing_id,
            concept,
            label,
            normalized_label,
            value,
            unit,
            latest_weight as weight,
            axis,
            member,
            statement,
            latest_abstract_id as abstract_id,
            period_end,
            fiscal_year,
            latest_position as position,
            is_abstract,
            is_synthetic,
            '10-K' as source_type
        FROM all_filings_data
        ORDER BY company_id, statement, position, period_end DESC;
    """
    )

    # Create unique index on yearly_financials for concurrent refresh
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_yearly_financials_unique_id
        ON yearly_financials (id);
        """
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


def downgrade() -> None:
    # Drop the unique index
    op.execute("DROP INDEX IF EXISTS idx_yearly_financials_unique_composite;")
    op.execute("DROP INDEX IF EXISTS idx_yearly_financials_unique_id;")
    # Drop the yearly financials view
    op.execute("DROP VIEW IF EXISTS yearly_financials;")
