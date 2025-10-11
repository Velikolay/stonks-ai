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
                f.company_id,
                ff.label,
                COALESCE(cn.normalized_label, ff.label) as normalized_label,
                ff.value,
                ff.unit,
                ff.parsed_axis as axis,
                ff.parsed_member as member,
                ff.statement,
                ff.period_end,
                f.fiscal_year,
                f.fiscal_period_end,
                -- Get the latest abstracts for this metric combination
                FIRST_VALUE(ff.abstracts) OVER (
                    PARTITION BY f.company_id, ff.statement, COALESCE(cn.normalized_label, ff.label), ff.axis, ff.member
                    ORDER BY f.fiscal_period_end DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) as latest_abstracts
            FROM financial_facts ff
            JOIN filings f ON ff.filing_id = f.id
            LEFT JOIN concept_normalizations cn ON ff.concept = cn.concept
                AND (cn.statement IS NULL OR ff.statement = cn.statement)
            WHERE f.form_type = '10-K'
        ),
        all_filings_data_ext AS (
            SELECT
                all_filings_data.*,
                CASE
                    WHEN jsonb_typeof(latest_abstracts) = 'array' THEN (
                        SELECT array_agg(rtrim(elem->>'label', ':'))
                        FROM jsonb_array_elements(latest_abstracts) AS elem
                        WHERE elem->>'label' IS NOT NULL
                    )
                    ELSE NULL
                END AS latest_abstract_labels
            FROM all_filings_data
        )
        SELECT
            company_id,
            label,
            normalized_label,
            value,
            unit,
            axis,
            member,
            statement,
            latest_abstract_labels as abstracts,
            period_end,
            fiscal_year,
            fiscal_period_end,
            '10-K' as source_type
        FROM all_filings_data_ext
        ORDER BY company_id, fiscal_year DESC, statement;
    """
    )


def downgrade() -> None:
    # Drop the yearly financials view
    op.execute("DROP VIEW IF EXISTS yearly_financials;")
