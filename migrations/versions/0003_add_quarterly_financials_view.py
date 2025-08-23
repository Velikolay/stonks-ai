"""add_quarterly_financials_view

Revision ID: 0003
Revises: 0002
Create Date: 2025-08-22 01:15:46.030435

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create quarterly financial metrics view with correct quarter assignment
    # based on actual fiscal period end dates rather than assuming 10-K = Q4

    op.execute(
        """
        CREATE MATERIALIZED VIEW quarterly_financials AS
        WITH all_filings_data AS (
            -- Get all filing data with proper quarter assignment based on fiscal_period_end
            SELECT
                f.company_id,
                f.fiscal_year,
                f.fiscal_quarter,  -- This is already calculated correctly in the filings table
                ff.label,
                ff.value,
                ff.unit,
                ff.statement,
                ff.concept,
                ff.axis,
                ff.member,
                ff.period_end,
                ff.period_start,
                f.form_type as source_type,
                f.fiscal_period_end
            FROM financial_facts ff
            JOIN filings f ON ff.filing_id = f.id
            WHERE f.form_type IN ('10-Q', '10-K')
        ),
        quarterly_filings AS (
            -- Get quarterly data from 10-Q filings
            SELECT
                company_id,
                fiscal_year,
                fiscal_quarter,
                label,
                value,
                unit,
                statement,
                concept,
                axis,
                member,
                period_end,
                period_start,
                source_type,
                fiscal_period_end
            FROM all_filings_data
            WHERE source_type = '10-Q'
        ),
        annual_filings AS (
            -- Get annual data from 10-K filings
            SELECT
                company_id,
                fiscal_year,
                fiscal_quarter,
                label,
                value,
                unit,
                statement,
                concept,
                axis,
                member,
                period_end,
                period_start,
                source_type,
                fiscal_period_end
            FROM all_filings_data
            WHERE source_type = '10-K'
        ),
        missing_quarters AS (
            -- Calculate missing quarters by subtracting the sum of previous 3 10-Q filings from 10-K data
            SELECT
                a.company_id,
                a.fiscal_year,
                a.fiscal_quarter,
                a.label,
                a.value - COALESCE(prev_quarters_sum.sum_value, 0) as value,
                a.unit,
                a.statement,
                a.period_end,
                a.period_start,
                'calculated' as source_type,
                a.fiscal_period_end
            FROM annual_filings a
            LEFT JOIN LATERAL (
                -- Get the sum of the previous 3 10-Q filings for each 10-K filing
                SELECT SUM(q.value) as sum_value
                FROM (
                    SELECT q.value
                    FROM quarterly_filings q
                    WHERE q.company_id = a.company_id
                        AND q.statement = a.statement
                        AND q.concept = a.concept
                        AND COALESCE(q.axis, '') = COALESCE(a.axis, '')
                        AND COALESCE(q.member, '') = COALESCE(a.member, '')
                        AND q.fiscal_period_end < a.fiscal_period_end
                    ORDER BY q.fiscal_period_end DESC
                    LIMIT 3
                ) q
            ) prev_quarters_sum ON true
        )
        -- Combine all quarterly data
        SELECT
            company_id,
            fiscal_year,
            fiscal_quarter,
            label,
            value,
            unit,
            statement,
            period_end,
            period_start,
            source_type
        FROM quarterly_filings

        UNION ALL

        SELECT
            company_id,
            fiscal_year,
            fiscal_quarter,
            label,
            value,
            unit,
            statement,
            period_end,
            period_start,
            source_type
        FROM missing_quarters
        WHERE value IS NOT NULL AND value != 0
        ORDER BY company_id, fiscal_year, fiscal_quarter, label;
    """
    )


def downgrade() -> None:
    # Drop the quarterly financials view
    op.execute("DROP VIEW IF EXISTS quarterly_financials;")
