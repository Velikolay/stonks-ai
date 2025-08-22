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
            LEFT JOIN (
                -- Get the sum of the previous 3 10-Q filings for each 10-K filing
                SELECT
                    k.company_id,
                    k.statement,
                    k.concept,
                    k.axis,
                    k.member,
                    k.fiscal_year,
                    k.fiscal_quarter,
                    SUM(q.value) as sum_value
                FROM annual_filings k
                JOIN (
                    -- Get only the previous 3 quarters for each 10-K filing
                    SELECT
                        q.company_id,
                        q.statement,
                        q.concept,
                        q.axis,
                        q.member,
                        q.value,
                        q.fiscal_period_end,
                        ROW_NUMBER() OVER (
                            PARTITION BY q.company_id, q.statement, q.concept, q.axis, q.member, k.fiscal_period_end
                            ORDER BY q.fiscal_period_end DESC
                        ) as rn
                    FROM quarterly_filings q
                    CROSS JOIN annual_filings k
                    WHERE q.company_id = k.company_id
                        AND q.statement = k.statement
                        AND q.concept = k.concept
                        AND COALESCE(q.axis, '') = COALESCE(k.axis, '')
                        AND COALESCE(q.member, '') = COALESCE(k.member, '')
                        AND q.fiscal_period_end < k.fiscal_period_end
                ) q ON q.rn <= 3
                GROUP BY k.company_id, k.statement, k.concept, k.axis, k.member, k.fiscal_year, k.fiscal_quarter
            ) prev_quarters_sum ON
                a.company_id = prev_quarters_sum.company_id
                AND a.statement = prev_quarters_sum.statement
                AND a.concept = prev_quarters_sum.concept
                AND COALESCE(a.axis, '') = COALESCE(prev_quarters_sum.axis, '')
                AND COALESCE(a.member, '') = COALESCE(prev_quarters_sum.member, '')
                AND a.fiscal_year = prev_quarters_sum.fiscal_year
                AND a.fiscal_quarter = prev_quarters_sum.fiscal_quarter
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
