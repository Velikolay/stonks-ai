"""add_quarterly_financials_view

Revision ID: 0004
Revises: 0003
Create Date: 2025-08-22 01:15:46.030435

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0004"
down_revision = "0003"
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
                COALESCE(cn.normalized_label, ff.label) as normalized_label,
                ff.value,
                ff.unit,
                ff.statement,
                ff.concept,
                ff.axis,
                ff.member,
                ff.period_end,
                ff.period_start,
                ff.period,
                f.form_type as source_type,
                f.fiscal_period_end
            FROM financial_facts ff
            JOIN filings f ON ff.filing_id = f.id
            LEFT JOIN concept_normalizations cn ON ff.concept = cn.concept
                AND (cn.statement IS NULL OR ff.statement = cn.statement)
            WHERE f.form_type IN ('10-Q', '10-K')
        ),
        quarterly_filings_raw AS (
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
                period,
                normalized_label,
                source_type,
                fiscal_period_end
            FROM all_filings_data
            WHERE source_type = '10-Q'
        ),
        quarterly_filings_with_prev AS (
            -- Add previous quarter data for YTD conversion
            SELECT
                q.*,
                CASE
                    WHEN (fiscal_period_end - LAG(fiscal_period_end) OVER w) BETWEEN 80 AND 100
                    THEN LAG(value) OVER w
                    ELSE NULL
                END AS prev_value
            FROM quarterly_filings_raw q
            WINDOW w AS (
                PARTITION BY company_id, statement, normalized_label, axis, member
                ORDER BY fiscal_period_end
            )
        ),
        quarterly_filings AS (
            -- Convert YTD data to quarterly values
            SELECT
                company_id,
                fiscal_year,
                fiscal_quarter,
                label,
                CASE
                    WHEN period = 'Q' THEN value
                    WHEN period = 'YTD' AND prev_value IS NULL THEN value
                    WHEN period = 'YTD' AND prev_value IS NOT NULL THEN value - prev_value
                    -- For NULL period (balance sheet items), use value as-is
                    ELSE value
                END as value,
                unit,
                statement,
                concept,
                axis,
                member,
                period_end,
                normalized_label,
                source_type,
                fiscal_period_end
            FROM quarterly_filings_with_prev
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
                normalized_label,
                source_type,
                fiscal_period_end
            FROM all_filings_data
            WHERE source_type = '10-K'
        ),
        quarterly_with_ranks AS (
            -- Rank quarterly filings for each annual filing
            SELECT
                q.*,
                a.company_id as k_company_id,
                a.fiscal_year as k_fiscal_year,
                a.fiscal_quarter as k_fiscal_quarter,
                a.value as k_value,
                a.unit as k_unit,
                a.statement as k_statement,
                a.period_end as k_period_end,
                a.fiscal_period_end as k_fiscal_period_end,
                a.label as k_label,
                a.normalized_label as k_normalized_label,
                ROW_NUMBER() OVER (
                    PARTITION BY q.company_id, q.statement, q.normalized_label, q.axis, q.member, a.fiscal_period_end
                    ORDER BY q.fiscal_period_end DESC
                ) as rn
            FROM quarterly_filings q
            JOIN annual_filings a ON
                q.company_id = a.company_id
                AND q.statement = a.statement
                AND q.normalized_label = a.normalized_label
                AND COALESCE(q.axis, '') = COALESCE(a.axis, '')
                AND COALESCE(q.member, '') = COALESCE(a.member, '')
                AND q.fiscal_period_end < a.fiscal_period_end
        ),
        missing_quarters AS (
            SELECT
                k_company_id as company_id,
                k_fiscal_year as fiscal_year,
                k_fiscal_quarter as fiscal_quarter,
                k_label as label,
                k_value - COALESCE(SUM(value) FILTER (WHERE rn <= 3), 0) as value,
                k_unit as unit,
                k_statement as statement,
                k_period_end as period_end,
                k_normalized_label as normalized_label,
                'calculated' as source_type,
                k_fiscal_period_end as fiscal_period_end
            FROM quarterly_with_ranks
            -- Balance Sheet data is snapshot in time accumulation so we don't need to calculate it quarterly
            WHERE k_statement != 'Balance Sheet'
            GROUP BY k_company_id, k_fiscal_year, k_fiscal_quarter, k_label, k_value, k_unit, k_statement, k_period_end, k_normalized_label, k_fiscal_period_end
            HAVING COUNT(*) FILTER (WHERE rn <= 3) = 3
        )

        -- Combine all quarterly data
        SELECT
            company_id,
            label,
            normalized_label,
            value,
            unit,
            statement,
            period_end,
            fiscal_year,
            fiscal_quarter,
            source_type
        FROM quarterly_filings

        UNION ALL

        -- Balance Sheet data is snapshot in time accumulation
        SELECT
            company_id,
            label,
            normalized_label,
            value,
            unit,
            statement,
            period_end,
            fiscal_year,
            fiscal_quarter,
            source_type
        FROM annual_filings
        WHERE statement = 'Balance Sheet'

        UNION ALL

        SELECT
            company_id,
            label,
            normalized_label,
            value,
            unit,
            statement,
            period_end,
            fiscal_year,
            fiscal_quarter,
            source_type
        FROM missing_quarters
        WHERE value IS NOT NULL AND value != 0
        ORDER BY company_id, fiscal_year DESC, fiscal_quarter DESC, statement, label;
    """
    )


def downgrade() -> None:
    # Drop the quarterly financials view
    op.execute("DROP VIEW IF EXISTS quarterly_financials;")
