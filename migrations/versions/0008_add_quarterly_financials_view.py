"""add_quarterly_financials_view

Revision ID: 0008
Revises: 0007
Create Date: 2025-08-22 01:15:46.030435

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create quarterly financial metrics view with correct quarter assignment
    # based on actual fiscal period end dates rather than assuming 10-K = Q4

    op.execute(
        """
        CREATE MATERIALIZED VIEW quarterly_financials AS

        WITH ordered_filings AS (
            SELECT
                f.*,
                ROW_NUMBER() OVER (
                    PARTITION BY company_id
                    ORDER BY fiscal_period_end, id
                ) AS seq
            FROM filings f
            WHERE form_type IN ('10-K', '10-Q')
        ),
        filings_cte AS (
            SELECT
                o.id,
                o.company_id,
                o.fiscal_year,
                o.fiscal_quarter,
                CASE
                    WHEN o.form_type = '10-K' THEN o.id
                    ELSE (
                        SELECT k.id
                        FROM ordered_filings k
                        WHERE k.company_id = o.company_id
                        AND k.form_type = '10-K'
                        AND k.seq > o.seq
                        ORDER BY k.seq
                        LIMIT 1
                    )
                END AS fiscal_tag
            FROM ordered_filings o
        ),
        all_filings_data AS (
            -- Get all filing data with proper quarter assignment based on fiscal_period_end
            SELECT
                ff.company_id,
                ff.filing_id,
                ff.id,
                ff.parent_id,
                ff.label,
                ff.normalized_label,
                CASE
                    -- opposite weights means we have to flip the value
                    WHEN ff.weight * FIRST_VALUE(ff.weight) OVER w < 0 THEN -1 * ff.value
                    ELSE ff.value
                END as value,
                ff.unit,
                ff.statement,
                ff.concept,
                ff.axis,
                ff.member,
                ff.period_end,
                ff.period,
                ff.is_abstract,
                ff.is_synthetic,
                ff.form_type as source_type,
                f.fiscal_year,
                f.fiscal_quarter,
                f.fiscal_tag,
                -- Get the latest abstract, position, and weight for this metric
                FIRST_VALUE(ff.abstract_id) OVER w AS latest_abstract_id,
                FIRST_VALUE(ff.position) OVER w AS latest_position,
                FIRST_VALUE(ff.weight) OVER w AS latest_weight
            FROM financial_facts_normalized ff
            JOIN filings_cte f
            ON
                ff.company_id = f.company_id
                AND ff.filing_id = f.id
            WINDOW w AS (
                PARTITION BY ff.company_id, ff.statement, ff.normalized_label, ff.axis, ff.member
                ORDER BY ff.period_end DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ),
        quarterly_filings_raw AS (
            -- Get quarterly data from 10-Q filings
            SELECT
                company_id,
                filing_id,
                id,
                parent_id,
                fiscal_year,
                fiscal_quarter,
                fiscal_tag,
                label,
                normalized_label,
                value,
                unit,
                statement,
                concept,
                axis,
                member,
                latest_abstract_id as abstract_id,
                latest_weight as weight,
                latest_position as position,
                period_end,
                period,
                is_abstract,
                is_synthetic,
                source_type
            FROM all_filings_data
            WHERE source_type = '10-Q'
        ),
        quarterly_filings_with_prev AS (
            -- Add previous quarter data for YTD conversion
            SELECT
                q.*,
                CASE
                    WHEN (period_end - LAG(period_end) OVER w) BETWEEN 80 AND 100
                    THEN LAG(value) OVER w
                    ELSE NULL
                END AS prev_value
            FROM quarterly_filings_raw q
            WINDOW w AS (
                PARTITION BY company_id, statement, normalized_label, axis, member
                ORDER BY period_end
            )
        ),
        quarterly_filings AS (
            -- Convert YTD data to quarterly values
            SELECT
                company_id,
                filing_id,
                id,
                parent_id,
                fiscal_year,
                fiscal_quarter,
                fiscal_tag,
                label,
                normalized_label,
                CASE
                    WHEN period = 'Q' THEN value
                    WHEN period = 'YTD' AND prev_value IS NULL THEN value
                    WHEN period = 'YTD' AND prev_value IS NOT NULL THEN value - prev_value
                    -- For NULL period (balance sheet items), use value as-is
                    ELSE value
                END as value,
                weight,
                unit,
                statement,
                concept,
                axis,
                member,
                abstract_id,
                period_end,
                position,
                is_abstract,
                is_synthetic,
                source_type
            FROM quarterly_filings_with_prev
        ),
        annual_filings AS (
            -- Get annual data from 10-K filings
            SELECT
                company_id,
                filing_id,
                id,
                parent_id,
                fiscal_year,
                fiscal_quarter,
                fiscal_tag,
                label,
                value,
                latest_weight as weight,
                unit,
                statement,
                concept,
                axis,
                member,
                latest_abstract_id as abstract_id,
                period_end,
                normalized_label,
                latest_position as position,
                is_abstract,
                is_synthetic,
                source_type
            FROM all_filings_data
            WHERE source_type = '10-K'
        ),
        quarterly_aggregation AS (
            SELECT
                company_id,
                statement,
                normalized_label,
                axis,
                member,
                fiscal_tag,
                SUM(value) AS value
            FROM quarterly_filings
            GROUP BY
                company_id,
                statement,
                normalized_label,
                axis,
                member,
                fiscal_tag
        ),
        missing_quarters AS (
            SELECT
                a.company_id,
                a.filing_id,
                a.id,
                a.parent_id,
                a.fiscal_year,
                a.fiscal_quarter,
                a.concept,
                a.label,
                a.value - q.value AS value,
                a.unit,
                a.weight,
                a.axis,
                a.member,
                a.statement,
                a.abstract_id,
                a.period_end,
                a.normalized_label,
                a.position,
                a.is_abstract,
                a.is_synthetic,
                'calculated' AS source_type
            FROM annual_filings a
            JOIN quarterly_aggregation q
            ON
                q.company_id = a.company_id
                AND q.statement = a.statement
                AND q.normalized_label = a.normalized_label
                AND q.axis = a.axis
                AND q.member = a.member
                AND q.fiscal_tag = a.fiscal_tag
            WHERE
                a.statement != 'Balance Sheet'
                AND a.normalized_label NOT ILIKE 'Shares Outstanding%'
        )

        -- Combine all quarterly data
        SELECT
            id,
            parent_id,
            company_id,
            filing_id,
            concept,
            label,
            normalized_label,
            value,
            weight,
            unit,
            axis,
            member,
            statement,
            abstract_id,
            period_end,
            fiscal_year,
            fiscal_quarter,
            position,
            is_abstract,
            is_synthetic,
            source_type
        FROM quarterly_filings

        UNION ALL

        -- Balance Sheet data is point in time accumulation
        SELECT
            id,
            parent_id,
            company_id,
            filing_id,
            concept,
            label,
            normalized_label,
            value,
            weight,
            unit,
            axis,
            member,
            statement,
            abstract_id,
            period_end,
            fiscal_year,
            fiscal_quarter,
            position,
            is_abstract,
            is_synthetic,
            source_type
        FROM annual_filings
        WHERE
            statement = 'Balance Sheet'
            OR normalized_label ILIKE 'Shares Outstanding%'

        UNION ALL

        SELECT
            id,
            parent_id,
            company_id,
            filing_id,
            concept,
            label,
            normalized_label,
            value,
            weight,
            unit,
            axis,
            member,
            statement,
            abstract_id,
            period_end,
            fiscal_year,
            fiscal_quarter,
            position,
            is_abstract,
            is_synthetic,
            source_type
        FROM missing_quarters;
    """
    )

    # Create unique index on quarterly_financials for concurrent refresh
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_quarterly_financials_unique_id
        ON quarterly_financials (id);
        """
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


def downgrade() -> None:
    # Drop the sort index
    op.execute("DROP INDEX IF EXISTS idx_quarterly_financials_order;")
    # Drop the unique index
    op.execute("DROP INDEX IF EXISTS idx_quarterly_financials_unique_composite;")
    op.execute("DROP INDEX IF EXISTS idx_quarterly_financials_unique_id;")
    # Drop the quarterly financials view
    op.execute("DROP VIEW IF EXISTS quarterly_financials;")
