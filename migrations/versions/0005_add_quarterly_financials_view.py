"""add_quarterly_financials_view

Revision ID: 0005
Revises: 0004
Create Date: 2025-08-22 01:15:46.030435

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create quarterly financial metrics view with correct quarter assignment
    # based on actual fiscal period end dates rather than assuming 10-K = Q4

    op.execute(
        """
        CREATE MATERIALIZED VIEW quarterly_financials AS
        WITH RECURSIVE abstracts AS (
            SELECT id, parent_id, filing_id, label, ARRAY[label] AS path
            FROM financial_facts
            WHERE parent_id IS NULL AND is_abstract = TRUE

            UNION ALL

            SELECT ff.id, ff.parent_id, ff.filing_id, ff.label, a.path || ff.label
            FROM financial_facts ff
            JOIN abstracts a ON ff.parent_id = a.id AND ff.filing_id = a.filing_id
            WHERE ff.is_abstract = TRUE
        ),
        all_filings_data AS (
            -- Get all filing data with proper quarter assignment based on fiscal_period_end
            SELECT
                f.company_id,
                f.fiscal_year,
                f.fiscal_quarter,  -- This is already calculated correctly in the filings table
                ff.label,
                COALESCE(cno.normalized_label, cn.normalized_label, ff.label) as normalized_label,
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
                ff.parsed_axis,
                ff.parsed_member,
                ff.period_end,
                ff.period,
                f.form_type as source_type,
                f.fiscal_period_end,
                -- Get the latest abstracts, position, and weight for this metric
                FIRST_VALUE(a.path) OVER w AS latest_abstracts,
                FIRST_VALUE(ff.position) OVER w AS latest_position,
                FIRST_VALUE(ff.weight) OVER w AS latest_weight
            FROM financial_facts ff
            JOIN filings f
            ON
                ff.filing_id = f.id
            LEFT JOIN abstracts a
            ON
                ff.filing_id = a.filing_id
                AND ff.parent_id = a.id
            LEFT JOIN concept_normalization_overrides cno
            ON
                ff.statement = cno.statement
                AND ff.concept = cno.concept
            LEFT JOIN concept_normalization cn
            ON
                f.company_id = cn.company_id
                AND ff.statement = cn.statement
                AND ff.concept = cn.concept
                AND ff.label = cn.label
            WHERE
                f.form_type IN ('10-Q', '10-K')
                AND ff.is_abstract = FALSE
            WINDOW w AS (
                PARTITION BY f.company_id, ff.statement, COALESCE(cno.normalized_label, cn.normalized_label, ff.label), ff.axis, ff.member
                ORDER BY f.fiscal_period_end DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ),
        quarterly_filings_raw AS (
            -- Get quarterly data from 10-Q filings
            SELECT
                company_id,
                fiscal_year,
                fiscal_quarter,
                label,
                normalized_label,
                value,
                unit,
                statement,
                concept,
                axis,
                member,
                parsed_axis,
                parsed_member,
                latest_abstracts as abstracts,
                latest_weight as weight,
                latest_position as position,
                period_end,
                period,
                fiscal_period_end,
                source_type
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
                parsed_axis,
                parsed_member,
                abstracts,
                period_end,
                fiscal_period_end,
                position,
                source_type
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
                latest_weight as weight,
                unit,
                statement,
                concept,
                axis,
                member,
                parsed_axis,
                parsed_member,
                latest_abstracts as abstracts,
                period_end,
                normalized_label,
                fiscal_period_end,
                latest_position as position,
                source_type
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
                a.weight as k_weight,
                a.unit as k_unit,
                a.parsed_axis as k_parsed_axis,
                a.parsed_member as k_parsed_member,
                a.statement as k_statement,
                a.abstracts as k_abstracts,
                a.period_end as k_period_end,
                a.fiscal_period_end as k_fiscal_period_end,
                a.label as k_label,
                a.normalized_label as k_normalized_label,
                a.position as k_position,
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
                k_weight as weight,
                k_parsed_axis as parsed_axis,
                k_parsed_member as parsed_member,
                k_statement as statement,
                k_abstracts as abstracts,
                k_period_end as period_end,
                k_fiscal_period_end as fiscal_period_end,
                k_normalized_label as normalized_label,
                k_position as position,
                'calculated' as source_type
            FROM quarterly_with_ranks
            -- Balance Sheet data is snapshot in time accumulation so we don't need to calculate it quarterly
            WHERE
                k_statement != 'Balance Sheet'
                AND k_normalized_label NOT ILIKE 'Shares Outstanding%'
            GROUP BY k_company_id, k_fiscal_year, k_fiscal_quarter, k_label, k_normalized_label, k_value, k_unit, k_weight, k_parsed_axis, k_parsed_member, k_statement, k_period_end, k_fiscal_period_end, k_abstracts, k_position
            HAVING COUNT(*) FILTER (WHERE rn <= 3) = 3
        )

        -- Combine all quarterly data
        SELECT
            company_id,
            label,
            normalized_label,
            value,
            weight,
            unit,
            parsed_axis as axis,
            parsed_member as member,
            statement,
            abstracts,
            period_end,
            fiscal_year,
            fiscal_quarter,
            position,
            source_type
        FROM quarterly_filings

        UNION ALL

        -- Balance Sheet data is point in time accumulation
        SELECT
            company_id,
            label,
            normalized_label,
            value,
            weight,
            unit,
            parsed_axis as axis,
            parsed_member as member,
            statement,
            abstracts,
            period_end,
            fiscal_year,
            fiscal_quarter,
            position,
            source_type
        FROM annual_filings
        WHERE
            statement = 'Balance Sheet'
            OR normalized_label ILIKE 'Shares Outstanding%'

        UNION ALL

        SELECT
            company_id,
            label,
            normalized_label,
            value,
            weight,
            unit,
            parsed_axis as axis,
            parsed_member as member,
            statement,
            abstracts,
            period_end,
            fiscal_year,
            fiscal_quarter,
            position,
            source_type
        FROM missing_quarters
        WHERE value IS NOT NULL AND value != 0
        ORDER BY company_id, statement, fiscal_year DESC, fiscal_quarter DESC, position;
    """
    )


def downgrade() -> None:
    # Drop the quarterly financials view
    op.execute("DROP VIEW IF EXISTS quarterly_financials;")
