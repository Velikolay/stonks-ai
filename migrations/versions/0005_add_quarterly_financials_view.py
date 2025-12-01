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

        WITH all_filings_data AS (
            -- Get all filing data with proper quarter assignment based on fiscal_period_end
            SELECT
                ff.company_id,
                ff.filing_id,
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
                ff.form_type as source_type,
                f.fiscal_year,
                f.fiscal_quarter,
                cno.aggregation,
                -- Get the latest abstracts, position, and weight for this metric
                FIRST_VALUE(COALESCE(ano.path, an.path)) OVER w AS latest_abstracts,
                FIRST_VALUE(COALESCE(ano.concept_path, an.concept_path)) OVER w AS latest_abstract_concepts,
                FIRST_VALUE(ff.position) OVER w AS latest_position,
                FIRST_VALUE(ff.weight) OVER w AS latest_weight
            FROM financial_facts ff
            JOIN filings f
            ON
                ff.filing_id = f.id
            LEFT JOIN abstract_normalization_overrides ano
            ON
                ff.statement = ano.statement
                AND ff.concept = ano.concept
            LEFT JOIN abstract_normalization an
            ON
                ff.filing_id = an.filing_id
                AND ff.parent_id = an.id
            LEFT JOIN concept_normalization_overrides cno
            ON
                ff.statement = cno.statement
                AND ff.concept = cno.concept
            LEFT JOIN concept_normalization cn
            ON
                ff.company_id = cn.company_id
                AND ff.statement = cn.statement
                AND ff.concept = cn.concept
                AND ff.label = cn.label
            WHERE
                ff.is_abstract = FALSE
                -- AND ff.form_type IN ('10-Q', '10-K')
            WINDOW w AS (
                PARTITION BY ff.company_id, ff.statement, COALESCE(cno.normalized_label, cn.normalized_label, ff.label), ff.axis, ff.member
                ORDER BY ff.period_end DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ),
        quarterly_filings_raw AS (
            -- Get quarterly data from 10-Q filings
            SELECT
                company_id,
                filing_id,
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
                latest_abstract_concepts as abstract_concepts,
                latest_weight as weight,
                latest_position as position,
                period_end,
                period,
                aggregation,
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
                abstract_concepts,
                period_end,
                position,
                aggregation,
                source_type
            FROM quarterly_filings_with_prev
        ),
        annual_filings AS (
            -- Get annual data from 10-K filings
            SELECT
                company_id,
                filing_id,
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
                latest_abstract_concepts as abstract_concepts,
                period_end,
                normalized_label,
                latest_position as position,
                aggregation,
                source_type
            FROM all_filings_data
            WHERE source_type = '10-K'
        ),
        quarterly_with_ranks AS (
            -- Rank quarterly filings for each annual filing
            SELECT
                q.*,
                a.company_id as k_company_id,
                a.filing_id as k_filing_id,
                a.fiscal_year as k_fiscal_year,
                a.fiscal_quarter as k_fiscal_quarter,
                a.concept as k_concept,
                a.value as k_value,
                a.weight as k_weight,
                a.unit as k_unit,
                a.parsed_axis as k_parsed_axis,
                a.parsed_member as k_parsed_member,
                a.statement as k_statement,
                a.abstracts as k_abstracts,
                a.abstract_concepts as k_abstract_concepts,
                a.period_end as k_period_end,
                a.label as k_label,
                a.normalized_label as k_normalized_label,
                a.position as k_position,
                a.aggregation as k_aggregation,
                ROW_NUMBER() OVER (
                    PARTITION BY q.company_id, q.statement, q.normalized_label, q.axis, q.member, a.period_end
                    ORDER BY q.period_end DESC
                ) as rn
            FROM quarterly_filings q
            JOIN annual_filings a ON
                q.company_id = a.company_id
                AND q.statement = a.statement
                AND q.normalized_label = a.normalized_label
                AND q.axis = a.axis
                AND q.member = a.member
                AND q.period_end < a.period_end
        ),
        missing_quarters AS (
            SELECT
                k_company_id as company_id,
                k_filing_id as filing_id,
                k_fiscal_year as fiscal_year,
                k_fiscal_quarter as fiscal_quarter,
                k_concept as concept,
                k_label as label,
                k_value - COALESCE(SUM(value) FILTER (WHERE rn <= 3), 0) as value,
                k_unit as unit,
                k_weight as weight,
                k_parsed_axis as parsed_axis,
                k_parsed_member as parsed_member,
                k_statement as statement,
                k_abstracts as abstracts,
                K_abstract_concepts as abstract_concepts,
                k_period_end as period_end,
                k_normalized_label as normalized_label,
                k_position as position,
                k_aggregation as aggregation,
                'calculated' as source_type
            FROM quarterly_with_ranks
            -- Balance Sheet data is snapshot in time accumulation so we don't need to calculate it quarterly
            WHERE
                k_statement != 'Balance Sheet'
                AND k_normalized_label NOT ILIKE 'Shares Outstanding%'
            GROUP BY k_company_id, k_filing_id, k_fiscal_year, k_fiscal_quarter, k_concept, k_label, k_normalized_label, k_value, k_unit, k_weight, k_parsed_axis, k_parsed_member, k_statement, k_period_end, k_abstracts, k_abstract_concepts, k_position, k_aggregation
            HAVING COUNT(*) FILTER (WHERE rn <= 3) = 3
        ),
        normalized_concepts AS (
            -- Combine all quarterly data
            SELECT
                company_id,
                filing_id,
                concept,
                label,
                normalized_label,
                value,
                weight,
                unit,
                parsed_axis as axis,
                parsed_member as member,
                statement,
                abstracts,
                abstract_concepts,
                period_end,
                fiscal_year,
                fiscal_quarter,
                position,
                aggregation,
                source_type
            FROM quarterly_filings

            UNION ALL

            -- Balance Sheet data is point in time accumulation
            SELECT
                company_id,
                filing_id,
                concept,
                label,
                normalized_label,
                value,
                weight,
                unit,
                parsed_axis as axis,
                parsed_member as member,
                statement,
                abstracts,
                abstract_concepts,
                period_end,
                fiscal_year,
                fiscal_quarter,
                position,
                aggregation,
                source_type
            FROM annual_filings
            WHERE
                statement = 'Balance Sheet'
                OR normalized_label ILIKE 'Shares Outstanding%'

            UNION ALL

            SELECT
                company_id,
                filing_id,
                concept,
                label,
                normalized_label,
                value,
                weight,
                unit,
                parsed_axis as axis,
                parsed_member as member,
                statement,
                abstracts,
                abstract_concepts,
                period_end,
                fiscal_year,
                fiscal_quarter,
                position,
                aggregation,
                source_type
            FROM missing_quarters
            WHERE value IS NOT NULL AND value != 0
        ),
        -- Calculate missing group aggregations / totals
        aggregated_concepts AS (
            SELECT DISTINCT ON (company_id, statement, normalized_label)
                *
            FROM normalized_concepts
            WHERE aggregation IS NOT NULL
            ORDER BY company_id, statement, normalized_label, period_end DESC
        ),
        missing_aggregated_concepts AS (
            SELECT
                nc.company_id,
                nc.filing_id,
                ac.concept,
                ac.label,
                ac.normalized_label,
                SUM(nc.value * nc.weight),
                ac.weight,
                ac.unit,
                ac.axis,
                ac.member,
                ac.statement,
                ac.abstracts,
                ac.abstract_concepts,
                nc.period_end,
                nc.fiscal_year,
                nc.fiscal_quarter,
                ac.position,
                ac.aggregation,
                nc.source_type
            FROM normalized_concepts nc
            JOIN aggregated_concepts ac
            ON
                nc.company_id = ac.company_id
                AND nc.statement = ac.statement
                AND nc.abstracts = ac.abstracts
            WHERE
                nc.normalized_label != ac.normalized_label
                AND NOT EXISTS (
                    SELECT 1 FROM normalized_concepts nc_check
                    WHERE nc_check.company_id = nc.company_id
                        AND nc_check.filing_id = nc.filing_id
                        AND nc_check.statement = nc.statement
                        AND nc_check.normalized_label = ac.normalized_label
                )
            GROUP BY
                nc.company_id,
                nc.filing_id,
                ac.concept,
                ac.label,
                ac.normalized_label,
                ac.weight,
                ac.unit,
                ac.axis,
                ac.member,
                ac.statement,
                ac.abstracts,
                ac.abstract_concepts,
                nc.period_end,
                nc.fiscal_year,
                nc.fiscal_quarter,
                ac.position,
                ac.aggregation,
                nc.source_type
        )
        SELECT * FROM normalized_concepts
        UNION ALL
        SELECT * FROM missing_aggregated_concepts
        ORDER BY company_id, statement, position, period_end DESC;
    """
    )

    # Create unique index on quarterly_financials for concurrent refresh
    # Using a functional index to handle NULL values in axis and member columns
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_quarterly_financials_unique
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


def downgrade() -> None:
    # Drop the unique index
    op.execute("DROP INDEX IF EXISTS idx_quarterly_financials_unique;")
    # Drop the quarterly financials view
    op.execute("DROP VIEW IF EXISTS quarterly_financials;")
