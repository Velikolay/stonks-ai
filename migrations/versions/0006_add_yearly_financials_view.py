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
                ff.concept,
                ff.label,
                COALESCE(cno.normalized_label, cn.normalized_label, ff.label) as normalized_label,
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
                f.fiscal_year,
                cno.aggregation,
                -- Get the latest abstracts for this metric combination
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
                ff.form_type = '10-K'
                AND ff.is_abstract = FALSE
            WINDOW w AS (
                PARTITION BY ff.company_id, ff.statement, COALESCE(cno.normalized_label, cn.normalized_label, ff.label), ff.axis, ff.member
                ORDER BY ff.period_end DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        )
        SELECT
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
            latest_abstracts as abstracts,
            latest_abstract_concepts as abstract_concepts,
            period_end,
            fiscal_year,
            latest_position as position,
            aggregation,
            '10-K' as source_type
        FROM all_filings_data
        ORDER BY company_id, statement, period_end DESC, position
    """
    )

    # Create unique index on yearly_financials for concurrent refresh
    # Using a functional index to handle NULL values in axis and member columns
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_yearly_financials_unique
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
    op.execute("DROP INDEX IF EXISTS idx_yearly_financials_unique;")
    # Drop the yearly financials view
    op.execute("DROP VIEW IF EXISTS yearly_financials;")
