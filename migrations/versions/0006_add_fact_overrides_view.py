"""Add facts overrides view

Revision ID: 0006
Revises: 0005
Create Date: 2024-12-20 12:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE VIEW financial_facts_overridden AS

        SELECT
            ff.id,
            ff.filing_id,
            ff.company_id,
            ff.form_type,
            COALESCE(r.to_concept, ff.concept) AS concept,
            ff.label,
            ff.is_abstract,
            ff.value,
            ff.comparative_value,
            ff.weight,
            ff.unit,
            COALESCE(r.to_axis, ff.axis) AS axis,
            COALESCE(r.to_member, ff.member) AS member,
            ff.member_label,
            ff.statement,
            ff.period_end,
            ff.comparative_period_end,
            ff.period,
            ff.position,
            ff.parent_id,
            ff.abstract_id,
            r.id AS fact_override_id
        FROM financial_facts ff
        LEFT JOIN LATERAL (
            SELECT r.*
            FROM financial_facts_overrides r
            WHERE
                r.statement = ff.statement
                AND r.concept = ff.concept
                AND (r.company_id = ff.company_id OR r.is_global = TRUE)
                AND (r.axis IS NULL OR r.axis = ff.axis)
                AND (r.member IS NULL OR r.member = ff.member)
                AND (r.label IS NULL OR r.label = ff.label)
                AND (r.form_type IS NULL OR r.form_type = ff.form_type)
                AND (
                    r.from_period IS NULL
                    OR ff.period_end >= r.from_period::date
                )
                AND (
                    r.to_period IS NULL
                    OR ff.period_end <= r.to_period::date
                )
            ORDER BY
                (r.company_id = ff.company_id) DESC,
                (
                    (r.axis IS NOT NULL)::int
                    + (r.member IS NOT NULL)::int
                    + (r.label IS NOT NULL)::int
                    + (r.form_type IS NOT NULL)::int
                    + (r.from_period IS NOT NULL)::int
                    + (r.to_period IS NOT NULL)::int
                ) DESC,
                r.updated_at DESC
            LIMIT 1
        ) r ON TRUE
        """
    )


def downgrade() -> None:
    # Drop the views
    op.execute("DROP VIEW IF EXISTS financial_facts_overridden")
