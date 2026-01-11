"""Add dimension normalization views

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

    # Create view for axis normalization
    op.execute(
        """
        CREATE VIEW dimension_normalization_grouping AS

        WITH RECURSIVE dimension_normalized_base AS (
            SELECT DISTINCT ON (
                ff.company_id,
                ff.axis,
                ff.member,
                ff.member_label,
                dno.normalized_axis_label,
                dno.normalized_member_label
            )
                ff.company_id,
                ff.axis,
                ff.member,
                ff.member_label,
                dno.normalized_axis_label,
                dno.normalized_member_label,
                ff.period_end,
                md5(ff.company_id || '|' || ff.axis || '|' || ff.member || '|' || ff.member_label || '|' || COALESCE(dno.normalized_axis_label, '') || '|' || COALESCE(dno.normalized_member_label, '')) as id
            FROM financial_facts ff
            LEFT JOIN dimension_normalization_overrides as dno
            ON
                (ff.axis = dno.axis AND dno.member = '' AND dno.member_label = '')
                OR (ff.axis = dno.axis AND ff.member = dno.member)
                OR (ff.axis = dno.axis AND ff.member_label = dno.member_label)
            WHERE
                ff.axis <> ''
            ORDER BY
                ff.company_id,
                ff.axis,
                ff.member,
                ff.member_label,
                dno.normalized_axis_label,
                dno.normalized_member_label,
                ff.period_end DESC
        ),
        exploded AS (
            SELECT id, member AS key FROM dimension_normalized_base
            UNION ALL
            SELECT id, COALESCE(normalized_member_label, member_label) AS key FROM dimension_normalized_base
        ),
        edges AS (
            SELECT DISTINCT
                e1.id AS src,
                e2.id AS dst
            FROM exploded e1
            JOIN exploded e2
                ON e1.key = e2.key
        ),
        groups AS (
            -- base case: every row starts as its own component
            SELECT
                id,
                id AS group_id
            FROM dimension_normalized_base

            UNION

            -- recursive step: spread group ids across edges
            SELECT
                e.dst AS id,
                c.group_id
            FROM groups c
            JOIN edges e
                ON e.src = c.id
        ),
        groups_by_id AS (
            SELECT
                id,
                MIN(group_id) AS group_id
            FROM groups
            GROUP BY id
        ),
        canonical AS (
            SELECT DISTINCT ON (g.group_id)
                g.group_id,
                COALESCE(b.normalized_member_label, b.member_label) AS normalized_member_label,
                COALESCE(b.normalized_axis_label, b.axis) AS normalized_axis_label,
                b.period_end AS group_max_period_end
            FROM groups_by_id g
            JOIN dimension_normalized_base b USING (id)
            ORDER BY
                g.group_id,

                -- prefer rows that already have normalized values
                (b.normalized_member_label IS NOT NULL) DESC,
                (b.normalized_axis_label  IS NOT NULL) DESC,

                -- recent periods are preferred
                b.period_end DESC,

                -- deterministic tie-break
                b.id ASC
        )

        SELECT
            b.company_id,
            b.axis,
            b.member,
            b.member_label,
            c.normalized_axis_label,
            c.normalized_member_label,
            g.group_id,
            c.group_max_period_end
        FROM dimension_normalized_base b
        JOIN groups_by_id g USING (id)
        JOIN canonical c USING (group_id)
        """
    )

    # Create view for chaining dimension normalization
    op.execute(
        """
        CREATE VIEW dimension_normalization_chaining AS

        WITH RECURSIVE facts AS (
          SELECT
            ff.*,
            -- apply the grouping normalization so it is part of the chaining algorithm
            -- this avoids complex combining logic later on
            COALESCE(cno.normalized_label, cn.normalized_label, ff.label) as normalized_label,
            ff.value * ff.weight as normalized_value,
            ff.comparative_value * ff.weight as normalized_comparative_value
          FROM financial_facts ff
          LEFT JOIN concept_normalization cn
            USING (company_id, statement, concept)
          LEFT JOIN concept_normalization_overrides cno
            USING (statement, concept)
          LEFT JOIN dimension_normalization_grouping dng
            ON ff.company_id = dng.company_id
            AND ff.axis = dng.axis
            AND ff.member = dng.member
            AND ff.member_label = dng.member_label
            AND ff.period_end <= dng.group_max_period_end
          WHERE
            ff.axis <> ''
        )

        SELECT * FROM facts
        """
    )

    # # Create merged view
    # op.execute(
    #     """
    #     CREATE VIEW dimension_normalization_combined AS

    #     SELECT DISTINCT ON (company_id, statement, concept)
    #         company_id,
    #         statement,
    #         concept,
    #         normalized_label,
    #         group_id,
    #         group_max_period_end
    #     FROM (
    #         SELECT *, 1 AS src_priority
    #         FROM concept_normalization_chaining

    #         UNION ALL

    #         SELECT *, 2 AS src_priority
    #         FROM concept_normalization_grouping
    #     ) t
    #     ORDER BY
    #         company_id,
    #         statement,
    #         concept,
    #         src_priority
    #     """
    # )

    # op.execute(
    #     """
    #     CREATE VIEW dimension_normalization AS

    #     WITH combined AS (
    #         SELECT * FROM concept_normalization_combined
    #     ),
    #     group_overrides AS (
    #       SELECT
    #         cn.group_id,
    #         MAX(cno.normalized_label) as normalized_label,
    #         MAX(cno.weight) as weight,
    #         MAX(cno.unit) as unit
    #       FROM combined cn
    #       JOIN concept_normalization_overrides cno
    #       ON cn.statement = cno.statement
    #       AND cn.concept = cno.concept
    #       GROUP BY cn.group_id
    #     )

    #     SELECT
    #       cn.company_id,
    #       cn.statement,
    #       cn.concept,
    #       COALESCE(go.normalized_label, cn.normalized_label) as normalized_label,
    #       -- COALESCE(go.weight, cn.weight) as weight,
    #       go.weight as weight,
    #       -- COALESCE(go.unit, cn.unit) as unit,
    #       go.unit as unit,
    #       cn.group_id,
    #       go.normalized_label IS NOT NULL as overridden
    #     FROM combined cn
    #     LEFT JOIN group_overrides go
    #     ON cn.group_id = go.group_id
    #     """
    # )


def downgrade() -> None:
    # Drop the views
    op.execute("DROP VIEW IF EXISTS dimension_normalization_grouping")
    # op.execute("DROP VIEW IF EXISTS dimension_normalization")
    # op.execute("DROP VIEW IF EXISTS dimension_normalization_combined")
    # op.execute("DROP VIEW IF EXISTS dimension_normalization_chaining")
    # op.execute("DROP VIEW IF EXISTS dimension_normalization_grouping")
