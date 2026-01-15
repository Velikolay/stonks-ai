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
                (dno.normalized_axis_label IS NOT NULL) AS overridden,
                md5(ff.company_id || '|' || ff.axis || '|' || ff.member || '|' || ff.member_label || '|' || COALESCE(dno.normalized_axis_label, '') || '|' || COALESCE(dno.normalized_member_label, '') || '|' || 'grouping') as id
            FROM financial_facts ff
            LEFT JOIN dimension_normalization_overrides as dno
            ON
                (ff.axis = dno.axis AND dno.member = '*' AND dno.member_label = '*')
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
                COALESCE(b.normalized_axis_label, b.axis) AS normalized_axis_label,
                COALESCE(b.normalized_member_label, b.member_label) AS normalized_member_label,
                MAX(b.period_end) OVER (PARTITION BY g.group_id) AS group_max_period_end,
                b.overridden
            FROM groups_by_id g
            JOIN dimension_normalized_base b USING (id)
            ORDER BY
                g.group_id,

                -- prefer rows that already have normalized values
                (b.normalized_axis_label IS NOT NULL) DESC,
                (b.normalized_member_label IS NOT NULL) DESC,

                -- recent periods are preferred
                b.period_end DESC
        )

        SELECT
            b.company_id,
            b.axis,
            b.member,
            b.member_label,
            c.normalized_axis_label,
            c.normalized_member_label,
            g.group_id,
            c.group_max_period_end,
            c.overridden
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
            COALESCE(dng.normalized_axis_label, ff.axis) as normalized_axis_label,
            COALESCE(dng.normalized_member_label, ff.member_label) as normalized_member_label,
            ff.value * ff.weight as normalized_value,
            ff.comparative_value * ff.weight as normalized_comparative_value,
            COALESCE(dng.overridden, FALSE) as overridden
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
        ),

        matches AS (
          SELECT
            f1.company_id,
            f1.statement,
            f1.concept as concept1,
            f2.concept as concept2,
            f1.normalized_label as label1,
            f2.normalized_label as label2,
            f1.period_end as period_end1,
            f2.period_end as period_end2,
            f1.axis as axis1,
            f2.axis as axis2,
            f1.member as member1,
            f2.member as member2,
            f1.member_label as member_label1,
            f2.member_label as member_label2,
            f1.normalized_axis_label as normalized_axis_label1,
            f2.normalized_axis_label as normalized_axis_label2,
            f1.normalized_member_label as normalized_member_label1,
            f2.normalized_member_label as normalized_member_label2,
            f1.overridden as overridden1,
            f2.overridden as overridden2
          FROM facts f1
          JOIN facts f2
          ON
            f1.company_id = f2.company_id
            AND f1.form_type = f2.form_type
            AND f1.statement = f2.statement
            AND f1.normalized_label = f2.normalized_label
            AND f1.normalized_comparative_value = f2.normalized_value
            AND f1.comparative_period_end = f2.period_end
          WHERE
            (
                f1.normalized_axis_label <> f2.normalized_axis_label
                OR f1.normalized_member_label <> f2.normalized_member_label
            )
            AND f1.period_end > f2.period_end
        ),

        -- 1. Identify all starting points (newest side of each directed link)
        roots AS (
          SELECT
            company_id,
            statement,
            concept1 as root_concept,
            label1 as root_label,
            axis1 as root_axis,
            member1 as root_member,
            member_label1 as root_member_label,
            normalized_axis_label1 as root_normalized_axis_label,
            normalized_member_label1 as root_normalized_member_label,
            overridden1 as root_overridden,
            period_end1 as root_period
          FROM matches
        ),

        -- 2. Start from *every* root concept
        chain AS (
        SELECT
          r.company_id,
          r.statement,
          r.root_concept as concept,
          r.root_label as label,
          r.root_axis as axis,
          r.root_member as member,
          r.root_member_label as member_label,
          r.root_normalized_axis_label as normalized_axis_label,
          r.root_normalized_member_label as normalized_member_label,
          r.root_overridden as overridden,
          r.root_period as current_period,
          r.root_period as root_period,
          md5(r.company_id || '|' || r.statement || '|' || r.root_concept || '|' || r.root_label || '|' || r.root_axis || '|' || r.root_member || '|' || r.root_member_label || '|' || r.root_normalized_axis_label || '|' || r.root_normalized_member_label || '|' || 'chaining') AS group_id
        FROM roots r

        UNION ALL

        -- 3. Recursively traverse the directed edges forward in time (newer → older)
        SELECT
          c.company_id,
          c.statement,
          m.concept2 as concept,
          m.label2 as label,
          m.axis2 as axis,
          m.member2 as member,
          m.member_label2 as member_label,
          m.normalized_axis_label2 as normalized_axis_label,
          m.normalized_member_label2 as normalized_member_label,
          m.overridden2 as overridden,
          m.period_end2 as current_period,
          c.root_period as root_period,
          c.group_id as group_id
        FROM chain c
        JOIN matches m
          ON m.company_id = c.company_id
          AND m.statement = c.statement
          AND m.concept1 = c.concept
          AND m.label1 = c.label
          AND m.axis1 = c.axis
          AND m.member1 = c.member
          AND m.member_label1 = c.member_label
          AND m.normalized_axis_label1 = c.normalized_axis_label
          AND m.normalized_member_label1 = c.normalized_member_label
          AND m.period_end1 <= c.current_period
        ),

        -- 4. Select normalized values giving priority to overridden values and recent periods
        group_normalized AS (
            SELECT DISTINCT ON (group_id)
                group_id,
                normalized_axis_label,
                normalized_member_label,
                overridden
            FROM chain
            ORDER BY group_id, overridden DESC, current_period DESC
        )

        -- 5. Collapse duplicates (same concept can appear in multiple roots, take the latest one)
        SELECT DISTINCT ON (company_id, axis, member, member_label)
          company_id,
          axis,
          member,
          member_label,
          gn.normalized_axis_label,
          gn.normalized_member_label,
          group_id,
          root_period as group_max_period_end,
          gn.overridden
        FROM chain
        JOIN group_normalized gn USING (group_id)
        ORDER BY company_id, axis, member, member_label, normalized_axis_label, normalized_member_label, root_period DESC
        """
    )

    # Create merged view
    op.execute(
        """
        CREATE VIEW dimension_normalization AS

        SELECT DISTINCT ON (company_id, axis, member, member_label)
            company_id,
            axis,
            member,
            member_label,
            normalized_axis_label,
            normalized_member_label,
            group_id,
            group_max_period_end,
            overridden
        FROM (
            SELECT *, 1 AS src_priority
            FROM dimension_normalization_chaining

            UNION ALL

            SELECT *, 2 AS src_priority
            FROM dimension_normalization_grouping
        ) t
        ORDER BY
            company_id,
            axis,
            member,
            member_label,
            src_priority
        """
    )


def downgrade() -> None:
    # Drop the views
    op.execute("DROP VIEW IF EXISTS dimension_normalization")
    op.execute("DROP VIEW IF EXISTS dimension_normalization_chaining")
    op.execute("DROP VIEW IF EXISTS dimension_normalization_grouping")
