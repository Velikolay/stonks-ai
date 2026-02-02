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
                COALESCE(dnoc.normalized_axis_label, dnog.normalized_axis_label),
                COALESCE(dnoc.normalized_member_label, dnog.normalized_member_label)
            )
                ff.company_id,
                ff.axis,
                ff.member,
                ff.member_label,
                COALESCE(dnoc.normalized_axis_label, dnog.normalized_axis_label) as normalized_axis_label,
                COALESCE(dnoc.normalized_member_label, dnog.normalized_member_label) as normalized_member_label,
                ff.period_end,
                (COALESCE(dnoc.normalized_axis_label, dnog.normalized_axis_label) IS NOT NULL) AS overridden,
                CASE
                    WHEN COALESCE(dnoc.is_global, dnog.is_global) = FALSE THEN 'company'
                    WHEN COALESCE(dnoc.is_global, dnog.is_global) = TRUE THEN 'global'
                    ELSE NULL
                END as override_priority,
                CASE
                    WHEN COALESCE(dnoc.normalized_member_label, dnog.normalized_member_label) IS NOT NULL THEN 'member'
                    WHEN COALESCE(dnoc.normalized_axis_label, dnog.normalized_axis_label) IS NOT NULL THEN 'axis'
                    ELSE NULL
                END as override_level,
                md5(ff.company_id || '|' || ff.axis || '|' || ff.member || '|' || ff.member_label || '|' || COALESCE(dnoc.normalized_axis_label, dnog.normalized_axis_label, '') || '|' || COALESCE(dnoc.normalized_member_label, dnog.normalized_member_label, '') || '|' || 'grouping') as id
            FROM financial_facts ff
            LEFT JOIN dimension_normalization_overrides as dnoc
            ON
                ff.company_id = dnoc.company_id
                AND (
                    (ff.axis = dnoc.axis AND dnoc.member = '*' AND dnoc.member_label = '*')
                    OR (ff.axis = dnoc.axis AND ff.member = dnoc.member)
                    OR (ff.axis = dnoc.axis AND ff.member_label = dnoc.member_label)
                )
            LEFT JOIN dimension_normalization_overrides as dnog
            ON
                dnog.is_global = TRUE
                AND (
                    (ff.axis = dnog.axis AND dnog.member = '*' AND dnog.member_label = '*')
                    OR (ff.axis = dnog.axis AND ff.member = dnog.member)
                    OR (ff.axis = dnog.axis AND ff.member_label = dnog.member_label)
                )
            WHERE
                ff.axis <> ''
            ORDER BY
                ff.company_id,
                ff.axis,
                ff.member,
                ff.member_label,
                COALESCE(dnoc.normalized_axis_label, dnog.normalized_axis_label),
                COALESCE(dnoc.normalized_member_label, dnog.normalized_member_label),
                ff.period_end DESC
        ),
        exploded AS (
            SELECT id, member AS key FROM dimension_normalized_base
            UNION
            SELECT id, member_label AS key FROM dimension_normalized_base
            UNION
            SELECT id, normalized_member_label AS key FROM dimension_normalized_base
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
                b.overridden,
                b.override_priority,
                b.override_level
            FROM groups_by_id g
            JOIN dimension_normalized_base b USING (id)
            ORDER BY
                g.group_id,

                -- prefer rows that already have normalized values
                CASE b.override_priority
                    WHEN 'company' THEN 1
                    WHEN 'global' THEN 2
                    ELSE 3
                END,

                CASE b.override_level
                    WHEN 'member' THEN 1
                    WHEN 'axis' THEN 2
                    ELSE 3
                END,

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
            c.overridden,
            c.override_priority,
            c.override_level
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
            COALESCE(cnoc.normalized_label, cnog.normalized_label, cn.normalized_label, ff.label) as normalized_label,
            COALESCE(dng.normalized_axis_label, ff.axis) as normalized_axis_label,
            COALESCE(dng.normalized_member_label, ff.member_label) as normalized_member_label,
            ff.value * ff.weight as normalized_value,
            ff.comparative_value * ff.weight as normalized_comparative_value,
            COALESCE(dng.overridden, FALSE) as overridden,
            dng.override_priority,
            dng.override_level
          FROM financial_facts ff
          LEFT JOIN concept_normalization cn
            USING (company_id, statement, concept)
          LEFT JOIN concept_normalization_overrides cnoc
            USING (company_id, statement, concept)
          LEFT JOIN concept_normalization_overrides cnog
            ON ff.statement = cnog.statement
            AND ff.concept = cnog.concept
            AND cnog.is_global = TRUE
          LEFT JOIN dimension_normalization_grouping dng
            ON ff.company_id = dng.company_id
            AND ff.axis = dng.axis
            AND ff.member = dng.member
            AND ff.member_label = dng.member_label
            AND ff.period_end <= dng.group_max_period_end
          WHERE
            ff.axis <> ''
        ),

        same_period_pairs AS (
          SELECT DISTINCT
            f1.company_id,
            f1.statement,
            f1.normalized_label,
            f1.normalized_axis_label as normalized_axis_label1,
            f1.normalized_member_label as normalized_member_label1,
            f2.normalized_axis_label as normalized_axis_label2,
            f2.normalized_member_label as normalized_member_label2
          FROM facts f1
          JOIN facts f2
            ON f1.company_id = f2.company_id
            AND f1.statement = f2.statement
            AND f1.normalized_label = f2.normalized_label
            AND f1.period_end = f2.period_end
          WHERE NOT (
            f1.normalized_axis_label = f2.normalized_axis_label
            AND f1.normalized_member_label = f2.normalized_member_label
          )
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
            f2.overridden as overridden2,
            f1.override_priority as override_priority1,
            f2.override_priority as override_priority2,
            f1.override_level as override_level1,
            f2.override_level as override_level2
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
                (
                    (
                        NOT f1.overridden OR NOT f2.overridden
                    ) AND (
                        f1.normalized_axis_label <> f2.normalized_axis_label
                        OR f1.normalized_member_label <> f2.normalized_member_label
                    )
                ) OR (
                    f1.override_level = 'axis' AND f2.override_level = 'axis' AND f1.normalized_axis_label = f2.normalized_axis_label AND f1.normalized_member_label <> f2.normalized_member_label
                ) OR (
                    f1.override_level = 'axis' AND f2.override_level = 'member' AND f1.normalized_axis_label = f2.normalized_member_label AND f1.normalized_member_label <> f2.normalized_member_label
                ) OR (
                    f1.override_level = 'member' AND f2.override_level = 'axis' AND f1.normalized_member_label = f2.normalized_axis_label AND f1.normalized_axis_label <> f2.normalized_axis_label
                )
            )
            AND f1.period_end > f2.period_end
            AND NOT EXISTS (
              SELECT 1
              FROM same_period_pairs spp
              WHERE
                spp.company_id = f1.company_id
                AND spp.statement = f1.statement
                AND spp.normalized_label = f1.normalized_label
                AND spp.normalized_axis_label1 = f1.normalized_axis_label
                AND spp.normalized_member_label1 = f1.normalized_member_label
                AND spp.normalized_axis_label2 = f2.normalized_axis_label
                AND spp.normalized_member_label2 = f2.normalized_member_label
            )
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
            override_priority1 as root_override_priority,
            override_level1 as root_override_level,
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
          r.root_override_priority as override_priority,
          r.root_override_level as override_level,
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
          m.override_priority2 as override_priority,
          m.override_level2 as override_level,
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
                overridden,
                override_priority,
                override_level
            FROM chain
            ORDER BY
                group_id,

                -- prefer rows that already have normalized values
                CASE override_priority
                    WHEN 'company' THEN 1
                    WHEN 'global' THEN 2
                    ELSE 3
                END,

                CASE override_level
                    WHEN 'member' THEN 1
                    WHEN 'axis' THEN 2
                    ELSE 3
                END,

                -- recent periods are preferred
                current_period DESC
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
          gn.overridden,
          gn.override_priority,
          gn.override_level
        FROM chain
        JOIN group_normalized gn USING (group_id)
        ORDER BY company_id, axis, member, member_label, normalized_axis_label, normalized_member_label, root_period DESC
        """
    )

    # Create merged view
    op.execute(
        """
        CREATE VIEW dimension_normalization AS

        WITH base AS (
            SELECT * FROM dimension_normalization_grouping

            UNION ALL

            SELECT * FROM dimension_normalization_chaining
        ),
        groups AS (
            SELECT DISTINCT company_id, group_id
            FROM base
        ),
        group_members AS (
            -- group_id -> set of (company_id, axis, member, member_label)
            SELECT DISTINCT
                company_id,
                axis,
                member,
                member_label,
                group_id
            FROM base
        ),
        group_edges AS (
            -- connect groups that overlap on at least one (company_id, axis, member, member_label)
            SELECT DISTINCT
                gm1.company_id,
                gm1.group_id as group_id1,
                gm2.group_id as group_id2
            FROM group_members gm1
            JOIN group_members gm2
                ON gm1.company_id = gm2.company_id
                AND gm1.axis = gm2.axis
                AND gm1.member = gm2.member
                AND gm1.member_label = gm2.member_label
            WHERE gm1.group_id <> gm2.group_id
        ),
        group_components AS (
            -- connected components over group_ids, per company_id
            WITH RECURSIVE reach AS (
                SELECT company_id, group_id as root_group_id, group_id
                FROM groups

                UNION

                SELECT r.company_id, r.root_group_id, e.group_id2 as group_id
                FROM reach r
                JOIN group_edges e
                    ON e.company_id = r.company_id
                    AND e.group_id1 = r.group_id
            )
            SELECT
                company_id,
                group_id,
                MIN(root_group_id) as component_id
            FROM reach
            GROUP BY company_id, group_id
        ),
        component_canonical AS (
            -- pick one canonical normalized pair per connected component
            SELECT DISTINCT ON (gc.company_id, gc.component_id)
                gc.company_id,
                gc.component_id,
                b.normalized_axis_label,
                b.normalized_member_label,
                b.overridden,
                b.override_priority,
                b.override_level
            FROM group_components gc
            JOIN base b
                ON b.company_id = gc.company_id
                AND b.group_id = gc.group_id
            ORDER BY
                gc.company_id,
                gc.component_id,
                -- prefer higher-priority overrides
                CASE b.override_priority
                    WHEN 'company' THEN 1
                    WHEN 'global' THEN 2
                    ELSE 3
                END,
                CASE b.override_level
                    WHEN 'member' THEN 1
                    WHEN 'axis' THEN 2
                    ELSE 3
                END,
                -- prefer more recent groups, then chaining over grouping as tie-breaker
                b.group_max_period_end DESC
        ),
        component_group_max_period_end AS (
            SELECT
                gc.company_id,
                gc.component_id,
                MAX(b.group_max_period_end) as group_max_period_end
            FROM group_components gc
            JOIN base b
                ON b.company_id = gc.company_id
                AND b.group_id = gc.group_id
            GROUP BY gc.company_id, gc.component_id
        )

        SELECT DISTINCT ON (gm.company_id, gm.axis, gm.member, gm.member_label)
            gm.company_id,
            gm.axis,
            gm.member,
            gm.member_label,
            cc.normalized_axis_label,
            cc.normalized_member_label,
            gc.component_id as group_id,
            cgmp.group_max_period_end,
            cc.overridden,
            cc.override_priority,
            cc.override_level
        FROM group_members gm
        JOIN group_components gc
            ON gc.company_id = gm.company_id
            AND gc.group_id = gm.group_id
        JOIN component_canonical cc
            ON cc.company_id = gc.company_id
            AND cc.component_id = gc.component_id
        JOIN component_group_max_period_end cgmp
            ON cgmp.company_id = gc.company_id
            AND cgmp.component_id = gc.component_id
        ORDER BY
            gm.company_id,
            gm.axis,
            gm.member,
            gm.member_label,
            -- deterministic pick (all rows for a given element should agree after unification)
            gc.component_id
        """
    )


def downgrade() -> None:
    # Drop the views
    op.execute("DROP VIEW IF EXISTS dimension_normalization")
    op.execute("DROP VIEW IF EXISTS dimension_normalization_chaining")
    op.execute("DROP VIEW IF EXISTS dimension_normalization_grouping")
