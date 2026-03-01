"""Add concept normalization views

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

    # Create view for grouping concept normalization
    op.execute(
        """
        CREATE VIEW concept_normalization_grouping AS

        SELECT
            company_id,
            statement,
            concept,
            (ARRAY_AGG(label ORDER BY period_end DESC))[1] AS normalized_label,
            md5(company_id || '|' || statement || '|' || concept || '|' || 'grouping') AS group_id,
            MAX(period_end) AS group_max_period_end
        FROM financial_facts ff
        WHERE axis = ''
        GROUP BY
            company_id,
            statement,
            concept
        HAVING
            COUNT(DISTINCT label) > 1
            -- filters concepts that appear more than once in the same filing / statement
            AND COUNT(DISTINCT (filing_id, label)) = COUNT(DISTINCT filing_id)
        """
    )

    # Create view for chaining concept normalization
    op.execute(
        """
        CREATE VIEW concept_normalization_chaining AS

        WITH RECURSIVE facts AS (
          SELECT
            ff.*,
            -- apply the grouping normalization so it is part of the chaining algorithm
            -- this avoids complex combining logic later on
            COALESCE(cng.normalized_label, ff.label) as normalized_label,
            ff.value * ff.weight as normalized_value,
            ff.comparative_value * ff.weight as normalized_comparative_value
          FROM financial_facts ff
          LEFT JOIN concept_normalization_grouping cng
            ON ff.company_id = cng.company_id
            AND ff.statement = cng.statement
            AND ff.concept = cng.concept
            AND ff.period_end <= cng.group_max_period_end
          WHERE
            ff.axis = ''
        ),

        candidate_matches AS (
          SELECT DISTINCT ON (f1.company_id, f1.statement, f1.concept, f2.concept, f1.period_end, f2.period_end)
            f1.company_id,
            f1.statement,
            f1.concept as concept1,
            f2.concept as concept2,
            f1.period_end as period_end1,
            f2.period_end as period_end2,
            f1.normalized_label as label1,
            f2.normalized_label as label2
          FROM facts f1
          JOIN facts f2
          ON
            f1.company_id = f2.company_id
            AND f1.form_type = f2.form_type
            AND f1.statement = f2.statement
            AND f1.normalized_comparative_value = f2.normalized_value
            AND f1.comparative_period_end = f2.period_end
          WHERE
            f1.concept <> f2.concept
            AND f1.period_end > f2.period_end
            AND NOT EXISTS (
                SELECT 1
                FROM financial_facts fx
                WHERE fx.company_id = f1.company_id
                    AND fx.statement = f1.statement
                    AND fx.period_end = f1.period_end
                    AND fx.concept = f2.concept
            )
            AND NOT EXISTS (
                SELECT 1
                FROM financial_facts fx
                WHERE fx.company_id = f2.company_id
                    AND fx.statement = f2.statement
                    AND fx.period_end = f2.period_end
                    AND fx.concept = f1.concept
            )
          ORDER BY f1.company_id, f1.statement, f1.concept, f2.concept, f1.period_end, f2.period_end
        ),

        overlapping_matches AS (
          SELECT DISTINCT ON (a.company_id, a.statement, a.concept1, a.concept2)
            a.company_id,
            a.statement,
            a.concept1,
            a.concept2,
            a.label1,
            a.label2
          FROM candidate_matches a
          JOIN candidate_matches b
            ON a.company_id = b.company_id
            AND a.statement = b.statement
            AND a.concept1 = b.concept1
            AND a.concept2 = b.concept2
            AND (
              a.period_end1 = b.period_end2
              OR a.period_end2 = b.period_end1
            )
          ORDER BY a.company_id, a.statement, a.concept1, a.concept2
        ),

        mirror_matches AS (
          SELECT DISTINCT ON (a.company_id, a.statement, a.concept1, a.concept2)
            a.company_id,
            a.statement,
            a.concept1,
            a.concept2,
            a.label1,
            a.label2
          FROM candidate_matches a
          JOIN candidate_matches b
            ON a.company_id = b.company_id
            AND a.statement = b.statement
            AND a.concept1 = b.concept2
            AND a.concept2 = b.concept1
            AND a.period_end1 = b.period_end1
            AND a.period_end2 = b.period_end2
          ORDER BY a.company_id, a.statement, a.concept1, a.concept2
        ),

        false_matches AS (
          SELECT * FROM overlapping_matches
          UNION
          SELECT * FROM mirror_matches
        ),

        matches AS (
          SELECT
            cm.company_id,
            cm.statement,
            cm.label1,
            cm.label2,
            cm.concept1,
            cm.concept2,
            cm.period_end1,
            cm.period_end2
          FROM candidate_matches cm
            WHERE
            NOT EXISTS (
              SELECT 1
              FROM false_matches fm
              WHERE
              cm.company_id = fm.company_id
              AND cm.statement = fm.statement
              AND cm.concept1 = fm.concept1
              AND cm.concept2 = fm.concept2
            )
        ),

        -- 1. Identify all starting points (newest side of each directed link)
        roots AS (
          SELECT
            company_id,
            statement,
            concept1 as root_concept,
            label1 as root_label,
            period_end1 as root_period
          FROM matches
        ),

        -- 2. Start from *every* root concept
        chain AS (
        SELECT
          r.company_id,
          r.statement,
          r.root_concept as concept,
          r.root_label as normalized_label,
          r.root_period as current_period,
          r.root_period as root_period,
          md5(r.company_id || '|' || r.statement || '|' || r.root_concept || '|' || 'chaining') AS group_id
        FROM roots r

        UNION ALL

        -- 3. Recursively traverse the directed edges forward in time (newer → older)
        SELECT
          c.company_id,
          c.statement,
          m.concept2 as concept,
          c.normalized_label,
          m.period_end2 as current_period,
          c.root_period as root_period,
          c.group_id as group_id
        FROM chain c
        JOIN matches m
          ON m.company_id = c.company_id
          AND m.statement = c.statement
          AND m.concept1 = c.concept
          AND m.period_end1 <= c.current_period
        )

        -- 4. Collapse duplicates (same concept can appear in multiple roots, take the latest one)
        SELECT DISTINCT ON (company_id, statement, concept)
          company_id,
          statement,
          concept,
          normalized_label,
          group_id,
          root_period as group_max_period_end
        FROM chain
        ORDER BY company_id, statement, concept, root_period DESC
        """
    )

    # Create merged view
    op.execute(
        """
        CREATE VIEW concept_normalization_combined AS

        SELECT DISTINCT ON (company_id, statement, concept)
            company_id,
            statement,
            concept,
            normalized_label,
            group_id,
            group_max_period_end
        FROM (
            SELECT *, 1 AS src_priority
            FROM concept_normalization_chaining

            UNION ALL

            SELECT *, 2 AS src_priority
            FROM concept_normalization_grouping
        ) t
        ORDER BY
            company_id,
            statement,
            concept,
            src_priority
        """
    )

    op.execute(
        """
        CREATE VIEW concept_normalization AS

        WITH combined AS (
            SELECT * FROM concept_normalization_combined
        ),
        global_group_overrides AS (
          SELECT
            cn.group_id,
            MAX(cno.normalized_label) as normalized_label,
            MAX(cno.weight) as weight,
            MAX(cno.unit) as unit
          FROM combined cn
          JOIN concept_normalization_overrides cno
          ON cn.statement = cno.statement
            AND cn.concept = cno.concept
            AND cno.is_global = TRUE
          GROUP BY cn.group_id
        ),
        company_group_overrides AS (
          SELECT
            cn.group_id,
            cn.company_id,
            MAX(cno.normalized_label) as normalized_label,
            MAX(cno.weight) as weight,
            MAX(cno.unit) as unit
          FROM combined cn
          JOIN concept_normalization_overrides cno
          ON cn.company_id = cno.company_id
            AND cn.statement = cno.statement
            AND cn.concept = cno.concept
          GROUP BY
            cn.group_id,
            cn.company_id
        )

        SELECT
          cn.company_id,
          cn.statement,
          cn.concept,
          COALESCE(cgo.normalized_label, ggo.normalized_label, cn.normalized_label) as normalized_label,
          -- COALESCE(cgo.weight, ggo.weight, cn.weight) as weight,
          COALESCE(cgo.weight, ggo.weight) as weight,
          -- COALESCE(cgo.unit, ggo.unit, cn.unit) as unit,
          COALESCE(cgo.unit, ggo.unit) as unit,
          cn.group_id,
          COALESCE(cgo.normalized_label, ggo.normalized_label) IS NOT NULL as overridden
        FROM combined cn
        LEFT JOIN company_group_overrides cgo
        ON cn.company_id = cgo.company_id
          AND cn.group_id = cgo.group_id
        LEFT JOIN global_group_overrides ggo
        ON cn.group_id = ggo.group_id
        """
    )

    op.execute(
        """
        CREATE VIEW parent_normalization_expansion AS

        WITH concept_normalization_cte AS (
          SELECT * FROM concept_normalization
        ),
        concept_expansion AS (
            SELECT
                cne.company_id,
                cne.statement,
                cno.concept,
                cno.parent_concept,
                cne.concept as concept_expand
            FROM concept_normalization_cte cn
            JOIN LATERAL (
                SELECT
                    *
                FROM
                    concept_normalization_overrides o
                WHERE
                    o.statement = cn.statement
                    AND o.concept = cn.concept
                    AND (
                        o.company_id = cn.company_id
                        OR o.is_global = TRUE
                    )
                ORDER BY
                    (o.company_id = cn.company_id) DESC
                LIMIT 1
            ) cno ON TRUE
            JOIN concept_normalization_cte cne
            ON
                cn.group_id = cne.group_id
            WHERE cno.parent_concept IS NOT NULL
        ),
        parent_concept_expansion AS (
            SELECT
                cne.company_id,
                cne.statement,
                cno.concept,
                cno.parent_concept,
                cne.concept as parent_concept_expand
            FROM concept_normalization_cte cn
            JOIN LATERAL (
                SELECT
                    *
                FROM
                    concept_normalization_overrides o
                WHERE
                    o.statement = cn.statement
                    AND o.parent_concept = cn.concept
                    AND (
                        o.company_id = cn.company_id
                        OR o.is_global = TRUE
                    )
                ORDER BY
                    (o.company_id = cn.company_id) DESC
                LIMIT 1
            ) cno ON TRUE
            JOIN concept_normalization_cte cne
            ON
                cn.group_id = cne.group_id
            WHERE cno.parent_concept IS NOT NULL
        ),
        transitive_expansion AS (
            SELECT
                ce.company_id,
                ce.statement,
                ce.concept_expand as concept,
                pce.parent_concept_expand as parent_concept,
                ce.concept as concept_source,
                ce.parent_concept as parent_concept_source
            FROM concept_expansion ce
            JOIN parent_concept_expansion pce
            ON
                ce.company_id = pce.company_id
                AND ce.statement = pce.statement
                AND ce.concept = pce.concept
        )

        SELECT company_id, statement, concept_expand as concept, parent_concept, concept as concept_source, parent_concept as parent_concept_source FROM concept_expansion
        UNION
        SELECT company_id, statement, concept, parent_concept_expand as parent_concept, concept as concept_source, parent_concept as parent_concept_source FROM parent_concept_expansion
        UNION
        SELECT company_id, statement, concept, parent_concept, concept_source, parent_concept_source FROM transitive_expansion
        """
    )


def downgrade() -> None:
    # Drop the views
    op.execute("DROP VIEW IF EXISTS parent_normalization_expansion")
    op.execute("DROP VIEW IF EXISTS concept_normalization")
    op.execute("DROP VIEW IF EXISTS concept_normalization_combined")
    op.execute("DROP VIEW IF EXISTS concept_normalization_chaining")
    op.execute("DROP VIEW IF EXISTS concept_normalization_grouping")
