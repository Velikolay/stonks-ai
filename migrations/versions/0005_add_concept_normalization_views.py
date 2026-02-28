"""Add concept normalization views

Revision ID: 0005
Revises: 0004
Create Date: 2024-12-20 12:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:

    op.execute(
        """
        CREATE VIEW concept_normalization_overridden_facts AS

        SELECT
            ff.id,
            ff.company_id,
            ff.filing_id,
            ff.statement,
            ff.form_type,

            ff.weight,
            ff.value,
            ff.period_end,

            ff.comparative_value,
            ff.comparative_period_end,

            ff.concept,
            ff.label,

            COALESCE(r.normalized_concept, ff.concept) AS normalized_concept,
            COALESCE(r.normalized_label, ff.label) AS normalized_label,

            (r.concept IS NOT NULL) AS overridden
        FROM financial_facts ff
        LEFT JOIN LATERAL (
            SELECT r.*
            FROM concept_normalization_overrides r
            WHERE
                r.statement = ff.statement
                AND r.concept = ff.concept
                AND (r.company_id = ff.company_id OR r.is_global = TRUE)
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
                    (r.label IS NOT NULL)::int
                    + (r.form_type IS NOT NULL)::int
                    + (r.from_period IS NOT NULL)::int
                    + (r.to_period IS NOT NULL)::int
                ) DESC,
                r.updated_at DESC
            LIMIT 1
        ) r ON TRUE
        WHERE
            ff.axis = ''
        """
    )

    # Create view for grouping concept normalization
    op.execute(
        """
        CREATE VIEW concept_normalization_grouping AS

        WITH base AS (
            SELECT * FROM concept_normalization_overridden_facts
        ),
        groups AS (
            SELECT
                company_id,
                statement,
                normalized_concept,
                (ARRAY_AGG(normalized_label ORDER BY period_end DESC))[1]
                    AS group_normalized_label,
                md5(company_id || '|' || statement || '|' || normalized_concept || '|' || 'grouping')
                    AS group_id,
                MAX(period_end) AS group_max_period_end
            FROM base
            GROUP BY
                company_id,
                statement,
                normalized_concept
            HAVING
                COUNT(DISTINCT normalized_label) > 1
                -- filters concepts that appear more than once in the same filing / statement
                AND COUNT(DISTINCT (filing_id, normalized_label)) = COUNT(DISTINCT filing_id)
        )
        SELECT
            b.id,
            b.company_id,
            b.filing_id,
            b.statement,
            b.form_type,
            b.period_end,
            b.concept,
            b.label,
            b.normalized_concept,
            CASE
                WHEN b.overridden THEN b.normalized_label
                ELSE g.group_normalized_label
            END AS normalized_label,
            g.group_id,
            g.group_max_period_end,
            b.overridden
        FROM base b
        JOIN groups g
            ON b.company_id = g.company_id
            AND b.statement = g.statement
            AND b.normalized_concept = g.normalized_concept
        """
    )

    # Create view for chaining concept normalization
    op.execute(
        """
        CREATE VIEW concept_normalization_chaining AS

        WITH RECURSIVE facts AS (
          SELECT
            ff.id,
            ff.company_id,
            ff.filing_id,
            ff.statement,
            ff.form_type,
            ff.period_end,
            ff.comparative_period_end,
            ff.concept,
            ff.label,
            ff.normalized_concept,
            ff.overridden,
            -- apply grouping normalization so it is part of the chaining algorithm
            -- this avoids complex combining logic later on
            COALESCE(
              ff.normalized_label,
              cng.normalized_label,
              ff.label
            ) as normalized_label,
            ff.value * ff.weight as normalized_value,
            ff.comparative_value * ff.weight as normalized_comparative_value
          FROM concept_normalization_overridden_facts ff
          LEFT JOIN concept_normalization_grouping cng
            ON ff.id = cng.id
        ),

        candidate_matches AS (
          SELECT DISTINCT ON (
            f1.company_id,
            f1.statement,
            f1.normalized_concept,
            f2.normalized_concept,
            f1.period_end,
            f2.period_end
          )
            f1.company_id,
            f1.statement,
            f1.normalized_concept as concept1,
            f2.normalized_concept as concept2,
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
            f1.normalized_concept <> f2.normalized_concept
            AND f1.period_end > f2.period_end
            AND NOT EXISTS (
                SELECT 1
                FROM facts fx
                WHERE fx.company_id = f1.company_id
                    AND fx.statement = f1.statement
                    AND fx.period_end = f1.period_end
                    AND fx.normalized_concept = f2.normalized_concept
            )
            AND NOT EXISTS (
                SELECT 1
                FROM facts fx
                WHERE fx.company_id = f2.company_id
                    AND fx.statement = f2.statement
                    AND fx.period_end = f2.period_end
                    AND fx.normalized_concept = f1.normalized_concept
            )
          ORDER BY
            f1.company_id,
            f1.statement,
            f1.normalized_concept,
            f2.normalized_concept,
            f1.period_end,
            f2.period_end
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
        ),

        chain_map AS (
          SELECT DISTINCT ON (company_id, statement, concept)
            company_id,
            statement,
            concept,
            normalized_label,
            group_id,
            root_period as group_max_period_end
          FROM chain
          ORDER BY company_id, statement, concept, root_period DESC
        )

        SELECT
          f.id,
          f.company_id,
          f.filing_id,
          f.statement,
          f.form_type,
          f.period_end,
          f.concept,
          f.label,
          f.normalized_concept,
          CASE
            WHEN f.overridden THEN f.normalized_label
            ELSE cm.normalized_label
          END AS normalized_label,
          cm.group_id,
          cm.group_max_period_end,
          f.overridden
        FROM facts f
        JOIN chain_map cm
          ON cm.company_id = f.company_id
          AND cm.statement = f.statement
          AND cm.concept = f.normalized_concept
          AND f.period_end <= cm.group_max_period_end
        """
    )

    # Create merged view
    op.execute(
        """
        CREATE VIEW concept_normalization AS

        WITH unioned AS (
            -- chaining
            SELECT
                t.id,
                t.company_id,
                t.filing_id,
                t.statement,
                t.form_type,
                t.period_end,
                t.concept,
                t.label,
                t.normalized_concept,
                t.normalized_label,
                t.group_id,
                t.group_max_period_end,
                t.overridden,
                0 AS src_priority
            FROM concept_normalization_chaining t

            UNION ALL

            -- next: grouping
            SELECT
                t.id,
                t.company_id,
                t.filing_id,
                t.statement,
                t.form_type,
                t.period_end,
                t.concept,
                t.label,
                t.normalized_concept,
                t.normalized_label,
                t.group_id,
                t.group_max_period_end,
                t.overridden,
                1 AS src_priority
            FROM concept_normalization_grouping t

            UNION ALL

            -- last: explicit overrides
            SELECT
                ofx.id,
                ofx.company_id,
                ofx.filing_id,
                ofx.statement,
                ofx.form_type,
                ofx.period_end,
                ofx.concept,
                ofx.label,
                ofx.normalized_concept,
                ofx.normalized_label,
                md5(
                    ofx.company_id
                    || '|'
                    || ofx.statement
                    || '|'
                    || ofx.normalized_concept
                    || '|'
                    || 'override'
                ) AS group_id,
                MAX(ofx.period_end) OVER (
                    PARTITION BY
                        ofx.company_id,
                        ofx.statement,
                        ofx.normalized_concept
                ) AS group_max_period_end,
                TRUE AS overridden,
                2 AS src_priority
            FROM concept_normalization_overridden_facts ofx
            WHERE ofx.overridden = TRUE
        )
        SELECT DISTINCT ON (id)
            id,
            company_id,
            filing_id,
            statement,
            form_type,
            period_end,
            concept,
            label,
            normalized_concept,
            normalized_label,
            group_id,
            group_max_period_end,
            overridden,
            src_priority
        FROM unioned
        ORDER BY
            id,
            src_priority,
            group_max_period_end DESC
        """
    )

    op.execute(
        """
        CREATE VIEW parent_normalization_expansion AS

        WITH concept_normalization_cte AS (
          SELECT DISTINCT ON (company_id, statement, normalized_concept)
            company_id,
            statement,
            normalized_concept AS concept,
            group_id
          FROM concept_normalization
          ORDER BY
            company_id,
            statement,
            normalized_concept,
            overridden DESC,
            src_priority,
            group_max_period_end DESC
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
                    concept_hierarchy o
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
                    concept_hierarchy o
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
    op.execute("DROP VIEW IF EXISTS concept_normalization_chaining")
    op.execute("DROP VIEW IF EXISTS concept_normalization_grouping")
