"""Add concept normalization views

Revision ID: 0004
Revises: 0003
Create Date: 2024-12-20 12:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create view for chaining concept normalization
    op.execute(
        """
        CREATE VIEW concept_normalization_chaining AS

        WITH RECURSIVE facts AS (
          SELECT
            c.id as company_id,
            ff.*,
            ff.value * ff.weight as normalized_value,
            ff.comparative_value * ff.weight as normalized_comparative_value
          FROM financial_facts ff
          JOIN filings f
          ON ff.filing_id = f.id
          JOIN companies c
          ON f.company_id = c.id
          WHERE f.form_type = '10-K'
        ),

        candidate_matches AS (
          SELECT DISTINCT
            f1.company_id,
            f1.statement,
            f1.label as label1,
            f2.label as label2,
            f1.concept as concept1,
            f2.concept as concept2,
            f1.period_end as period_end1,
            f2.period_end as period_end2
          FROM facts f1
          JOIN facts f2
          ON
            f1.company_id = f2.company_id
            AND f1.statement = f2.statement
            AND f1.normalized_comparative_value = f2.normalized_value
            AND f1.comparative_period_end = f2.period_end
          WHERE
            (f1.concept <> f2.concept OR f1.label <> f2.label)
            AND f1.period_end > f2.period_end
        ),

        overlapping_matches AS (
          SELECT DISTINCT
            a.company_id,
            a.statement,
            a.label1,
            a.label2,
            a.concept1,
            a.concept2
          FROM candidate_matches a
          JOIN candidate_matches b
            ON a.company_id = b.company_id
            AND a.statement = b.statement
            AND a.label1 = b.label1
            AND a.label2 = b.label2
            AND a.concept1 = b.concept1
            AND a.concept2 = b.concept2
            AND (
              a.period_end1 = b.period_end2
              OR a.period_end2 = b.period_end1
            )
        ),

        mirror_matches AS (
          SELECT DISTINCT
            a.company_id,
            a.statement,
            a.label1,
            a.label2,
            a.concept1,
            a.concept2
          FROM candidate_matches a
          JOIN candidate_matches b
            ON a.company_id = b.company_id
            AND a.statement = b.statement
            AND a.label1 = b.label2
            AND a.label2 = b.label1
            AND a.concept1 = b.concept2
            AND a.concept2 = b.concept1
            AND a.period_end1 = b.period_end1
            AND a.period_end2 = b.period_end2
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
              AND cm.label1 = fm.label1
              AND cm.label2 = fm.label2
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
          r.root_label as label,
          r.root_label as normalized_label,
          r.root_period as current_period,
          r.root_period as root_period,
          gen_random_uuid() AS group_id
        FROM roots r

        UNION ALL

        -- 3. Recursively traverse the directed edges forward in time (newer → older)
        SELECT
          c.company_id,
          c.statement,
          m.concept2 as concept,
          m.label2 as label,
          c.normalized_label,
          m.period_end2 as current_period,
          c.root_period as root_period,
          c.group_id as group_id
        FROM chain c
        JOIN matches m
          ON m.company_id = c.company_id
          AND m.statement = c.statement
          AND m.concept1 = c.concept
          AND m.label1 = c.label
          AND m.period_end1 <= c.current_period
        )

        -- 4. Collapse duplicates (same concept can appear in multiple roots, take the latest one)
        SELECT DISTINCT ON (company_id, statement, concept, label)
          company_id,
          statement,
          concept,
          label,
          normalized_label,
          group_id,
          root_period as group_max_period_end
        FROM chain
        ORDER BY company_id, statement, concept, label, root_period DESC
        """
    )

    # Create view for grouping concept normalization
    op.execute(
        """
        CREATE VIEW concept_normalization_grouping AS

        WITH facts AS (
          SELECT
            c.id as company_id,
            ff.*
          FROM financial_facts ff
          JOIN filings f
          ON ff.filing_id = f.id
          JOIN companies c
          ON f.company_id = c.id
          WHERE f.form_type = '10-K'
        ),

        normalized_labels AS (
          SELECT
            company_id,
            statement,
            concept,
            (ARRAY_AGG(label ORDER BY period_end DESC))[1] AS normalized_label,
            gen_random_uuid() AS group_id,
            MAX(period_end) AS group_max_period_end
          FROM facts
          GROUP BY
            company_id,
            statement,
            concept
          HAVING COUNT(DISTINCT label) > 1
        )

        SELECT DISTINCT
          nl.company_id,
          nl.statement,
          nl.concept,
          f.label,
          nl.normalized_label,
          nl.group_id,
          nl.group_max_period_end
        FROM normalized_labels nl
        JOIN facts f
        ON
          nl.company_id = f.company_id
          AND nl.statement = f.statement
          AND nl.concept = f.concept
        """
    )

    # Create merged view
    op.execute(
        """
        CREATE VIEW concept_normalization_combined AS

        WITH RECURSIVE combined AS (
        -- Combine both views with source tracking
        SELECT
            company_id,
            statement,
            concept,
            label,
            normalized_label,
            group_id,
            group_max_period_end
        FROM concept_normalization_chaining

        UNION ALL

        SELECT
            company_id,
            statement,
            concept,
            label,
            normalized_label,
            group_id,
            group_max_period_end
        FROM concept_normalization_grouping
        ),

        -- Identify group merges: groups that share at least one concept
        group_links AS (
        SELECT DISTINCT
            c1.company_id,
            c1.statement,
            c1.concept,
            c1.group_id as group_id_1,
            c2.group_id as group_id_2,
            CASE
                WHEN c1.group_max_period_end > c2.group_max_period_end THEN c1.group_id
                WHEN c1.group_max_period_end = c2.group_max_period_end AND c1.group_id >= c2.group_id THEN c1.group_id
                ELSE c2.group_id
            END as group_id,
            CASE
                WHEN c1.group_max_period_end > c2.group_max_period_end THEN c1.normalized_label
                WHEN c1.group_max_period_end = c2.group_max_period_end AND c1.group_id >= c2.group_id THEN c1.normalized_label
                ELSE c2.normalized_label
            END as normalized_label,
            GREATEST(c1.group_max_period_end, c2.group_max_period_end) as group_max_period_end
        FROM combined c1
        JOIN combined c2
            ON c1.company_id = c2.company_id
            AND c1.statement = c2.statement
            AND c1.concept = c2.concept
            AND c1.group_id <> c2.group_id
        ),

        transitive_merges AS (
        SELECT
            company_id,
            statement,
            group_id_1,
            group_id_2,
            group_id,
            group_max_period_end,
            normalized_label
        FROM group_links

        UNION

        SELECT
            tm.company_id,
            tm.statement,
            tm.group_id_1,
            gl.group_id_2,
            CASE
                WHEN tm.group_max_period_end > gl.group_max_period_end THEN tm.group_id
                WHEN tm.group_max_period_end = gl.group_max_period_end AND tm.group_id >= gl.group_id THEN tm.group_id
                ELSE gl.group_id
            END as group_id,
            GREATEST(tm.group_max_period_end, gl.group_max_period_end) as group_max_period_end,
            CASE
                WHEN tm.group_max_period_end > gl.group_max_period_end THEN tm.normalized_label
                WHEN tm.group_max_period_end = gl.group_max_period_end AND tm.group_id >= gl.group_id THEN tm.normalized_label
                ELSE gl.normalized_label
            END as normalized_label
        FROM transitive_merges tm
        JOIN group_links gl
        ON
            tm.company_id = gl.company_id
            AND tm.statement = gl.statement
            AND tm.group_id_2 = gl.group_id_1
        WHERE tm.group_id_1 <> gl.group_id_2
        ),

        canonical_groups AS (
        SELECT DISTINCT ON (company_id, statement, group_id_1)
            company_id,
            statement,
            group_id_1 as original_group_id,
            group_id as final_group_id,
            group_max_period_end,
            normalized_label
        FROM
            transitive_merges
        ORDER BY company_id, statement, group_id_1, group_max_period_end DESC
        )

        -- Final output with all concepts
        SELECT DISTINCT
            c.company_id,
            c.statement,
            c.concept,
            c.label,
            COALESCE(cg.normalized_label, c.normalized_label) as normalized_label,
            COALESCE(cg.final_group_id, c.group_id) as group_id,
            COALESCE(cg.group_max_period_end, c.group_max_period_end) as group_max_period_end
        FROM combined c
        LEFT JOIN canonical_groups cg
        ON
            c.company_id = cg.company_id
            AND c.statement = cg.statement
            AND c.group_id = cg.original_group_id
        """
    )

    op.execute(
        """
        CREATE VIEW concept_normalization AS

        WITH concept_normalization_stable AS (
          SELECT * FROM concept_normalization_combined
        ),
        group_overrides AS (
          SELECT
            cn.group_id,
            MAX(cno.normalized_label) as normalized_label
          FROM concept_normalization_stable cn
          JOIN concept_normalization_overrides cno
          ON cn.statement = cno.statement
          AND cn.concept = cno.concept
          GROUP BY cn.group_id
        )

        SELECT
          cn.company_id,
          cn.statement,
          cn.concept,
          cn.label,
          COALESCE(go.normalized_label, cn.normalized_label) as normalized_label,
          cn.group_id,
          go.normalized_label IS NOT NULL as overridden
        FROM concept_normalization_stable cn
        LEFT JOIN group_overrides go
        ON cn.group_id = go.group_id
        """
    )

    op.execute(
        """
        CREATE VIEW abstract_normalization_overrides AS

        WITH RECURSIVE abstract_normalization_overrides_cte AS (
            SELECT
                statement,
                concept,
                is_abstract,
                ARRAY[normalized_label] AS path,
                ARRAY[concept] AS concept_path
            FROM concept_normalization_overrides
            -- start from a top level abstracts and work down
            WHERE
                is_abstract = TRUE
                AND parent_concept IS NULL

            UNION ALL

            SELECT
                cno.statement,
                cno.concept,
                cno.is_abstract,
                CASE
                    WHEN cno.is_abstract THEN ano.path || cno.normalized_label
                    ELSE ano.path
                END AS path,
                CASE
                    WHEN cno.is_abstract THEN ano.concept_path || cno.concept
                    ELSE ano.concept_path
                END AS concept_path
            FROM concept_normalization_overrides cno
            JOIN abstract_normalization_overrides_cte ano
            ON
                cno.parent_concept = ano.concept
                AND cno.statement = ano.statement
        )
        SELECT * FROM abstract_normalization_overrides_cte
        """
    )

    op.execute(
        """
        CREATE VIEW abstract_normalization AS

        WITH RECURSIVE abstract_normalization_cte AS (
            SELECT
                ff.id,
                ff.filing_id,
                ff.statement,
                COALESCE(ano.path, ARRAY[ff.label]) AS path,
                COALESCE(ano.concept_path, ARRAY[ff.concept]) AS concept_path
            FROM financial_facts ff
            LEFT JOIN abstract_normalization_overrides ano
            ON
                ff.concept = ano.concept
                AND ff.statement = ano.statement
            -- start from a top level abstracts and work down
            WHERE
                ff.is_abstract = TRUE
                AND ff.parent_id IS NULL

            UNION ALL

            SELECT
                ff.id,
                ff.filing_id,
                ff.statement,
                CASE
                    WHEN ano.path IS NULL THEN a.path || ARRAY[ff.label]
                    ELSE ano.path
                END AS path,
                CASE
                    WHEN ano.concept_path IS NULL THEN a.concept_path || ARRAY[ff.concept]
                    ELSE ano.concept_path
                END AS concept_path
            FROM financial_facts ff
            LEFT JOIN abstract_normalization_overrides ano
            ON
                ff.concept = ano.concept
                AND ff.statement = ano.statement
            JOIN abstract_normalization_cte a
            ON
                a.id = ff.parent_id
                AND a.filing_id = ff.filing_id
            WHERE
                ff.is_abstract = TRUE
        )
        SELECT * FROM abstract_normalization_cte
        """
    )


def downgrade() -> None:
    # Drop the views
    op.execute("DROP VIEW IF EXISTS abstract_normalization")
    op.execute("DROP VIEW IF EXISTS abstract_normalization_overrides")
    op.execute("DROP VIEW IF EXISTS concept_normalization")
    op.execute("DROP VIEW IF EXISTS concept_normalization_combined")
    op.execute("DROP VIEW IF EXISTS concept_normalization_chaining")
    op.execute("DROP VIEW IF EXISTS concept_normalization_grouping")
