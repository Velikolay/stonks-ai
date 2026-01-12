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
        group_overrides AS (
          SELECT
            cn.group_id,
            MAX(cno.normalized_label) as normalized_label,
            MAX(cno.weight) as weight,
            MAX(cno.unit) as unit
          FROM combined cn
          JOIN concept_normalization_overrides cno
          ON cn.statement = cno.statement
          AND cn.concept = cno.concept
          GROUP BY cn.group_id
        )

        SELECT
          cn.company_id,
          cn.statement,
          cn.concept,
          COALESCE(go.normalized_label, cn.normalized_label) as normalized_label,
          -- COALESCE(go.weight, cn.weight) as weight,
          go.weight as weight,
          -- COALESCE(go.unit, cn.unit) as unit,
          go.unit as unit,
          cn.group_id,
          go.normalized_label IS NOT NULL as overridden
        FROM combined cn
        LEFT JOIN group_overrides go
        ON cn.group_id = go.group_id
        """
    )

    op.execute(
        """
        CREATE VIEW parent_normalization_expansion AS

        WITH concept_normalization_by_filing AS (
          SELECT
            cn.company_id,
            ff.filing_id,
            cn.statement,
            cn.concept,
            cn.group_id
          FROM concept_normalization cn
          JOIN financial_facts ff
            ON cn.company_id = ff.company_id
            AND cn.statement = ff.statement
            AND cn.concept = ff.concept
        ),
        concept_expansion AS (
            SELECT
                cne.company_id,
                cne.filing_id,
                cne.statement,
                cno.concept,
                cno.parent_concept,
                cne.concept as concept_expand
            FROM concept_normalization_overrides cno
            JOIN concept_normalization_by_filing cn
            ON
                cno.statement = cn.statement
                AND cno.concept = cn.concept
            JOIN concept_normalization_by_filing cne
            ON
                cn.group_id = cne.group_id
            WHERE
                cno.parent_concept IS NOT NULL
        ),
        parent_concept_expansion AS (
            SELECT
                cne.company_id,
                cne.filing_id,
                cne.statement,
                cno.concept,
                cno.parent_concept,
                cne.concept as parent_concept_expand
            FROM concept_normalization_overrides cno
            JOIN concept_normalization_by_filing cn
            ON
                cno.statement = cn.statement
                AND cno.parent_concept = cn.concept
            JOIN concept_normalization_by_filing cne
            ON
                cn.group_id = cne.group_id
            WHERE
                cno.parent_concept IS NOT NULL
        ),
        transitive_expansion AS (
            SELECT
                ce.company_id,
                ce.filing_id,
                ce.statement,
                ce.concept_expand as concept,
                pce.parent_concept_expand as parent_concept,
                ce.concept as concept_source,
                ce.parent_concept as parent_concept_source
            FROM concept_expansion ce
            JOIN parent_concept_expansion pce
            ON
                ce.company_id = pce.company_id
                AND ce.filing_id = pce.filing_id
                AND ce.statement = pce.statement
                AND ce.concept = pce.concept
        )

        SELECT company_id, filing_id, statement, concept_expand as concept, parent_concept, concept as concept_source, parent_concept as parent_concept_source FROM concept_expansion
        UNION
        SELECT company_id, filing_id, statement, concept, parent_concept_expand as parent_concept, concept as concept_source, parent_concept as parent_concept_source FROM parent_concept_expansion
        UNION
        SELECT company_id, filing_id, statement, concept, parent_concept, concept_source, parent_concept_source FROM transitive_expansion
        """
    )


def downgrade() -> None:
    # Drop the views
    op.execute("DROP VIEW IF EXISTS parent_normalization_expansion")
    op.execute("DROP VIEW IF EXISTS concept_normalization")
    op.execute("DROP VIEW IF EXISTS concept_normalization_combined")
    op.execute("DROP VIEW IF EXISTS concept_normalization_chaining")
    op.execute("DROP VIEW IF EXISTS concept_normalization_grouping")
