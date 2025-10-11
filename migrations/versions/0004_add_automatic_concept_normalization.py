"""Add automatic concept normalization view

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
            f.company_id,
            ff.*
          FROM financial_facts ff
            JOIN filings f
            ON ff.filing_id = f.id
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
            AND f1.comparative_value = f2.value
            AND f1.comparative_period_end = f2.period_end
          WHERE
            f1.label != f2.label
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
            f.company_id,
            ff.*
          FROM financial_facts ff
          JOIN filings f
          ON ff.filing_id = f.id
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


def downgrade() -> None:
    # Drop the view
    op.execute("DROP VIEW IF EXISTS concept_normalization_chaining")
    op.execute("DROP VIEW IF EXISTS concept_normalization_grouping")
