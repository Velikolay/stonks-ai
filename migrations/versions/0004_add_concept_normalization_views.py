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
            ff.*,
            ff.value * ff.weight as normalized_value,
            ff.comparative_value * ff.weight as normalized_comparative_value
          FROM financial_facts ff
          WHERE
            ff.axis = ''
        ),

        candidate_matches AS (
          SELECT DISTINCT ON (f1.company_id, f1.statement, f1.concept, f1.concept, f1.period_end, f2.period_end)
            f1.company_id,
            f1.statement,
            f1.concept as concept1,
            f2.concept as concept2,
            f1.period_end as period_end1,
            f2.period_end as period_end2,
            f1.label as label1,
            f2.label as label2
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
          ORDER BY f1.company_id, f1.statement, f1.concept, f1.concept, f1.period_end, f2.period_end
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
        FROM financial_facts
        WHERE
            axis = ''
        GROUP BY
            company_id,
            statement,
            concept
        HAVING COUNT(DISTINCT label) > 1
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
                normalized_label,
                group_id,
                group_max_period_end
            FROM concept_normalization_chaining

            UNION ALL

            SELECT
                company_id,
                statement,
                concept,
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

        WITH group_overrides AS (
          SELECT
            cn.group_id,
            MAX(cno.normalized_label) as normalized_label,
            MAX(cno.weight) as weight,
            MAX(cno.unit) as unit
          FROM concept_normalization_combined cn
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
        FROM concept_normalization_combined cn
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

    op.execute(
        """
        CREATE VIEW normalized_financial_facts AS

        WITH RECURSIVE parent_normalization_expansion_cte AS (
            SELECT * FROM parent_normalization_expansion
        ),
        normalized_facts AS (
            /* -------------------------------------------------
            * Anchor: existing fact rows (preserve identity)
            * ------------------------------------------------- */
            SELECT
                ff.id,
                ff.company_id,
                ff.filing_id,
                ff.form_type,
                ff.concept,
                ff.label,
                COALESCE(cno.normalized_label, cn.normalized_label, ff.label) as normalized_label,
                ff.is_abstract,
                CASE
                    WHEN ff.weight * COALESCE(cno.weight, cn.weight, ff.weight) < 0
                    THEN -ff.value
                    ELSE ff.value
                END as value,
                CASE
                    WHEN ff.weight * COALESCE(cno.weight, cn.weight, ff.weight) < 0
                    THEN -ff.comparative_value
                    ELSE ff.comparative_value
                END as comparative_value,
                COALESCE(cno.weight, cn.weight, ff.weight) as weight,
                COALESCE(cno.unit, cn.unit, ff.unit) as unit,
                ff.axis,
                ff.member,
                ff.parsed_axis,
                ff.parsed_member,
                ff.statement,
                ff.period_end,
                ff.comparative_period_end,
                ff.period,
                ff.position,

                CASE
                    WHEN COALESCE(cno.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NOT NULL THEN
                        ffp.id
                    WHEN COALESCE(cno.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                        abs(hashtextextended(
                            ff.statement || '|' || COALESCE(cno.parent_concept, pne.parent_concept_source) || '|' ||
                            ff.filing_id::text || '|' || ff.company_id::text,
                            0
                        ))
                    ELSE
                        ff.parent_id
                END as parent_id,
                CASE
                    WHEN COALESCE(cno.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    COALESCE(cno.parent_concept, pne.parent_concept_source)
                ELSE
                    NULL
                END as parent_concept,

                CASE
                    WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NOT NULL THEN
                        ffa.id
                    WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                        abs(hashtextextended(
                            ff.statement || '|' || cno.abstract_concept || '|' ||
                            ff.filing_id::text || '|' || ff.company_id::text,
                            0
                        ))
                    ELSE
                        ff.abstract_id
                END as abstract_id,
                CASE
                    WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                        cno.abstract_concept
                    ELSE
                        NULL
                END as abstract_concept,

                FALSE as is_synthetic
            FROM financial_facts ff
            LEFT JOIN concept_normalization_overrides cno
            ON
                ff.statement = cno.statement
                AND ff.concept = cno.concept

            LEFT JOIN parent_normalization_expansion_cte pne
            ON
                pne.company_id = ff.company_id
                AND pne.filing_id  = ff.filing_id
                AND pne.statement  = ff.statement
                AND pne.concept    = ff.concept

            LEFT JOIN financial_facts ffp
            ON
                ffp.company_id = ff.company_id
                AND ffp.filing_id  = ff.filing_id
                AND ffp.statement  = ff.statement
                AND ffp.concept    = COALESCE(cno.parent_concept, pne.parent_concept)

            LEFT JOIN financial_facts ffa
            ON
                ffa.company_id = ff.company_id
                AND ffa.filing_id  = ff.filing_id
                AND ffa.statement  = ff.statement
                AND ffa.concept    = cno.abstract_concept

            LEFT JOIN concept_normalization cn
            ON
                ff.company_id = cn.company_id
                AND ff.statement = cn.statement
                AND ff.concept = cn.concept

            UNION

            /* -------------------------------
            * Add missing parent / abstract concept
            * ------------------------------- */
            SELECT
                new.id AS id,
                f.company_id,
                f.filing_id,
                f.form_type,
                cno.concept,
                cno.normalized_label AS label,
                cno.normalized_label AS normalized_label,
                cno.is_abstract,
                0 AS value,
                0 AS comparative_value,
                cno.weight,
                cno.unit,
                '' AS axis,
                '' AS member,
                '' AS parsed_axis,
                '' AS parsed_member,
                cno.statement,
                f.period_end,
                f.comparative_period_end,
                f.period,
                99 AS position,

                CASE
                    WHEN COALESCE(cno.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NOT NULL THEN
                        ffp.id
                    WHEN COALESCE(cno.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                        abs(hashtextextended(
                            f.statement || '|' || COALESCE(cno.parent_concept, pne.parent_concept_source) || '|' ||
                            f.filing_id::text || '|' || f.company_id::text,
                            0
                        ))
                    ELSE
                        NULL
                END as parent_id,
                CASE
                    WHEN COALESCE(cno.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    COALESCE(cno.parent_concept, pne.parent_concept_source)
                ELSE
                    NULL
                END as parent_concept,

                CASE
                    WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NOT NULL THEN
                        ffa.id
                    WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                        abs(hashtextextended(
                            f.statement || '|' || cno.abstract_concept || '|' ||
                            f.filing_id::text || '|' || f.company_id::text,
                            0
                        ))
                    ELSE
                        NULL
                END as abstract_id,
                CASE
                    WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                        cno.abstract_concept
                    ELSE
                        NULL
                END as abstract_concept,

                TRUE AS is_synthetic
            FROM normalized_facts f

            JOIN LATERAL (
                VALUES
                    (f.parent_concept, f.parent_id),
                    (f.abstract_concept, f.abstract_id)
            ) AS new(concept, id)
                ON new.concept IS NOT NULL

            JOIN concept_normalization_overrides cno
                ON cno.statement = f.statement
                AND cno.concept = new.concept

            LEFT JOIN parent_normalization_expansion_cte pne
                ON pne.company_id = f.company_id
                AND pne.filing_id  = f.filing_id
                AND pne.statement  = f.statement
                AND pne.concept    = new.concept

            LEFT JOIN financial_facts ffp
                ON ffp.company_id = f.company_id
                AND ffp.filing_id  = f.filing_id
                AND ffp.statement  = f.statement
                AND ffp.concept    = COALESCE(cno.parent_concept, pne.parent_concept)

            LEFT JOIN financial_facts ffa
                ON ffa.company_id = f.company_id
                AND ffa.filing_id  = f.filing_id
                AND ffa.statement  = f.statement
                AND ffa.concept    = cno.abstract_concept
        ),
        synthetic_rollup AS (
            /* ---------------------------------------
            * 1. Base: real nodes contribute their value
            * --------------------------------------- */
            SELECT
                nf.id,
                nf.parent_id,
                nf.value * nf.weight AS contrib_value,
                nf.comparative_value * nf.weight AS contrib_comparative_value
            FROM normalized_facts nf
            WHERE
                NOT nf.is_abstract
                AND NOT nf.is_synthetic

            UNION ALL

            /* ---------------------------------------
            * 2. Push values upward
            * --------------------------------------- */
            SELECT
                nf.id,
                nf.parent_id,
                sr.contrib_value * nf.weight AS contrib_value,
                sr.contrib_comparative_value * nf.weight AS contrib_comparative_value
            FROM synthetic_rollup sr
            JOIN normalized_facts nf
                ON nf.id = sr.parent_id
            WHERE
                NOT nf.is_abstract
                AND nf.is_synthetic
        )

        SELECT
            nf.id,
            nf.company_id,
            nf.filing_id,
            nf.form_type,
            nf.concept,
            nf.label,
            nf.normalized_label,
            nf.is_abstract,
            CASE
                WHEN nf.is_synthetic THEN
                    SUM(sr.contrib_value)
                ELSE nf.value
            END AS value,
            CASE
                WHEN nf.is_synthetic THEN
                    SUM(sr.contrib_comparative_value)
                ELSE nf.comparative_value
            END AS comparative_value,
            nf.weight,
            nf.unit,
            nf.axis,
            nf.member,
            nf.parsed_axis,
            nf.parsed_member,
            nf.statement,
            nf.period_end,
            nf.comparative_period_end,
            nf.period,
            nf.position,
            nf.parent_id,
            nf.abstract_id,
            nf.is_synthetic
        FROM normalized_facts nf
        LEFT JOIN synthetic_rollup sr
            ON sr.id = nf.id
        GROUP BY
            nf.id,
            nf.company_id,
            nf.filing_id,
            nf.form_type,
            nf.concept,
            nf.label,
            nf.normalized_label,
            nf.is_abstract,
            nf.value,
            nf.comparative_value,
            nf.weight,
            nf.unit,
            nf.axis,
            nf.member,
            nf.parsed_axis,
            nf.parsed_member,
            nf.statement,
            nf.period_end,
            nf.comparative_period_end,
            nf.period,
            nf.position,
            nf.parent_id,
            nf.abstract_id,
            nf.is_synthetic
        """
    )

    # Create unique index on normalized_financial_facts to detect duplicates and enable concurrent refresh
    # op.execute(
    #     """
    #     CREATE UNIQUE INDEX IF NOT EXISTS idx_normalized_financial_facts_unique_id
    #     ON normalized_financial_facts (id);
    # """
    # )

    # op.execute(
    #     """
    #     CREATE UNIQUE INDEX IF NOT EXISTS idx_normalized_financial_facts_unique_composite
    #     ON normalized_financial_facts (
    #         company_id,
    #         filing_id,
    #         statement,
    #         concept,
    #         normalized_label,
    #         axis,
    #         member
    #     );
    # """
    # )


def downgrade() -> None:
    # Drop the unique index
    # op.execute("DROP INDEX IF EXISTS idx_normalized_financial_facts_unique_composite")
    # op.execute("DROP INDEX IF EXISTS idx_normalized_financial_facts_unique_id")

    # Drop the views
    op.execute("DROP VIEW IF EXISTS normalized_financial_facts")
    op.execute("DROP VIEW IF EXISTS parent_normalization_expansion")
    op.execute("DROP VIEW IF EXISTS concept_normalization")
    op.execute("DROP VIEW IF EXISTS concept_normalization_combined")
    op.execute("DROP VIEW IF EXISTS concept_normalization_chaining")
    op.execute("DROP VIEW IF EXISTS concept_normalization_grouping")
