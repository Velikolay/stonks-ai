"""Add fact normalization views

Revision ID: 0007
Revises: 0006
Create Date: 2024-12-20 12:00:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:

    op.execute(
        """
        CREATE VIEW financial_facts_normalized AS

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
                COALESCE(cn.normalized_label, ff.label) as normalized_label,
                ff.is_abstract,
                CASE
                    WHEN ff.weight * COALESCE(ch.weight, ff.weight) < 0
                    THEN -ff.value
                    ELSE ff.value
                END as value,
                CASE
                    WHEN ff.weight * COALESCE(ch.weight, ff.weight) < 0
                    THEN -ff.comparative_value
                    ELSE ff.comparative_value
                END as comparative_value,
                COALESCE(ch.weight, ff.weight) as weight,
                ff.unit as unit,
                COALESCE(dn.normalized_axis_label, ff.axis) as axis,
                COALESCE(dn.normalized_member_label, ff.member_label) as member,
                ff.statement,
                ff.period_end,
                ff.comparative_period_end,
                ff.period,
                ff.position,

                CASE
                    WHEN COALESCE(ch.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NOT NULL THEN
                        ffp.id
                    WHEN COALESCE(ch.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                        abs(hashtextextended(
                            ff.statement || '|' || COALESCE(ch.parent_concept, pne.parent_concept_source) || '|' ||
                            ff.filing_id::text || '|' || ff.company_id::text,
                            0
                        ))
                    ELSE
                        ff.parent_id
                END as parent_id,
                CASE
                    WHEN COALESCE(ch.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    COALESCE(ch.parent_concept, pne.parent_concept_source)
                ELSE
                    NULL
                END as parent_concept,

                CASE
                    WHEN ch.abstract_concept IS NOT NULL AND ffa.id IS NOT NULL THEN
                        ffa.id
                    WHEN ch.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                        abs(hashtextextended(
                            ff.statement || '|' || ch.abstract_concept || '|' ||
                            ff.filing_id::text || '|' || ff.company_id::text,
                            0
                        ))
                    ELSE
                        ff.abstract_id
                END as abstract_id,
                CASE
                    WHEN ch.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                        ch.abstract_concept
                    ELSE
                        NULL
                END as abstract_concept,

                FALSE as is_synthetic
            FROM financial_facts ff

            LEFT JOIN concept_normalization cn
            ON ff.id = cn.id

            LEFT JOIN LATERAL (
                SELECT
                    h.weight,
                    h.parent_concept,
                    h.abstract_concept
                FROM
                    concept_hierarchy h
                WHERE
                    h.statement = ff.statement
                    AND h.concept = COALESCE(cn.normalized_concept, ff.concept)
                    AND (
                        h.company_id = ff.company_id
                        OR h.is_global = TRUE
                    )
                ORDER BY
                    (h.company_id = ff.company_id) DESC
                LIMIT 1
            ) ch ON TRUE

            LEFT JOIN parent_normalization_expansion_cte pne
            ON
                pne.company_id = ff.company_id
                AND pne.statement  = ff.statement
                AND pne.concept    = COALESCE(cn.normalized_concept, ff.concept)

            LEFT JOIN financial_facts ffp
            ON
                ffp.company_id = ff.company_id
                AND ffp.filing_id  = ff.filing_id
                AND ffp.statement  = ff.statement
                AND ffp.concept    = COALESCE(ch.parent_concept, pne.parent_concept)

            LEFT JOIN financial_facts ffa
            ON
                ffa.company_id = ff.company_id
                AND ffa.filing_id  = ff.filing_id
                AND ffa.statement  = ff.statement
                AND ffa.concept    = ch.abstract_concept

            LEFT JOIN dimension_normalization dn
            ON
                ff.company_id = dn.company_id
                AND ff.axis = dn.axis
                AND ff.member = dn.member
                AND ff.member_label = dn.member_label

            UNION

            /* -------------------------------
            * Add missing parent / abstract concept
            * ------------------------------- */
            SELECT
                new.id AS id,
                f.company_id,
                f.filing_id,
                f.form_type,
                new.concept as concept,
                cno.normalized_label as label,
                cno.normalized_label as normalized_label,
                (new.concept LIKE '%Abstract') as is_abstract,
                0 AS value,
                0 AS comparative_value,
                COALESCE(ch.weight, 1) as weight,
                f.unit as unit,
                '' AS axis,
                '' AS member,
                f.statement,
                f.period_end,
                f.comparative_period_end,
                f.period,
                99 AS position,

                CASE
                    WHEN COALESCE(ch.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NOT NULL THEN
                        ffp.id
                    WHEN COALESCE(ch.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                        abs(hashtextextended(
                            f.statement || '|' || COALESCE(ch.parent_concept, pne.parent_concept_source) || '|' ||
                            f.filing_id::text || '|' || f.company_id::text,
                            0
                        ))
                    ELSE
                        NULL
                END as parent_id,
                CASE
                    WHEN COALESCE(ch.parent_concept, pne.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    COALESCE(ch.parent_concept, pne.parent_concept_source)
                ELSE
                    NULL
                END as parent_concept,

                CASE
                    WHEN ch.abstract_concept IS NOT NULL AND ffa.id IS NOT NULL THEN
                        ffa.id
                    WHEN ch.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                        abs(hashtextextended(
                            f.statement || '|' || ch.abstract_concept || '|' ||
                            f.filing_id::text || '|' || f.company_id::text,
                            0
                        ))
                    ELSE
                        NULL
                END as abstract_id,
                CASE
                    WHEN ch.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                        ch.abstract_concept
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

            JOIN LATERAL (
                SELECT
                    h.weight,
                    h.parent_concept,
                    h.abstract_concept
                FROM
                    concept_hierarchy h
                WHERE
                    h.statement = f.statement
                    AND h.concept = new.concept
                    AND (
                        h.company_id = f.company_id
                        OR h.is_global = TRUE
                    )
                ORDER BY
                    (h.company_id = f.company_id) DESC
                LIMIT 1
            ) ch ON TRUE

            JOIN LATERAL (
                SELECT
                    r.normalized_label
                FROM concept_normalization_overrides r
                WHERE
                    r.statement = f.statement
                    AND r.concept = new.concept
                    AND (r.company_id = f.company_id OR r.is_global = TRUE)
                    AND r.label IS NULL
                    AND r.form_type IS NULL
                    AND r.from_period IS NULL
                    AND r.to_period IS NULL
                ORDER BY
                    (r.company_id = f.company_id) DESC,
                    r.updated_at DESC
                LIMIT 1
            ) cno ON TRUE

            LEFT JOIN parent_normalization_expansion_cte pne
                ON pne.company_id = f.company_id
                AND pne.statement  = f.statement
                AND pne.concept    = new.concept

            LEFT JOIN financial_facts ffp
                ON ffp.company_id = f.company_id
                AND ffp.filing_id  = f.filing_id
                AND ffp.statement  = f.statement
                AND ffp.concept    = COALESCE(ch.parent_concept, pne.parent_concept)

            LEFT JOIN financial_facts ffa
                ON ffa.company_id = f.company_id
                AND ffa.filing_id  = f.filing_id
                AND ffa.statement  = f.statement
                AND ffa.concept    = ch.abstract_concept
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
                sr.contrib_value * COALESCE(nf.weight, 1) AS contrib_value,
                sr.contrib_comparative_value * COALESCE(nf.weight, 1) AS contrib_comparative_value
            FROM synthetic_rollup sr
            JOIN normalized_facts nf
                ON nf.id = sr.parent_id
            WHERE
                NOT nf.is_abstract
                AND nf.is_synthetic
        )

        SELECT DISTINCT ON (
            nf.company_id,
            nf.statement,
            nf.concept,
            nf.normalized_label,
            nf.axis,
            nf.member,
            nf.period_end
        )
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
            nf.statement,
            nf.period_end,
            nf.comparative_period_end,
            nf.period,
            nf.position,
            nf.parent_id,
            nf.abstract_id,
            nf.is_synthetic
        ORDER BY
            nf.company_id,
            nf.statement,
            nf.concept,
            nf.normalized_label,
            nf.axis,
            nf.member,
            nf.period_end
        """
    )


def downgrade() -> None:
    # Drop the views
    op.execute("DROP VIEW IF EXISTS financial_facts_normalized")
