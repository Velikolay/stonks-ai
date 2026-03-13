CREATE OR REPLACE PROCEDURE refresh_financial_facts_normalized(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE tmp_financial_facts_normalized_new ON COMMIT DROP AS
    WITH RECURSIVE financial_facts_overridden_cte AS (
        SELECT
            ff.id,
            ff.filing_id,
            ff.company_id,
            ff.form_type,
            COALESCE(ffo.concept, ff.concept) AS concept,
            ff.label,
            ff.is_abstract,
            ff.value,
            ff.comparative_value,
            COALESCE(ffo.weight, ff.weight) AS weight,
            ff.unit,
            COALESCE(ffo.axis, ff.axis) AS axis,
            COALESCE(ffo.member, ff.member) AS member,
            COALESCE(ffo.member_label, ff.member_label) AS member_label,
            ff.statement,
            ff.period_end,
            ff.comparative_period_end,
            ff.period,
            ff.position,
            ff.parent_id,
            ff.abstract_id,
            ffo.fact_override_id
        FROM financial_facts ff
        LEFT JOIN financial_facts_overridden ffo
            ON ffo.id = ff.id
            AND ffo.company_id = ff.company_id
        WHERE ff.company_id = ANY(company_ids)
    ),
    hierarchy_normalization_cte AS (
        SELECT *
        FROM hierarchy_normalization
        WHERE company_id = ANY(company_ids)
    ),
    normalized_facts AS (
        SELECT
            ff.id,
            ff.company_id,
            ff.filing_id,
            ff.form_type,
            ff.concept,
            ff.label,
            COALESCE(cno.normalized_label, cn.normalized_label, ff.label) AS normalized_label,
            ff.is_abstract,
            CASE
                WHEN ff.weight * COALESCE(cno.weight, cn.weight, ff.weight) < 0
                THEN -ff.value
                ELSE ff.value
            END AS value,
            CASE
                WHEN ff.weight * COALESCE(cno.weight, cn.weight, ff.weight) < 0
                THEN -ff.comparative_value
                ELSE ff.comparative_value
            END AS comparative_value,
            COALESCE(cno.weight, cn.weight, ff.weight) AS weight,
            COALESCE(cno.unit, cn.unit, ff.unit) AS unit,
            COALESCE(dn.normalized_axis_label, ff.axis) AS axis,
            COALESCE(dn.normalized_member_label, ff.member_label) AS member,
            ff.statement,
            ff.period_end,
            ff.comparative_period_end,
            ff.period,
            ff.position,
            CASE
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NOT NULL THEN
                    ffp.id
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    abs(hashtextextended(
                        ff.statement || '|' || COALESCE(cno.parent_concept, hn.parent_concept_source) || '|'
                        || ff.filing_id::text || '|' || ff.company_id::text,
                        0
                    ))
                ELSE
                    ff.parent_id
            END AS parent_id,
            CASE
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    COALESCE(cno.parent_concept, hn.parent_concept_source)
                ELSE
                    NULL
            END AS parent_concept,
            CASE
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NOT NULL THEN
                    ffa.id
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                    abs(hashtextextended(
                        ff.statement || '|' || cno.abstract_concept || '|'
                        || ff.filing_id::text || '|' || ff.company_id::text,
                        0
                    ))
                ELSE
                    ff.abstract_id
            END AS abstract_id,
            CASE
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                    cno.abstract_concept
                ELSE
                    NULL
            END AS abstract_concept,
            FALSE AS is_synthetic
        FROM financial_facts_overridden_cte ff
        LEFT JOIN LATERAL (
            SELECT *
            FROM concept_normalization_overrides o
            WHERE
                o.statement = ff.statement
                AND o.concept = ff.concept
                AND (o.company_id = ff.company_id OR o.is_global = TRUE)
            ORDER BY (o.company_id = ff.company_id) DESC
            LIMIT 1
        ) cno ON TRUE
        LEFT JOIN hierarchy_normalization_cte hn
            ON hn.company_id = ff.company_id
            AND hn.statement = ff.statement
            AND hn.concept = ff.concept
        LEFT JOIN financial_facts_overridden_cte ffp
            ON ffp.company_id = ff.company_id
            AND ffp.filing_id = ff.filing_id
            AND ffp.statement = ff.statement
            AND ffp.concept = COALESCE(cno.parent_concept, hn.parent_concept)
        LEFT JOIN financial_facts_overridden_cte ffa
            ON ffa.company_id = ff.company_id
            AND ffa.filing_id = ff.filing_id
            AND ffa.statement = ff.statement
            AND ffa.concept = cno.abstract_concept
        LEFT JOIN concept_normalization cn
            ON ff.company_id = cn.company_id
            AND ff.statement = cn.statement
            AND ff.concept = cn.concept
        LEFT JOIN dimension_normalization dn
            ON ff.company_id = dn.company_id
            AND ff.statement = dn.statement
            AND COALESCE(cno.normalized_label, cn.normalized_label, ff.label) = dn.normalized_label
            AND ff.axis = dn.axis
            AND ff.member = dn.member
            AND ff.member_label = dn.member_label

        UNION

        SELECT
            new.id AS id,
            f.company_id,
            f.filing_id,
            f.form_type,
            cno.concept,
            cno.normalized_label AS label,
            cno.normalized_label,
            cno.is_abstract,
            0 AS value,
            0 AS comparative_value,
            cno.weight,
            cno.unit,
            '' AS axis,
            '' AS member,
            cno.statement,
            f.period_end,
            f.comparative_period_end,
            f.period,
            99 AS position,
            CASE
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NOT NULL THEN
                    ffp.id
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    abs(hashtextextended(
                        f.statement || '|' || COALESCE(cno.parent_concept, hn.parent_concept_source) || '|'
                        || f.filing_id::text || '|' || f.company_id::text,
                        0
                    ))
                ELSE
                    NULL
            END AS parent_id,
            CASE
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    COALESCE(cno.parent_concept, hn.parent_concept_source)
                ELSE
                    NULL
            END AS parent_concept,
            CASE
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NOT NULL THEN
                    ffa.id
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                    abs(hashtextextended(
                        f.statement || '|' || cno.abstract_concept || '|'
                        || f.filing_id::text || '|' || f.company_id::text,
                        0
                    ))
                ELSE
                    NULL
            END AS abstract_id,
            CASE
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                    cno.abstract_concept
                ELSE
                    NULL
            END AS abstract_concept,
            TRUE AS is_synthetic
        FROM normalized_facts f
        JOIN LATERAL (
            VALUES
                (f.parent_concept, f.parent_id),
                (f.abstract_concept, f.abstract_id)
        ) AS new(concept, id)
            ON new.concept IS NOT NULL
        JOIN LATERAL (
            SELECT *
            FROM concept_normalization_overrides o
            WHERE
                o.statement = f.statement
                AND o.concept = new.concept
                AND (o.company_id = f.company_id OR o.is_global = TRUE)
            ORDER BY (o.company_id = f.company_id) DESC
            LIMIT 1
        ) cno ON TRUE
        LEFT JOIN hierarchy_normalization_cte hn
            ON hn.company_id = f.company_id
            AND hn.statement = f.statement
            AND hn.concept = new.concept
        LEFT JOIN financial_facts_overridden_cte ffp
            ON ffp.company_id = f.company_id
            AND ffp.filing_id = f.filing_id
            AND ffp.statement = f.statement
            AND ffp.concept = COALESCE(cno.parent_concept, hn.parent_concept)
        LEFT JOIN financial_facts_overridden_cte ffa
            ON ffa.company_id = f.company_id
            AND ffa.filing_id = f.filing_id
            AND ffa.statement = f.statement
            AND ffa.concept = cno.abstract_concept
    ),
    synthetic_rollup AS (
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
    ),
    rolled_up_facts AS (
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
                WHEN nf.is_synthetic THEN SUM(sr.contrib_value)
                ELSE nf.value
            END AS value,
            CASE
                WHEN nf.is_synthetic THEN SUM(sr.contrib_comparative_value)
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
    ),
    distinct_rolled_up_facts AS (
        SELECT DISTINCT ON (
            company_id, statement, normalized_label, axis, member, value, period_end
        )
            *
        FROM rolled_up_facts
        ORDER BY company_id, statement, normalized_label, axis, member, value, period_end, id
    ),
    chain_walk AS (
        SELECT
            r.id,
            r.company_id,
            r.statement,
            r.concept,
            r.normalized_label,
            r.axis,
            r.member,
            r.period_end,
            r.value,
            r.comparative_value,
            r.comparative_period_end,
            1::bigint AS depth
        FROM rolled_up_facts r
        WHERE r.comparative_value IS NOT NULL
          AND r.comparative_period_end IS NOT NULL

        UNION ALL

        SELECT
            COALESCE(r_value.id, r_concept.id) AS id,
            COALESCE(r_value.company_id, r_concept.company_id) AS company_id,
            COALESCE(r_value.statement, r_concept.statement) AS statement,
            COALESCE(r_value.concept, r_concept.concept) AS concept,
            COALESCE(r_value.normalized_label, r_concept.normalized_label) AS normalized_label,
            COALESCE(r_value.axis, r_concept.axis) AS axis,
            COALESCE(r_value.member, r_concept.member) AS member,
            COALESCE(r_value.period_end, r_concept.period_end) AS period_end,
            COALESCE(r_value.value, r_concept.value) AS value,
            COALESCE(r_value.comparative_value, r_concept.comparative_value) AS comparative_value,
            COALESCE(r_value.comparative_period_end, r_concept.comparative_period_end) AS comparative_period_end,
            c.depth + 1
        FROM chain_walk c
        -- use value match with priority
        LEFT JOIN distinct_rolled_up_facts r_value
            ON r_value.company_id = c.company_id
            AND r_value.statement = c.statement
            AND r_value.normalized_label = c.normalized_label
            AND r_value.axis = c.axis
            AND r_value.member = c.member
            AND r_value.value = c.comparative_value
            AND r_value.period_end = c.comparative_period_end
        -- fallback to concept match if value is not found
        LEFT JOIN distinct_rolled_up_facts r_concept
            ON r_concept.company_id = c.company_id
            AND r_concept.statement = c.statement
            AND r_concept.normalized_label = c.normalized_label
            AND r_concept.axis = c.axis
            AND r_concept.member = c.member
            AND r_concept.concept = c.concept
            AND r_concept.period_end = c.comparative_period_end
            AND r_value.id IS NULL

        WHERE r_value.id IS NOT NULL
        OR r_concept.id IS NOT NULL
    ),
    chain_depth_per_id AS (
        SELECT r.id, MAX(cw.depth) AS chain_depth
        FROM rolled_up_facts r
        LEFT JOIN chain_walk cw
            ON r.company_id = cw.company_id
            AND r.statement = cw.statement
            AND r.normalized_label = cw.normalized_label
            AND r.axis = cw.axis
            AND r.member = cw.member
            AND r.value = cw.value
            AND r.period_end = cw.period_end
        GROUP BY r.id
    ),
    deduped AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.company_id, r.statement, r.normalized_label,
                             r.axis, r.member, r.period_end
                ORDER BY COALESCE(cd.chain_depth, 1) DESC, r.id ASC
            ) AS rn
        FROM rolled_up_facts r
        LEFT JOIN chain_depth_per_id cd ON cd.id = r.id
    ),
    deduped_by_id AS (
        SELECT DISTINCT ON (id)
            id,
            company_id,
            filing_id,
            form_type,
            concept,
            label,
            normalized_label,
            is_abstract,
            value,
            comparative_value,
            weight,
            unit,
            axis,
            member,
            statement,
            period_end,
            comparative_period_end,
            period,
            position,
            parent_id,
            abstract_id,
            is_synthetic,
            rn > 1 AS is_duplicate
        FROM deduped
        ORDER BY id, is_duplicate ASC
    )
    SELECT * FROM deduped_by_id;

    DELETE FROM financial_facts_normalized ff
    WHERE
        ff.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_financial_facts_normalized_new t
            WHERE t.id = ff.id
        );

    INSERT INTO financial_facts_normalized (
        id,
        company_id,
        filing_id,
        form_type,
        concept,
        label,
        normalized_label,
        is_abstract,
        value,
        comparative_value,
        weight,
        unit,
        axis,
        member,
        statement,
        period_end,
        comparative_period_end,
        period,
        position,
        parent_id,
        abstract_id,
        is_synthetic,
        is_duplicate
    )
    SELECT
        id,
        company_id,
        filing_id,
        form_type,
        concept,
        label,
        normalized_label,
        is_abstract,
        value,
        comparative_value,
        weight,
        unit,
        axis,
        member,
        statement,
        period_end,
        comparative_period_end,
        period,
        position,
        parent_id,
        abstract_id,
        is_synthetic,
        is_duplicate
    FROM tmp_financial_facts_normalized_new
    ON CONFLICT (id) DO UPDATE
    SET
        company_id = EXCLUDED.company_id,
        filing_id = EXCLUDED.filing_id,
        form_type = EXCLUDED.form_type,
        concept = EXCLUDED.concept,
        label = EXCLUDED.label,
        normalized_label = EXCLUDED.normalized_label,
        is_abstract = EXCLUDED.is_abstract,
        value = EXCLUDED.value,
        comparative_value = EXCLUDED.comparative_value,
        weight = EXCLUDED.weight,
        unit = EXCLUDED.unit,
        axis = EXCLUDED.axis,
        member = EXCLUDED.member,
        statement = EXCLUDED.statement,
        period_end = EXCLUDED.period_end,
        comparative_period_end = EXCLUDED.comparative_period_end,
        period = EXCLUDED.period,
        position = EXCLUDED.position,
        parent_id = EXCLUDED.parent_id,
        abstract_id = EXCLUDED.abstract_id,
        is_synthetic = EXCLUDED.is_synthetic,
        is_duplicate = EXCLUDED.is_duplicate;
END;
$$;
