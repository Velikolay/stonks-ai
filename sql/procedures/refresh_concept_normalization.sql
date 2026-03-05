CREATE OR REPLACE PROCEDURE refresh_concept_normalization(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE tmp_concept_normalization_new ON COMMIT DROP AS
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
            ff.weight,
            ff.unit,
            COALESCE(ffo.axis, ff.axis) AS axis,
            COALESCE(ffo.member, ff.member) AS member,
            ff.member_label,
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
    concept_normalization_grouping AS (
        SELECT
            company_id,
            statement,
            concept,
            (ARRAY_AGG(label ORDER BY period_end DESC))[1] AS normalized_label,
            md5(company_id || '|' || statement || '|' || concept || '|' || 'grouping') AS group_id,
            MAX(period_end) AS group_max_period_end,
            'grouping' AS source
        FROM financial_facts_overridden_cte ff
        WHERE axis = ''
        GROUP BY
            company_id,
            statement,
            concept
        HAVING
            COUNT(DISTINCT label) > 1
            AND COUNT(DISTINCT (filing_id, label)) = COUNT(DISTINCT filing_id)
    ),
    facts AS (
        SELECT
            ff.*,
            COALESCE(cng.normalized_label, ff.label) AS normalized_label,
            ff.value * ff.weight AS normalized_value,
            ff.comparative_value * ff.weight AS normalized_comparative_value
        FROM financial_facts_overridden_cte ff
        LEFT JOIN concept_normalization_grouping cng
            ON ff.company_id = cng.company_id
            AND ff.statement = cng.statement
            AND ff.concept = cng.concept
            AND ff.period_end <= cng.group_max_period_end
        WHERE ff.axis = ''
    ),
    candidate_matches AS (
        SELECT DISTINCT ON (
            f1.company_id,
            f1.statement,
            f1.concept,
            f2.concept,
            f1.period_end,
            f2.period_end
        )
            f1.company_id,
            f1.statement,
            f1.concept AS concept1,
            f2.concept AS concept2,
            f1.period_end AS period_end1,
            f2.period_end AS period_end2,
            f1.normalized_label AS label1,
            f2.normalized_label AS label2
        FROM facts f1
        JOIN facts f2
            ON f1.company_id = f2.company_id
            AND f1.form_type = f2.form_type
            AND f1.statement = f2.statement
            AND f1.normalized_comparative_value = f2.normalized_value
            AND f1.comparative_period_end = f2.period_end
        WHERE
            f1.concept <> f2.concept
            AND f1.period_end > f2.period_end
            AND NOT EXISTS (
                SELECT 1
                FROM financial_facts_overridden_cte fx
                WHERE fx.company_id = f1.company_id
                    AND fx.statement = f1.statement
                    AND fx.period_end = f1.period_end
                    AND fx.concept = f2.concept
            )
            AND NOT EXISTS (
                SELECT 1
                FROM financial_facts_overridden_cte fx
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
            AND (a.period_end1 = b.period_end2 OR a.period_end2 = b.period_end1)
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
        WHERE NOT EXISTS (
            SELECT 1
            FROM false_matches fm
            WHERE cm.company_id = fm.company_id
                AND cm.statement = fm.statement
                AND cm.concept1 = fm.concept1
                AND cm.concept2 = fm.concept2
        )
    ),
    roots AS (
        SELECT
            company_id,
            statement,
            concept1 AS root_concept,
            label1 AS root_label,
            period_end1 AS root_period
        FROM matches
    ),
    chain AS (
        SELECT
            r.company_id,
            r.statement,
            r.root_concept AS concept,
            r.root_label AS normalized_label,
            r.root_period AS current_period,
            r.root_period AS root_period,
            md5(r.company_id || '|' || r.statement || '|' || r.root_concept || '|' || 'chaining') AS group_id
        FROM roots r

        UNION ALL

        SELECT
            c.company_id,
            c.statement,
            m.concept2 AS concept,
            c.normalized_label,
            m.period_end2 AS current_period,
            c.root_period AS root_period,
            c.group_id AS group_id
        FROM chain c
        JOIN matches m
            ON m.company_id = c.company_id
            AND m.statement = c.statement
            AND m.concept1 = c.concept
            AND m.period_end1 <= c.current_period
    ),
    concept_normalization_chaining AS (
        SELECT DISTINCT ON (company_id, statement, concept)
            company_id,
            statement,
            concept,
            normalized_label,
            group_id,
            root_period AS group_max_period_end,
            'chaining' AS source
        FROM chain
        ORDER BY company_id, statement, concept, root_period DESC
    ),
    concept_normalization_combined AS (
        SELECT DISTINCT ON (company_id, statement, concept)
            company_id,
            statement,
            concept,
            normalized_label,
            group_id,
            group_max_period_end,
            source
        FROM (
            SELECT *, 1 AS src_priority FROM concept_normalization_chaining
            UNION ALL
            SELECT *, 2 AS src_priority FROM concept_normalization_grouping
        ) t
        ORDER BY company_id, statement, concept, src_priority
    ),
    global_group_overrides AS (
        SELECT
            cn.group_id,
            MAX(cno.normalized_label) AS normalized_label,
            MAX(cno.weight) AS weight,
            MAX(cno.unit) AS unit
        FROM concept_normalization_combined cn
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
            MAX(cno.normalized_label) AS normalized_label,
            MAX(cno.weight) AS weight,
            MAX(cno.unit) AS unit
        FROM concept_normalization_combined cn
        JOIN concept_normalization_overrides cno
            ON cn.company_id = cno.company_id
            AND cn.statement = cno.statement
            AND cn.concept = cno.concept
        GROUP BY cn.group_id, cn.company_id
    )
    SELECT
        cn.company_id,
        cn.statement,
        cn.concept,
        COALESCE(cgo.normalized_label, ggo.normalized_label, cn.normalized_label) AS normalized_label,
        COALESCE(cgo.weight, ggo.weight) AS weight,
        COALESCE(cgo.unit, ggo.unit) AS unit,
        cn.group_id,
        cn.source,
        COALESCE(cgo.normalized_label, ggo.normalized_label) IS NOT NULL AS overridden
    FROM concept_normalization_combined cn
    LEFT JOIN company_group_overrides cgo
        ON cn.company_id = cgo.company_id
        AND cn.group_id = cgo.group_id
    LEFT JOIN global_group_overrides ggo
        ON cn.group_id = ggo.group_id
    WHERE cn.company_id = ANY(company_ids);

    INSERT INTO concept_normalization (
        company_id,
        statement,
        concept,
        normalized_label,
        weight,
        unit,
        group_id,
        source,
        overridden
    )
    SELECT
        company_id,
        statement,
        concept,
        normalized_label,
        weight,
        unit,
        group_id,
        source,
        overridden
    FROM tmp_concept_normalization_new
    ON CONFLICT (company_id, statement, concept) DO UPDATE
    SET
        normalized_label = EXCLUDED.normalized_label,
        weight = EXCLUDED.weight,
        unit = EXCLUDED.unit,
        group_id = EXCLUDED.group_id,
        source = EXCLUDED.source,
        overridden = EXCLUDED.overridden;

    DELETE FROM concept_normalization cn
    WHERE
        cn.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_concept_normalization_new t
            WHERE
                t.company_id = cn.company_id
                AND t.statement = cn.statement
                AND t.concept = cn.concept
        );
END;
$$;
