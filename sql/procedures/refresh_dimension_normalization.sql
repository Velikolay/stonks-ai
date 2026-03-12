CREATE OR REPLACE PROCEDURE refresh_dimension_normalization(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE tmp_dimension_normalization_new ON COMMIT DROP AS
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
    financial_facts_normalized_base AS (
        SELECT
            ff.*,
            COALESCE(cnoc.normalized_label, cnog.normalized_label, cn.normalized_label, ff.label) AS normalized_label
        FROM financial_facts_overridden_cte ff
        LEFT JOIN concept_normalization cn
            ON ff.company_id = cn.company_id
            AND ff.statement = cn.statement
            AND ff.concept = cn.concept
        LEFT JOIN concept_normalization_overrides cnoc
            ON ff.company_id = cnoc.company_id
            AND ff.statement = cnoc.statement
            AND ff.concept = cnoc.concept
        LEFT JOIN concept_normalization_overrides cnog
            ON ff.statement = cnog.statement
            AND ff.concept = cnog.concept
            AND cnog.is_global = TRUE
        WHERE ff.axis <> ''
    ),
    dimension_normalized_base AS (
        SELECT DISTINCT ON (
            ff.company_id,
            ff.statement,
            ff.normalized_label,
            ff.axis,
            ff.member,
            ff.member_label,
            dno.normalized_axis_label,
            dno.normalized_member_label
        )
            ff.company_id,
            ff.statement,
            ff.normalized_label,
            ff.axis,
            ff.member,
            ff.member_label,
            dno.normalized_axis_label AS normalized_axis_label,
            dno.normalized_member_label AS normalized_member_label,
            ff.period_end,
            (dno.id IS NOT NULL) AS overridden,
            CASE
                WHEN dno.is_global = FALSE THEN 'company'
                WHEN dno.is_global = TRUE THEN 'global'
                ELSE NULL
            END AS override_priority,
            CASE
                WHEN dno.normalized_member_label IS NOT NULL THEN 'member'
                WHEN dno.normalized_axis_label IS NOT NULL THEN 'axis'
                ELSE NULL
            END AS override_level,
            md5(
                ff.company_id || '|' || ff.statement || '|' || ff.normalized_label || '|'
                || ff.axis || '|' || ff.member || '|' || ff.member_label
                || '|' || COALESCE(dno.normalized_axis_label, '')
                || '|' || COALESCE(dno.normalized_member_label, '')
                || '|' || 'grouping'
            ) AS id
        FROM financial_facts_normalized_base ff
        LEFT JOIN LATERAL (
            SELECT dno.*
            FROM dimension_normalization_overrides dno
            WHERE
                dno.axis = ff.axis
                AND (dno.company_id = ff.company_id OR dno.is_global = TRUE)
                AND (dno.member IS NULL OR dno.member = ff.member)
                AND (dno.member_label IS NULL OR dno.member_label = ff.member_label)
            ORDER BY
                (dno.company_id = ff.company_id) DESC,
                (dno.member IS NOT NULL) DESC,
                (dno.member_label IS NOT NULL) DESC,
                dno.updated_at DESC
            LIMIT 1
        ) dno ON TRUE
        ORDER BY
            ff.company_id,
            ff.statement,
            ff.normalized_label,
            ff.axis,
            ff.member,
            ff.member_label,
            dno.normalized_axis_label,
            dno.normalized_member_label,
            ff.period_end DESC
    ),
    exploded AS (
        SELECT company_id, statement, normalized_label, id, member AS key
        FROM dimension_normalized_base
        UNION
        SELECT company_id, statement, normalized_label, id, member_label AS key
        FROM dimension_normalized_base
        UNION
        SELECT company_id, statement, normalized_label, id, normalized_member_label AS key
        FROM dimension_normalized_base
    ),
    edges AS (
        SELECT DISTINCT e1.id AS src, e2.id AS dst
        FROM exploded e1
        JOIN exploded e2
            ON LOWER(e1.key) = LOWER(e2.key)
            AND e1.company_id = e2.company_id
            AND e1.statement = e2.statement
            AND e1.normalized_label = e2.normalized_label
    ),
    groups AS (
        SELECT id, id AS group_id
        FROM dimension_normalized_base

        UNION

        SELECT e.dst AS id, c.group_id
        FROM groups c
        JOIN edges e ON e.src = c.id
    ),
    groups_by_id AS (
        SELECT id, MIN(group_id) AS group_id
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
            b.period_end DESC
    ),
    dimension_normalization_grouping AS (
        SELECT
            b.company_id,
            b.statement,
            b.normalized_label,
            b.axis,
            b.member,
            b.member_label,
            c.normalized_axis_label,
            c.normalized_member_label,
            g.group_id,
            c.group_max_period_end,
            'grouping' AS source,
            c.overridden,
            c.override_priority,
            c.override_level
        FROM dimension_normalized_base b
        JOIN groups_by_id g USING (id)
        JOIN canonical c USING (group_id)
    ),
    facts AS (
        SELECT
            ff.*,
            COALESCE(dng.normalized_axis_label, ff.axis) AS normalized_axis_label,
            COALESCE(dng.normalized_member_label, ff.member_label) AS normalized_member_label,
            ff.value * ff.weight AS normalized_value,
            ff.comparative_value * ff.weight AS normalized_comparative_value,
            COALESCE(dng.overridden, FALSE) AS overridden,
            dng.override_priority,
            dng.override_level
        FROM financial_facts_normalized_base ff
        LEFT JOIN dimension_normalization_grouping dng
            ON ff.company_id = dng.company_id
            AND ff.statement = dng.statement
            AND ff.normalized_label = dng.normalized_label
            AND ff.axis = dng.axis
            AND ff.member = dng.member
            AND ff.member_label = dng.member_label
            AND ff.period_end <= dng.group_max_period_end
    ),
    same_period_pairs AS (
        SELECT DISTINCT
            f1.company_id,
            f1.statement,
            f1.normalized_label,
            f1.normalized_axis_label AS normalized_axis_label1,
            f1.normalized_member_label AS normalized_member_label1,
            f2.normalized_axis_label AS normalized_axis_label2,
            f2.normalized_member_label AS normalized_member_label2
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
            f1.concept AS concept1,
            f2.concept AS concept2,
            f1.normalized_label AS label1,
            f2.normalized_label AS label2,
            f1.period_end AS period_end1,
            f2.period_end AS period_end2,
            f1.axis AS axis1,
            f2.axis AS axis2,
            f1.member AS member1,
            f2.member AS member2,
            f1.member_label AS member_label1,
            f2.member_label AS member_label2,
            f1.normalized_axis_label AS normalized_axis_label1,
            f2.normalized_axis_label AS normalized_axis_label2,
            f1.normalized_member_label AS normalized_member_label1,
            f2.normalized_member_label AS normalized_member_label2,
            f1.overridden AS overridden1,
            f2.overridden AS overridden2,
            f1.override_priority AS override_priority1,
            f2.override_priority AS override_priority2,
            f1.override_level AS override_level1,
            f2.override_level AS override_level2
        FROM facts f1
        JOIN facts f2
            ON f1.company_id = f2.company_id
            AND f1.form_type = f2.form_type
            AND f1.statement = f2.statement
            AND f1.normalized_label = f2.normalized_label
            AND f1.normalized_comparative_value = f2.normalized_value
            AND f1.comparative_period_end = f2.period_end
        WHERE
            (
                (
                    (NOT f1.overridden OR NOT f2.overridden)
                    AND (
                        f1.normalized_axis_label <> f2.normalized_axis_label
                        OR f1.normalized_member_label <> f2.normalized_member_label
                    )
                )
                OR (
                    f1.override_level = 'axis'
                    AND f2.override_level = 'axis'
                    AND f1.normalized_axis_label = f2.normalized_axis_label
                    AND f1.normalized_member_label <> f2.normalized_member_label
                )
                OR (
                    f1.override_level = 'axis'
                    AND f2.override_level = 'member'
                    AND f1.normalized_axis_label = f2.normalized_axis_label
                    AND f1.normalized_member_label <> f2.normalized_member_label
                )
                OR (
                    f1.override_level = 'member'
                    AND f2.override_level = 'axis'
                    AND f1.normalized_axis_label = f2.normalized_axis_label
                    AND f1.normalized_member_label <> f2.normalized_member_label
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
    roots AS (
        SELECT
            company_id,
            statement,
            concept1 AS root_concept,
            label1 AS root_label,
            axis1 AS root_axis,
            member1 AS root_member,
            member_label1 AS root_member_label,
            normalized_axis_label1 AS root_normalized_axis_label,
            normalized_member_label1 AS root_normalized_member_label,
            overridden1 AS root_overridden,
            override_priority1 AS root_override_priority,
            override_level1 AS root_override_level,
            period_end1 AS root_period
        FROM matches
    ),
    chain AS (
        SELECT
            r.company_id,
            r.statement,
            r.root_concept AS concept,
            r.root_label AS label,
            r.root_axis AS axis,
            r.root_member AS member,
            r.root_member_label AS member_label,
            r.root_normalized_axis_label AS normalized_axis_label,
            r.root_normalized_member_label AS normalized_member_label,
            r.root_overridden AS overridden,
            r.root_override_priority AS override_priority,
            r.root_override_level AS override_level,
            r.root_period AS current_period,
            r.root_period AS root_period,
            md5(
                r.company_id || '|' || r.statement || '|' || r.root_concept || '|' || r.root_label || '|'
                || r.root_axis || '|' || r.root_member || '|' || r.root_member_label || '|'
                || r.root_normalized_axis_label || '|' || r.root_normalized_member_label || '|'
                || 'chaining'
            ) AS group_id
        FROM roots r

        UNION ALL

        SELECT
            c.company_id,
            c.statement,
            m.concept2 AS concept,
            m.label2 AS label,
            m.axis2 AS axis,
            m.member2 AS member,
            m.member_label2 AS member_label,
            m.normalized_axis_label2 AS normalized_axis_label,
            m.normalized_member_label2 AS normalized_member_label,
            m.overridden2 AS overridden,
            m.override_priority2 AS override_priority,
            m.override_level2 AS override_level,
            m.period_end2 AS current_period,
            c.root_period AS root_period,
            c.group_id AS group_id
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
            current_period DESC
    ),
    dimension_normalization_chaining AS (
        SELECT DISTINCT ON (company_id, statement, normalized_label, axis, member, member_label)
            company_id,
            statement,
            label AS normalized_label,
            axis,
            member,
            member_label,
            gn.normalized_axis_label,
            gn.normalized_member_label,
            group_id,
            root_period AS group_max_period_end,
            'chaining' AS source,
            gn.overridden,
            gn.override_priority,
            gn.override_level
        FROM chain
        JOIN group_normalized gn USING (group_id)
        ORDER BY
            company_id,
            statement,
            label,
            axis,
            member,
            member_label,
            normalized_axis_label,
            normalized_member_label,
            root_period DESC
    ),
    base AS (
        SELECT * FROM dimension_normalization_grouping
        UNION ALL
        SELECT * FROM dimension_normalization_chaining
    ),
    groups_distinct AS (
        SELECT DISTINCT company_id, statement, normalized_label, group_id
        FROM base
    ),
    group_members AS (
        SELECT DISTINCT
            company_id,
            statement,
            normalized_label,
            axis,
            member,
            member_label,
            group_id
        FROM base
    ),
    group_edges AS (
        SELECT DISTINCT
            gm1.company_id,
            gm1.statement,
            gm1.normalized_label,
            gm1.group_id AS group_id1,
            gm2.group_id AS group_id2
        FROM group_members gm1
        JOIN group_members gm2
            ON gm1.company_id = gm2.company_id
            AND gm1.statement = gm2.statement
            AND gm1.normalized_label = gm2.normalized_label
            AND gm1.axis = gm2.axis
            AND gm1.member = gm2.member
            AND gm1.member_label = gm2.member_label
        WHERE gm1.group_id <> gm2.group_id
    ),
    group_components AS (
        WITH RECURSIVE reach AS (
            SELECT company_id, statement, normalized_label, group_id AS root_group_id, group_id
            FROM groups_distinct

            UNION

            SELECT r.company_id, r.statement, r.normalized_label, r.root_group_id, e.group_id2 AS group_id
            FROM reach r
            JOIN group_edges e
                ON e.company_id = r.company_id
                AND e.statement = r.statement
                AND e.normalized_label = r.normalized_label
                AND e.group_id1 = r.group_id
        )
        SELECT
            company_id,
            statement,
            normalized_label,
            group_id,
            MIN(root_group_id) AS component_id
        FROM reach
        GROUP BY company_id, statement, normalized_label, group_id
    ),
    component_canonical AS (
        SELECT DISTINCT ON (gc.company_id, gc.statement, gc.normalized_label, gc.component_id)
            gc.company_id,
            gc.statement,
            gc.normalized_label,
            gc.component_id,
            b.normalized_axis_label,
            b.normalized_member_label,
            b.source,
            b.overridden,
            b.override_priority,
            b.override_level
        FROM group_components gc
        JOIN base b
            ON b.company_id = gc.company_id
            AND b.statement = gc.statement
            AND b.normalized_label = gc.normalized_label
            AND b.group_id = gc.group_id
        ORDER BY
            gc.company_id,
            gc.statement,
            gc.normalized_label,
            gc.component_id,
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
            b.group_max_period_end DESC
    ),
    component_group_max_period_end AS (
        SELECT
            gc.company_id,
            gc.statement,
            gc.normalized_label,
            gc.component_id,
            MAX(b.group_max_period_end) AS group_max_period_end
        FROM group_components gc
        JOIN base b
            ON b.company_id = gc.company_id
            AND b.statement = gc.statement
            AND b.normalized_label = gc.normalized_label
            AND b.group_id = gc.group_id
        GROUP BY gc.company_id, gc.statement, gc.normalized_label, gc.component_id
    )
    SELECT DISTINCT ON (gm.company_id, gm.statement, gm.normalized_label, gm.axis, gm.member, gm.member_label)
        gm.company_id,
        gm.statement,
        gm.normalized_label,
        gm.axis,
        gm.member,
        gm.member_label,
        cc.normalized_axis_label,
        cc.normalized_member_label,
        gc.component_id AS group_id,
        cgmp.group_max_period_end,
        cc.source,
        cc.overridden,
        cc.override_priority,
        cc.override_level
    FROM group_members gm
    JOIN group_components gc
        ON gc.company_id = gm.company_id
        AND gc.statement = gm.statement
        AND gc.normalized_label = gm.normalized_label
        AND gc.group_id = gm.group_id
    JOIN component_canonical cc
        ON cc.company_id = gc.company_id
        AND cc.statement = gc.statement
        AND cc.normalized_label = gc.normalized_label
        AND cc.component_id = gc.component_id
    JOIN component_group_max_period_end cgmp
        ON cgmp.company_id = gc.company_id
        AND cgmp.statement = gc.statement
        AND cgmp.normalized_label = gc.normalized_label
        AND cgmp.component_id = gc.component_id
    ORDER BY
        gm.company_id,
        gm.statement,
        gm.normalized_label,
        gm.axis,
        gm.member,
        gm.member_label,
        gc.component_id;

    DELETE FROM dimension_normalization dn
    WHERE
        dn.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_dimension_normalization_new t
            WHERE
                t.company_id = dn.company_id
                AND t.statement = dn.statement
                AND t.normalized_label = dn.normalized_label
                AND t.axis = dn.axis
                AND t.member = dn.member
                AND t.member_label = dn.member_label
        );

    INSERT INTO dimension_normalization (
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        member_label,
        normalized_axis_label,
        normalized_member_label,
        group_id,
        group_max_period_end,
        source,
        overridden,
        override_priority,
        override_level
    )
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        member_label,
        normalized_axis_label,
        normalized_member_label,
        group_id,
        group_max_period_end,
        source,
        overridden,
        override_priority,
        override_level
    FROM tmp_dimension_normalization_new
    ON CONFLICT (company_id, statement, normalized_label, axis, member, member_label) DO UPDATE
    SET
        normalized_axis_label = EXCLUDED.normalized_axis_label,
        normalized_member_label = EXCLUDED.normalized_member_label,
        group_id = EXCLUDED.group_id,
        group_max_period_end = EXCLUDED.group_max_period_end,
        source = EXCLUDED.source,
        overridden = EXCLUDED.overridden,
        override_priority = EXCLUDED.override_priority,
        override_level = EXCLUDED.override_level;
END;
$$;
