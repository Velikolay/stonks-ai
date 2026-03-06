CREATE OR REPLACE PROCEDURE refresh_hierarchy_normalization(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE tmp_hierarchy_normalization_new ON COMMIT DROP AS
    WITH concept_normalization_cte AS (
        SELECT *
        FROM concept_normalization
        WHERE company_id = ANY(company_ids)
    ),
    concept_expansion AS (
        SELECT
            cne.company_id,
            cne.statement,
            cno.concept,
            cno.parent_concept,
            cne.concept AS concept_expand
        FROM concept_normalization_cte cn
        JOIN LATERAL (
            SELECT *
            FROM concept_normalization_overrides o
            WHERE
                o.statement = cn.statement
                AND o.concept = cn.concept
                AND (o.company_id = cn.company_id OR o.is_global = TRUE)
            ORDER BY (o.company_id = cn.company_id) DESC
            LIMIT 1
        ) cno ON TRUE
        JOIN concept_normalization_cte cne
            ON cn.group_id = cne.group_id
        WHERE cno.parent_concept IS NOT NULL
    ),
    parent_concept_expansion AS (
        SELECT
            cne.company_id,
            cne.statement,
            cno.concept,
            cno.parent_concept,
            cne.concept AS parent_concept_expand
        FROM concept_normalization_cte cn
        JOIN LATERAL (
            SELECT *
            FROM concept_normalization_overrides o
            WHERE
                o.statement = cn.statement
                AND o.parent_concept = cn.concept
                AND (o.company_id = cn.company_id OR o.is_global = TRUE)
            ORDER BY (o.company_id = cn.company_id) DESC
            LIMIT 1
        ) cno ON TRUE
        JOIN concept_normalization_cte cne
            ON cn.group_id = cne.group_id
        WHERE cno.parent_concept IS NOT NULL
    ),
    transitive_expansion AS (
        SELECT
            ce.company_id,
            ce.statement,
            ce.concept_expand AS concept,
            pce.parent_concept_expand AS parent_concept,
            ce.concept AS concept_source,
            ce.parent_concept AS parent_concept_source
        FROM concept_expansion ce
        JOIN parent_concept_expansion pce
            ON ce.company_id = pce.company_id
            AND ce.statement = pce.statement
            AND ce.concept = pce.concept
    ),
    raw_rows AS (
        SELECT
            company_id,
            statement,
            concept_expand AS concept,
            parent_concept,
            concept AS concept_source,
            parent_concept AS parent_concept_source
        FROM concept_expansion

        UNION ALL

        SELECT
            company_id,
            statement,
            concept,
            parent_concept_expand AS parent_concept,
            concept AS concept_source,
            parent_concept AS parent_concept_source
        FROM parent_concept_expansion

        UNION ALL

        SELECT
            company_id,
            statement,
            concept,
            parent_concept,
            concept_source,
            parent_concept_source
        FROM transitive_expansion
    )
    SELECT
        company_id,
        statement,
        concept,
        parent_concept,
        MIN(concept_source) AS concept_source,
        MIN(parent_concept_source) AS parent_concept_source
    FROM raw_rows
    GROUP BY
        company_id,
        statement,
        concept,
        parent_concept;

    DELETE FROM hierarchy_normalization hn
    WHERE
        hn.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_hierarchy_normalization_new t
            WHERE
                t.company_id = hn.company_id
                AND t.statement = hn.statement
                AND t.concept = hn.concept
                AND t.parent_concept = hn.parent_concept
        );

    INSERT INTO hierarchy_normalization (
        company_id,
        statement,
        concept,
        parent_concept,
        concept_source,
        parent_concept_source
    )
    SELECT
        company_id,
        statement,
        concept,
        parent_concept,
        concept_source,
        parent_concept_source
    FROM tmp_hierarchy_normalization_new
    ON CONFLICT (company_id, statement, concept, parent_concept) DO UPDATE
    SET
        concept_source = EXCLUDED.concept_source,
        parent_concept_source = EXCLUDED.parent_concept_source;
END;
$$;
