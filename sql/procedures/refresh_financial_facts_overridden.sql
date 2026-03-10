CREATE OR REPLACE PROCEDURE refresh_financial_facts_overridden(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE tmp_financial_facts_overridden_new ON COMMIT DROP AS
    SELECT
        ff.id,
        ff.company_id,
        ff.statement,
        COALESCE(r.to_concept, ff.concept) AS concept,
        COALESCE(r.to_axis, ff.axis) AS axis,
        COALESCE(r.to_member, ff.member) AS member,
        COALESCE(r.to_member_label, ff.member_label) AS member_label,
        COALESCE(r.to_weight, ff.weight) AS weight,
        r.id AS fact_override_id
    FROM financial_facts ff
    JOIN LATERAL (
        SELECT r.*
        FROM financial_facts_overrides r
        WHERE
            r.statement = ff.statement
            AND r.concept = ff.concept
            AND (r.company_id = ff.company_id OR r.is_global = TRUE)
            AND (r.axis IS NULL OR r.axis = ff.axis)
            AND (r.member IS NULL OR r.member = ff.member)
            AND (r.label IS NULL OR r.label = ff.label)
            AND (r.form_type IS NULL OR r.form_type = ff.form_type)
            AND (r.from_period IS NULL OR ff.period_end >= r.from_period)
            AND (r.to_period IS NULL OR ff.period_end <= r.to_period)
        ORDER BY
            (r.company_id = ff.company_id) DESC,
            (
                (r.axis IS NOT NULL)::int
                + (r.member IS NOT NULL)::int
                + (r.label IS NOT NULL)::int
                + (r.form_type IS NOT NULL)::int
                + (r.from_period IS NOT NULL)::int
                + (r.to_period IS NOT NULL)::int
            ) DESC,
            (r.to_period - r.from_period) ASC NULLS LAST,
            r.updated_at DESC
        LIMIT 1
    ) r ON TRUE
    WHERE ff.company_id = ANY(company_ids);

    DELETE FROM financial_facts_overridden ffo
    WHERE
        ffo.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_financial_facts_overridden_new t
            WHERE t.id = ffo.id
        );

    INSERT INTO financial_facts_overridden (
        id,
        company_id,
        statement,
        concept,
        axis,
        member,
        member_label,
        weight,
        fact_override_id
    )
    SELECT
        id,
        company_id,
        statement,
        concept,
        axis,
        member,
        member_label,
        weight,
        fact_override_id
    FROM tmp_financial_facts_overridden_new
    ON CONFLICT (id) DO UPDATE
    SET
        company_id = EXCLUDED.company_id,
        statement = EXCLUDED.statement,
        concept = EXCLUDED.concept,
        axis = EXCLUDED.axis,
        member = EXCLUDED.member,
        member_label = EXCLUDED.member_label,
        weight = EXCLUDED.weight,
        fact_override_id = EXCLUDED.fact_override_id;
END;
$$;
