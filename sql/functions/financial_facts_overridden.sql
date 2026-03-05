CREATE OR REPLACE FUNCTION financial_facts_overridden(company_ids int[])
RETURNS TABLE (
    id bigint,
    filing_id int,
    company_id int,
    form_type text,
    concept text,
    label text,
    is_abstract boolean,
    value numeric,
    comparative_value numeric,
    weight numeric,
    unit text,
    axis text,
    member text,
    member_label text,
    statement text,
    period_end date,
    comparative_period_end date,
    period period_type,
    "position" int,
    parent_id bigint,
    abstract_id bigint,
    fact_override_id int
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        ff.id,
        ff.filing_id,
        ff.company_id,
        ff.form_type,
        COALESCE(r.to_concept, ff.concept) AS concept,
        ff.label,
        ff.is_abstract,
        ff.value,
        ff.comparative_value,
        ff.weight,
        ff.unit,
        COALESCE(r.to_axis, ff.axis) AS axis,
        COALESCE(r.to_member, ff.member) AS member,
        ff.member_label,
        ff.statement,
        ff.period_end,
        ff.comparative_period_end,
        ff.period,
        ff.position,
        ff.parent_id,
        ff.abstract_id,
        r.id AS fact_override_id
    FROM financial_facts ff
    LEFT JOIN LATERAL (
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
            r.updated_at DESC
        LIMIT 1
    ) r ON TRUE
    WHERE ff.company_id = ANY(company_ids);
$$;
