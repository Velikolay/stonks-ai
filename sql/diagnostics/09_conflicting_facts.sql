-- Facts silently dropped before normalization.
--
-- refresh_financial_facts_normalized builds conflicting_fact_keys and excludes
-- every fact whose (company, statement, concept, label, axis, member,
-- member_label, period_end, form_type) group reports more than one distinct
-- value. That is usually a restatement across two filings. Nothing downstream
-- reports the loss, so check it here.

\echo '--- Fact groups excluded because they report conflicting values ---'
WITH base AS (
    SELECT
        ff.id,
        ff.company_id,
        ff.statement,
        COALESCE(ffo.concept, ff.concept) AS concept,
        ff.label,
        COALESCE(ffo.axis, ff.axis) AS axis,
        COALESCE(ffo.member, ff.member) AS member,
        COALESCE(ffo.member_label, ff.member_label) AS member_label,
        ff.period_end,
        ff.form_type,
        ff.filing_id,
        ff.value
    FROM financial_facts ff
    LEFT JOIN financial_facts_overridden ffo
        ON ffo.id = ff.id
        AND ffo.company_id = ff.company_id
)
SELECT
    company_id,
    statement,
    concept,
    label,
    axis,
    member,
    period_end,
    form_type,
    COUNT(*) AS facts,
    COUNT(DISTINCT value) AS distinct_values,
    array_agg(DISTINCT value) AS values,
    array_agg(DISTINCT filing_id) AS filing_ids
FROM base
GROUP BY
    company_id,
    statement,
    concept,
    label,
    axis,
    member,
    member_label,
    period_end,
    form_type
HAVING COUNT(DISTINCT value) > 1
ORDER BY distinct_values DESC, company_id, statement, concept
LIMIT 50;

\echo '--- Raw facts that never reached financial_facts_normalized ---'
SELECT
    ff.company_id,
    ff.statement,
    ff.concept,
    ff.label,
    ff.period_end,
    COUNT(*) AS facts
FROM financial_facts ff
WHERE NOT EXISTS (
    SELECT 1 FROM financial_facts_normalized n WHERE n.id = ff.id
)
GROUP BY ff.company_id, ff.statement, ff.concept, ff.label, ff.period_end
ORDER BY facts DESC
LIMIT 50;
