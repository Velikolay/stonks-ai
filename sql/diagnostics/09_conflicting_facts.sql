-- Slots where two filings disagree.
--
-- Normalization keeps the latest filing in the group and drops the earlier
-- facts. A group whose latest filing still reports two values is dropped
-- entirely. A quarter and its year-to-date total share a date and are not a
-- conflict, because period is part of the group. Nothing downstream reports
-- which value won, so check it here.

\echo '--- Fact groups where filings report conflicting values ---'
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
        ff.period,
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
    period,
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
    form_type,
    period
HAVING COUNT(DISTINCT value) > 1
ORDER BY distinct_values DESC, company_id, statement, concept
LIMIT 50;

\echo '--- Raw facts that never reached financial_facts_normalized ---'
-- A finer cut (the slot's amount cut again by a second, non-default
-- dimension) beside the same filing's fact on that slot is dropped on
-- purpose (without_finer_cuts in filings/refresh/rules/facts.py) and is not
-- listed.
WITH coordinates AS (
    SELECT fact_id, COUNT(*) AS n
    FROM financial_fact_dimensions
    WHERE NOT is_default
    GROUP BY fact_id
)
SELECT
    ff.company_id,
    ff.statement,
    ff.concept,
    ff.label,
    ff.period_end,
    COUNT(*) AS facts
FROM financial_facts ff
LEFT JOIN coordinates c ON c.fact_id = ff.id
WHERE NOT EXISTS (
    SELECT 1 FROM financial_facts_normalized n WHERE n.id = ff.id
)
  AND NOT EXISTS (
    SELECT 1
    FROM financial_facts twin
    LEFT JOIN coordinates tc ON tc.fact_id = twin.id
    WHERE twin.filing_id = ff.filing_id
      AND twin.statement = ff.statement
      AND twin.concept = ff.concept
      AND twin.axis = ff.axis
      AND twin.member = ff.member
      AND twin.period_end = ff.period_end
      AND twin.period IS NOT DISTINCT FROM ff.period
      AND ff.axis <> ''
      AND COALESCE(tc.n, 0) < COALESCE(c.n, 0)
  )
GROUP BY ff.company_id, ff.statement, ff.concept, ff.label, ff.period_end
ORDER BY facts DESC
LIMIT 50;
