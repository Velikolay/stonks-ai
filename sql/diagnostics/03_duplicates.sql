-- Duplicate and disjoint series.
-- A series is (company_id, statement, normalized_label, axis, member) over time.
-- Two concepts covering non-overlapping periods usually means a renamed concept
-- that should be merged with a concept override.

\echo '--- Rows the dedup step flagged as duplicates ---'
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    member,
    period_end,
    COUNT(*) AS duplicate_rows,
    array_agg(DISTINCT concept) AS concepts
FROM financial_facts_normalized
WHERE is_duplicate
GROUP BY company_id, statement, normalized_label, axis, member, period_end
ORDER BY duplicate_rows DESC, company_id, statement, normalized_label
LIMIT 50;

\echo '--- Disjoint series: one label carried by several concepts over separate periods ---'
WITH segments AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        concept,
        MIN(period_end) AS first_period,
        MAX(period_end) AS last_period
    FROM financial_facts_normalized
    WHERE NOT is_duplicate
      AND NOT is_abstract
    GROUP BY company_id, statement, normalized_label, axis, member, concept
)
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    member,
    COUNT(*) AS concept_count,
    array_agg(
        concept || ' [' || first_period || ' .. ' || last_period || ']'
        ORDER BY first_period
    ) AS segments
FROM segments
GROUP BY company_id, statement, normalized_label, axis, member
HAVING COUNT(*) > 1
ORDER BY concept_count DESC
LIMIT 50;

\echo '--- The reverse: one concept split across several normalized labels ---'
SELECT
    company_id,
    statement,
    concept,
    axis,
    member,
    COUNT(DISTINCT normalized_label) AS label_count,
    array_agg(DISTINCT normalized_label) AS labels
FROM financial_facts_normalized
WHERE NOT is_duplicate
GROUP BY company_id, statement, concept, axis, member
HAVING COUNT(DISTINCT normalized_label) > 1
ORDER BY label_count DESC
LIMIT 50;
