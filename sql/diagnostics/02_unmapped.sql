-- Concepts and dimensions that never acquired a normalized label.
-- These are the highest-leverage override candidates: fix the most frequent first.

\echo '--- Concepts with no row in concept_normalization, by fact count ---'
SELECT
    ff.company_id,
    ff.statement,
    ff.concept,
    COUNT(*) AS facts,
    MIN(ff.label) AS sample_label,
    MAX(ff.period_end) AS latest_period
FROM financial_facts ff
LEFT JOIN concept_normalization cn
    ON cn.company_id = ff.company_id
    AND cn.statement = ff.statement
    AND cn.concept = ff.concept
WHERE cn.concept IS NULL
  AND NOT ff.is_abstract
GROUP BY ff.company_id, ff.statement, ff.concept
ORDER BY facts DESC
LIMIT 50;

\echo '--- One concept reported under several labels (candidate relabels) ---'
SELECT
    company_id,
    statement,
    concept,
    COUNT(DISTINCT label) AS distinct_labels,
    array_agg(DISTINCT label) AS labels
FROM financial_facts
WHERE axis = ''
GROUP BY company_id, statement, concept
HAVING COUNT(DISTINCT label) > 1
ORDER BY distinct_labels DESC
LIMIT 50;

\echo '--- Dimensional facts with no row in dimension_normalization ---'
SELECT
    ff.company_id,
    ff.statement,
    ff.axis,
    ff.member,
    ff.member_label,
    COUNT(*) AS facts,
    MAX(ff.period_end) AS latest_period
FROM financial_facts ff
LEFT JOIN dimension_normalization dn
    ON dn.company_id = ff.company_id
    AND dn.statement = ff.statement
    AND dn.axis = ff.axis
    AND dn.member = ff.member
    AND dn.member_label = ff.member_label
WHERE ff.axis <> ''
  AND dn.axis IS NULL
GROUP BY ff.company_id, ff.statement, ff.axis, ff.member, ff.member_label
ORDER BY facts DESC
LIMIT 50;
