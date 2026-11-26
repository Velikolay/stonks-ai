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
-- A fact filed under an axis total (OperatingSegmentsMember) beside one real
-- slice is read on that slice, as rules/facts.py on_real_slice does, and a
-- fact override's axis and member win over both.
WITH slice AS (
    SELECT d.fact_id, MIN(d.axis) AS axis, MIN(d.member) AS member,
           MIN(d.member_label) AS member_label
    FROM financial_fact_dimensions d
    WHERE NOT d.is_default
    GROUP BY d.fact_id
    HAVING COUNT(*) = 1
),
shown AS (
    SELECT
        ff.company_id,
        ff.statement,
        ff.period_end,
        COALESCE(fo.axis, mv.axis, ff.axis) AS axis,
        COALESCE(fo.member, mv.member, ff.member) AS member,
        COALESCE(fo.member_label, mv.member_label, ff.member_label) AS member_label
    FROM financial_facts ff
    LEFT JOIN financial_facts_overridden fo
        ON fo.id = ff.id AND fo.source_fact_id IS NULL
    LEFT JOIN LATERAL (
        SELECT s.axis, s.member, s.member_label
        FROM slice s
        WHERE s.fact_id = ff.id
          AND s.axis <> ff.axis
          AND split_part(s.axis, ':', 2) IN (
              'StatementBusinessSegmentsAxis', 'StatementGeographicalAxis',
              'ProductOrServiceAxis', 'SegmentsAxis', 'GeographicalAreasAxis',
              'ProductsAndServicesAxis')
          AND EXISTS (
              SELECT 1 FROM financial_fact_dimensions t
              WHERE t.fact_id = ff.id AND t.axis = ff.axis AND t.is_default)
          AND NOT EXISTS (
              SELECT 1 FROM financial_facts tw
              WHERE tw.filing_id = ff.filing_id AND tw.concept = ff.concept
                AND tw.statement = ff.statement AND tw.period_end = ff.period_end
                AND tw.period IS NOT DISTINCT FROM ff.period
                AND tw.axis = s.axis AND tw.member = s.member
                AND tw.value IS DISTINCT FROM ff.value)
    ) mv ON true
    WHERE ff.axis <> ''
)
SELECT
    sh.company_id,
    sh.statement,
    sh.axis,
    sh.member,
    sh.member_label,
    COUNT(*) AS facts,
    MAX(sh.period_end) AS latest_period
FROM shown sh
LEFT JOIN dimension_normalization dn
    ON dn.company_id = sh.company_id
    AND dn.statement = sh.statement
    AND dn.axis = sh.axis
    AND dn.member = sh.member
    AND dn.member_label = sh.member_label
WHERE dn.axis IS NULL
GROUP BY sh.company_id, sh.statement, sh.axis, sh.member, sh.member_label
ORDER BY facts DESC
LIMIT 50;
