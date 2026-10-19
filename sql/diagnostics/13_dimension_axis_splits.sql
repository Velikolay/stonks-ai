-- Dimension axis splits: the same segment member lives under different
-- normalized axes for different metrics (e.g. Revenue on Product, Operating
-- Income still on ConsolidationItemsAxis). UI filters by axis, so the second
-- metric looks undimensioned.
--
-- Fix with dimension-normalization-overrides.csv: remap the raw axis/member to
-- the shared normalized_axis_label (usually Product). Confirm the members are
-- the same economic cut before merging.

\echo '--- Same member under Product for one metric but ConsolidationItems/Segments for another ---'
WITH dimmed AS (
    SELECT
        company_id,
        normalized_label,
        axis,
        member,
        COUNT(*) AS facts
    FROM financial_facts_normalized
    WHERE NOT is_duplicate
      AND COALESCE(axis, '') <> ''
      AND COALESCE(member, '') <> ''
    GROUP BY company_id, normalized_label, axis, member
),
by_member AS (
    SELECT
        company_id,
        member,
        array_agg(DISTINCT axis ORDER BY axis) AS axes,
        array_agg(DISTINCT normalized_label ORDER BY normalized_label) AS metrics,
        SUM(facts) AS facts
    FROM dimmed
    GROUP BY company_id, member
    HAVING COUNT(DISTINCT axis) > 1
       AND bool_or(axis = 'Product')
       AND bool_or(
           axis IN (
               'srt:ConsolidationItemsAxis',
               'us-gaap:ConsolidationItemsAxis',
               'us-gaap:StatementBusinessSegmentsAxis',
               'srt:StatementBusinessSegmentsAxis'
           )
           OR axis ILIKE '%Consolidation%'
           OR axis ILIKE '%BusinessSegment%'
       )
)
SELECT *
FROM by_member
ORDER BY facts DESC, company_id, member
LIMIT 50;

\echo '--- Product-like members still on ConsolidationItems / BusinessSegments (not remapped to Product) ---'
SELECT
    company_id,
    normalized_label,
    axis,
    member,
    COUNT(*) AS facts,
    MIN(period_end) AS first_period,
    MAX(period_end) AS last_period
FROM financial_facts_normalized
WHERE NOT is_duplicate
  AND COALESCE(axis, '') <> ''
  AND (
      axis IN (
          'srt:ConsolidationItemsAxis',
          'us-gaap:ConsolidationItemsAxis',
          'us-gaap:StatementBusinessSegmentsAxis',
          'srt:StatementBusinessSegmentsAxis'
      )
      OR axis ILIKE '%Consolidation%'
      OR axis ILIKE '%BusinessSegment%'
  )
  AND member ~* '^(Mobility|Delivery|Freight|Rides|Eats|Advertising|Services|Platform|Core Platform)$'
  -- Only flag when this company already uses Product somewhere (so a remap target exists).
  AND company_id IN (
      SELECT DISTINCT company_id
      FROM financial_facts_normalized
      WHERE axis = 'Product' AND NOT is_duplicate
  )
GROUP BY company_id, normalized_label, axis, member
ORDER BY facts DESC, company_id, normalized_label, member
LIMIT 50;

\echo '--- DN: one (company, member) inferred under several normalized axes ---'
SELECT
    company_id,
    member,
    array_agg(DISTINCT normalized_axis_label ORDER BY normalized_axis_label) AS normalized_axes,
    array_agg(DISTINCT normalized_label ORDER BY normalized_label) AS metrics,
    COUNT(*) AS dn_rows
FROM dimension_normalization
WHERE COALESCE(member, '') <> ''
  AND COALESCE(normalized_axis_label, '') <> ''
GROUP BY company_id, member
HAVING COUNT(DISTINCT normalized_axis_label) > 1
ORDER BY dn_rows DESC, company_id, member
LIMIT 50;
