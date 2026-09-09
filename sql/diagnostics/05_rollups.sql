-- Hierarchy arithmetic: a parent should equal the sum of its children.
-- A mismatch usually means a missing child, a wrong weight, or a line item
-- attached to the wrong parent_concept.
--
-- A child contributes value * weight, matching the synthetic_rollup CTE in
-- refresh_financial_facts_normalized. So a deduction such as Cost of Revenue is
-- stored positive with weight -1. A child that is stored already-negative *and*
-- carries weight -1 is double-counted, and shows up here.
--
-- Tolerance is 0.5% of the parent, so rounding in the source filing is ignored.

\echo '--- Yearly parents that do not equal the sum of their children ---'
WITH children AS (
    SELECT
        parent_id,
        SUM(value * COALESCE(weight, 1)) AS child_total,
        COUNT(*) AS child_count
    FROM yearly_financials
    WHERE parent_id IS NOT NULL
      AND NOT is_abstract
      AND value IS NOT NULL
    GROUP BY parent_id
)
SELECT
    p.company_id,
    p.statement,
    p.normalized_label,
    p.fiscal_year,
    p.value AS parent_value,
    c.child_total,
    p.value - c.child_total AS difference,
    c.child_count
FROM yearly_financials p
JOIN children c ON c.parent_id = p.id
WHERE NOT p.is_abstract
  AND p.value IS NOT NULL
  AND ABS(p.value - c.child_total) > GREATEST(ABS(p.value) * 0.005, 1)
ORDER BY ABS(p.value - c.child_total) DESC
LIMIT 50;

\echo '--- Synthetic rollup rows that summed to zero (children never attached) ---'
SELECT
    company_id,
    statement,
    normalized_label,
    fiscal_year,
    value
FROM yearly_financials
WHERE is_synthetic
  AND NOT is_abstract
  AND COALESCE(value, 0) = 0
ORDER BY company_id, statement, normalized_label, fiscal_year
LIMIT 50;
