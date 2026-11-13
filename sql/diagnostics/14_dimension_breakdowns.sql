-- Dimension breakdowns: each row's breakdowns column lists sets of members on
-- one axis that add up exactly to that row in that period. The row is another
-- member or the undimensioned line. refresh_financial_facts_normalized keeps a
-- set when it adds up in at least two 10-Ks and attaches it wherever it adds
-- up. Several breakdowns on one row are alternatives (regions and countries),
-- so summing every member of an axis over-counts.
--
-- Fix with dimension-breakdown-overrides.csv: list a parent's breakdowns to
-- replace what was inferred for it. Confirm in the filing first.

\echo '--- Breakdowns that do not add up in yearly_financials or quarterly_financials (should be empty) ---'
WITH breakdowns AS (
    SELECT 'yearly' AS source, y.company_id, y.statement, y.normalized_label,
           y.axis, y.member, y.period_end, y.value, b
    FROM yearly_financials y
    CROSS JOIN LATERAL jsonb_array_elements(y.breakdowns) AS b
    UNION ALL
    SELECT 'quarterly', q.company_id, q.statement, q.normalized_label,
           q.axis, q.member, q.period_end, q.value, b
    FROM quarterly_financials q
    CROSS JOIN LATERAL jsonb_array_elements(q.breakdowns) AS b
),
checked AS (
    SELECT
        bd.*,
        (
            SELECT SUM(m.value)
            FROM (
                SELECT company_id, statement, normalized_label, axis, member, period_end, value
                FROM yearly_financials
                WHERE bd.source = 'yearly'
                UNION ALL
                SELECT company_id, statement, normalized_label, axis, member, period_end, value
                FROM quarterly_financials
                WHERE bd.source = 'quarterly'
            ) m
            WHERE m.company_id = bd.company_id
              AND m.statement = bd.statement
              AND m.normalized_label = bd.normalized_label
              AND m.period_end = bd.period_end
              AND m.axis = bd.b->>'axis'
              AND m.member IN (SELECT jsonb_array_elements_text(bd.b->'members'))
        ) AS members_total
    FROM breakdowns bd
)
SELECT source, company_id, statement, normalized_label, axis, member, period_end,
       value, members_total, b AS breakdown
FROM checked
WHERE members_total IS DISTINCT FROM value
ORDER BY company_id, statement, normalized_label, period_end
LIMIT 50;

\echo '--- Axes with 10-K years where no breakdown adds up to the line: members there are not additive ---'
WITH axis_years AS (
    SELECT company_id, statement, normalized_label, axis, fiscal_year
    FROM yearly_financials
    WHERE axis <> ''
      AND value IS NOT NULL
    GROUP BY company_id, statement, normalized_label, axis, fiscal_year
    HAVING COUNT(*) >= 2
),
covered AS (
    SELECT
        a.*,
        EXISTS (
            SELECT 1
            FROM yearly_financials y
            CROSS JOIN LATERAL jsonb_array_elements(y.breakdowns) AS b
            WHERE y.company_id = a.company_id
              AND y.statement = a.statement
              AND y.normalized_label = a.normalized_label
              AND y.fiscal_year = a.fiscal_year
              AND y.axis = ''
              AND b->>'axis' = a.axis
        ) AS has_breakdown
    FROM axis_years a
)
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    COUNT(*) AS years,
    COUNT(*) FILTER (WHERE NOT has_breakdown) AS years_without_breakdown,
    MIN(fiscal_year) FILTER (WHERE NOT has_breakdown) AS first_year_without,
    MAX(fiscal_year) FILTER (WHERE NOT has_breakdown) AS last_year_without
FROM covered
GROUP BY company_id, statement, normalized_label, axis
HAVING COUNT(*) FILTER (WHERE NOT has_breakdown) > 0
ORDER BY years_without_breakdown DESC, company_id, normalized_label, axis
LIMIT 50;

\echo '--- Override breakdowns that never add up in any 10-K ---'
SELECT o.company_id, o.normalized_label, o.axis, o.parent_member, o.members
FROM dimension_breakdown_overrides o
WHERE cardinality(o.members) >= 2
  AND NOT EXISTS (
      SELECT 1
      FROM yearly_financials y
      CROSS JOIN LATERAL jsonb_array_elements(y.breakdowns) AS b
      WHERE y.company_id = o.company_id
        AND (o.normalized_label IS NULL OR y.normalized_label = o.normalized_label)
        AND y.member = o.parent_member
        AND b->>'axis' = o.axis
        AND b->'members' @> to_jsonb(o.members)
        AND jsonb_array_length(b->'members') = cardinality(o.members)
  )
ORDER BY o.company_id, o.axis, o.parent_member
LIMIT 50;

\echo '--- Every breakdown with the 10-K years it adds up in ---'
SELECT
    y.company_id,
    y.statement,
    y.normalized_label,
    b->>'axis' AS axis,
    y.member AS parent_member,
    b->'members' AS members,
    array_agg(y.fiscal_year ORDER BY y.fiscal_year) AS fiscal_years
FROM yearly_financials y
CROSS JOIN LATERAL jsonb_array_elements(y.breakdowns) AS b
GROUP BY y.company_id, y.statement, y.normalized_label, b->>'axis', y.member, b->'members'
ORDER BY y.company_id, y.statement, y.normalized_label, axis, parent_member, fiscal_years
LIMIT 50;
