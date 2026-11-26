-- Dimension breakdowns: each row of dimension_breakdowns lists a set of
-- members on one axis that add up exactly to a parent (another member or the
-- undimensioned line) in fiscal_years. Members coarser than the parent, such
-- as millions reprinted by a later filing, may miss it by half a unit each,
-- up to 0.1%. The dimension_breakdowns stage keeps a
-- set when it adds up in at least two 10-Ks (a year where one of its members
-- could be swapped for another member of equal value does not count). A year
-- where that set falls short is not stored; the query below finds it by
-- comparing the members to yearly_financials. Several
-- breakdowns of one parent are alternatives (regions and countries), so
-- summing every member of an axis over-counts.
--
-- Fix with dimension-breakdown-overrides.csv: list a parent's breakdowns to
-- replace what was inferred for it. Confirm in the filing first.

\echo '--- Exact breakdown years that do not add up in yearly_financials (should be empty) ---'
WITH breakdown_years AS (
    SELECT b.*, fy AS fiscal_year
    FROM dimension_breakdowns b
    CROSS JOIN LATERAL unnest(b.fiscal_years) AS fy
),
rounded AS (
    SELECT
        y.*,
        CASE
            WHEN y.value = 0 THEN 1
            WHEN y.value % 1000000 = 0 THEN 1000000
            WHEN y.value % 1000 = 0 THEN 1000
            ELSE 1
        END AS rounding
    FROM yearly_financials y
),
checked AS (
    SELECT
        bd.company_id,
        bd.statement,
        bd.normalized_label,
        bd.axis,
        bd.parent_member,
        bd.members,
        bd.source,
        bd.fiscal_year,
        m.members_total,
        m.members_rounding,
        p.parent_value,
        p.parent_rounding
    FROM breakdown_years bd
    CROSS JOIN LATERAL (
        SELECT SUM(y.value) AS members_total, MAX(y.rounding) AS members_rounding
        FROM rounded y
        WHERE y.company_id = bd.company_id
          AND y.statement = bd.statement
          AND y.normalized_label = bd.normalized_label
          AND y.fiscal_year = bd.fiscal_year
          AND y.axis = bd.axis
          AND y.member = ANY(bd.members)
    ) m
    CROSS JOIN LATERAL (
        SELECT MAX(y.value) AS parent_value, MAX(y.rounding) AS parent_rounding
        FROM rounded y
        WHERE y.company_id = bd.company_id
          AND y.statement = bd.statement
          AND y.normalized_label = bd.normalized_label
          AND y.fiscal_year = bd.fiscal_year
          AND COALESCE(y.axis, '') = CASE
              WHEN bd.parent_member = '' THEN '' ELSE bd.axis
          END
          AND COALESCE(y.member, '') = bd.parent_member
    ) p
)
SELECT *
FROM checked
WHERE members_total IS NULL
   OR parent_value IS NULL
   OR ABS(members_total - parent_value) > CASE
       WHEN members_rounding > parent_rounding THEN LEAST(
           cardinality(members) * members_rounding / 2,
           ABS(parent_value) / 1000
       )
       ELSE 0
   END
ORDER BY company_id, statement, normalized_label, axis, parent_member, fiscal_year
LIMIT 50;

\echo '--- Axes with 10-K years where no exact breakdown covers the line ---'
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
            FROM dimension_breakdowns b
            WHERE b.company_id = a.company_id
              AND b.statement = a.statement
              AND b.normalized_label = a.normalized_label
              AND b.axis = a.axis
              AND b.parent_member = ''
              AND a.fiscal_year = ANY(b.fiscal_years)
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
      FROM dimension_breakdowns b
      WHERE b.company_id = o.company_id
        AND (o.normalized_label IS NULL OR b.normalized_label = o.normalized_label)
        AND b.parent_member = o.parent_member
        AND b.axis = o.axis
        -- Same set, in any order.
        AND b.members @> o.members AND b.members <@ o.members
        AND b.source = 'override'
        AND cardinality(b.fiscal_years) > 0
  )
ORDER BY o.company_id, o.axis, o.parent_member
LIMIT 50;

\echo '--- Years a known breakdown falls short: the unallocated share is often a member left off the axis ---'
WITH parents AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        fiscal_year,
        MAX(value) AS value
    FROM yearly_financials
    WHERE value IS NOT NULL
    GROUP BY company_id, statement, normalized_label, axis, member, fiscal_year
),
member_totals AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        fiscal_year,
        member,
        MAX(value) AS value
    FROM yearly_financials
    WHERE axis <> '' AND value IS NOT NULL
    GROUP BY company_id, statement, normalized_label, axis, fiscal_year, member
)
SELECT
    b.company_id,
    b.statement,
    b.normalized_label,
    b.axis,
    b.parent_member,
    b.members,
    p.fiscal_year,
    p.value AS parent_value,
    round((100 * (1 - s.total / p.value))::numeric, 1) AS unallocated_pct
FROM dimension_breakdowns b
JOIN parents p
    ON p.company_id = b.company_id
    AND p.statement = b.statement
    AND p.normalized_label = b.normalized_label
    AND p.axis = CASE WHEN b.parent_member = '' THEN '' ELSE b.axis END
    AND p.member = b.parent_member
    AND NOT (p.fiscal_year = ANY(b.fiscal_years))
CROSS JOIN LATERAL (
    SELECT COUNT(*) AS n, MIN(m.value) AS min_value, SUM(m.value) AS total
    FROM member_totals m
    WHERE m.company_id = b.company_id
      AND m.statement = b.statement
      AND m.normalized_label = b.normalized_label
      AND m.axis = b.axis
      AND m.fiscal_year = p.fiscal_year
      AND m.member = ANY(b.members)
) s
WHERE p.value > 0
  AND s.n >= 2
  AND s.min_value >= 0
  AND s.total < p.value
ORDER BY b.company_id, b.statement, b.normalized_label, b.axis, b.parent_member,
         p.fiscal_year
LIMIT 50;

\echo '--- Members the chart draws with a remainder: no breakdown the API sends adds up to the member, and one falls short ---'
-- Mirrors _breakdowns_to_show in api/financials.py: every set is sent, earlier
-- presentations included, at a granularity that reports two of its members.
-- The chart opens a member into a split wherever one of its members is
-- present, falls back to an earlier presentation where the current one does
-- not add up, and draws a shortfall no sent set closes as
-- "<member> (remainder)". A shortfall against the undimensioned line is the
-- gray metric bar instead, which partial disclosures are expected to leave.
WITH layers AS (
    SELECT 'quarterly' AS granularity, company_id, statement, normalized_label,
           axis, member, period_end, value
    FROM quarterly_financials
    WHERE value IS NOT NULL
    UNION ALL
    SELECT 'yearly', company_id, statement, normalized_label,
           axis, member, period_end, value
    FROM yearly_financials
    WHERE value IS NOT NULL
),
member_rows AS (
    SELECT b.company_id, b.statement, b.normalized_label, b.axis, b.parent_member,
           b.members, m.granularity, m.period_end, m.member, m.value
    FROM dimension_breakdowns b
    JOIN layers m
        ON m.company_id = b.company_id
        AND m.statement = b.statement
        AND m.normalized_label = b.normalized_label
        AND m.axis = b.axis
        AND m.member = ANY(b.members)
),
member_totals AS (
    SELECT company_id, statement, normalized_label, axis, parent_member, members,
           granularity, period_end, SUM(value) AS total,
           string_agg(member, '; ' ORDER BY member) AS present
    FROM member_rows
    GROUP BY company_id, statement, normalized_label, axis, parent_member, members,
             granularity, period_end
),
sent AS (
    SELECT company_id, statement, normalized_label, axis, parent_member, members,
           granularity
    FROM member_rows
    GROUP BY company_id, statement, normalized_label, axis, parent_member, members,
             granularity
    HAVING COUNT(DISTINCT member) >= 2
),
checked AS (
    SELECT t.*, p.value AS parent_value,
           ABS(t.total - p.value) <= GREATEST(1, ABS(p.value) * 0.000001) AS adds_up,
           p.value - t.total > GREATEST(1, ABS(p.value) * 0.000001) AS falls_short
    FROM member_totals t
    JOIN sent USING (company_id, statement, normalized_label, axis, parent_member,
                     members, granularity)
    JOIN layers p
        ON p.granularity = t.granularity
        AND p.company_id = t.company_id
        AND p.statement = t.statement
        AND p.normalized_label = t.normalized_label
        AND p.axis = t.axis
        AND p.member = t.parent_member
        AND p.period_end = t.period_end
    WHERE t.parent_member <> ''
),
remainders AS (
    SELECT DISTINCT ON (company_id, statement, normalized_label, axis, parent_member,
                        granularity, period_end)
        *
    FROM checked c
    WHERE falls_short
      AND NOT EXISTS (
          SELECT 1
          FROM checked x
          WHERE x.adds_up
            AND x.company_id = c.company_id
            AND x.statement = c.statement
            AND x.normalized_label = c.normalized_label
            AND x.axis = c.axis
            AND x.parent_member = c.parent_member
            AND x.granularity = c.granularity
            AND x.period_end = c.period_end
      )
    ORDER BY company_id, statement, normalized_label, axis, parent_member,
             granularity, period_end, total DESC
)
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    parent_member,
    granularity,
    COUNT(*) AS periods,
    MIN(period_end) AS first_period,
    MAX(period_end) AS last_period,
    MAX(round((100 * (1 - total / parent_value))::numeric, 1)) AS max_remainder_pct,
    (array_agg(array_to_string(members, '; ') ORDER BY period_end))[1] AS split_in_first,
    (array_agg(present ORDER BY period_end))[1] AS present_in_first
FROM remainders
GROUP BY company_id, statement, normalized_label, axis, parent_member, granularity
ORDER BY company_id, statement, normalized_label, axis, parent_member, granularity
LIMIT 50;

\echo '--- Every breakdown with the 10-K years it adds up in ---'
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    parent_member,
    members,
    source,
    fiscal_years
FROM dimension_breakdowns
ORDER BY company_id, statement, normalized_label, axis, parent_member, fiscal_years
LIMIT 50;
