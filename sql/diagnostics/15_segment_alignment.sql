-- Segment alignment: does every segment line split the way revenue does, and
-- does the chart get a split for every year that has one?
--
-- Segment notes report revenue, costs and profit for the same segments, but
-- filers tag them on different axes (StatementBusinessSegmentsAxis for
-- revenue, ConsolidationItemsAxis = OperatingSegmentsMember beside it for
-- operating income, regions on StatementGeographicalAxis). The UI filters by
-- one axis, so a line on another axis looks unsplit. Fix with
-- dimension-normalization-overrides.csv rows keyed on the segment member's
-- qname on the axis the fact is read on, mapped to revenue's normalized axis.
-- Put the corporate / unallocated / elimination member on that axis too, or
-- the segments never add up to the line.

\echo '--- Segment members a line reports on another axis than revenue ---'
-- A member label revenue uses on one normalized axis, used by another
-- Income Statement line on a different one.
-- Only segment-type axes: legal entities, accounting-change columns and tax
-- authorities reuse member names without being the segment cut.
WITH dimmed AS (
    SELECT DISTINCT company_id, normalized_label, axis, member
    FROM yearly_financials
    WHERE statement = 'Income Statement' AND axis <> '' AND member <> ''
      AND (
          axis IN ('Product', 'Geographical', 'Segment')
          OR axis ~* '(BusinessSegments|ConsolidationItems|ProductOrService|Geographical|SegmentsAxis)'
      )
),
revenue AS (
    SELECT company_id, axis AS revenue_axis, member
    FROM dimmed
    WHERE normalized_label = 'Revenue'
)
SELECT
    d.company_id,
    d.normalized_label,
    d.member,
    d.axis AS line_axis,
    array_agg(DISTINCT r.revenue_axis ORDER BY r.revenue_axis) AS revenue_axes
FROM dimmed d
JOIN revenue r ON r.company_id = d.company_id AND lower(r.member) = lower(d.member)
WHERE d.normalized_label <> 'Revenue'
  AND NOT EXISTS (
      SELECT 1 FROM revenue r2
      WHERE r2.company_id = d.company_id
        AND lower(r2.member) = lower(d.member)
        AND r2.revenue_axis = d.axis
  )
GROUP BY d.company_id, d.normalized_label, d.member, d.axis
ORDER BY d.company_id, d.normalized_label, d.member
LIMIT 50;

\echo '--- Profit and cost lines with members on a revenue segment axis but no breakdown there ---'
-- Revenue's axes are those with a breakdown of the revenue line. A line with
-- two or more members on that axis and no breakdown is missing its
-- corporate / unallocated / elimination member, or the segments are profit
-- before unallocated costs (expected; explain it).
WITH revenue_axes AS (
    SELECT DISTINCT company_id, axis
    FROM dimension_breakdowns
    WHERE statement = 'Income Statement' AND normalized_label = 'Revenue'
      AND parent_member = ''
      AND (
          axis IN ('Product', 'Geographical', 'Segment')
          OR axis ~* '(BusinessSegments|ConsolidationItems|ProductOrService|Geographical|SegmentsAxis)'
      )
),
lines AS (
    SELECT y.company_id, y.normalized_label, y.axis,
           COUNT(DISTINCT y.member) AS members,
           COUNT(DISTINCT y.fiscal_year) AS years,
           MIN(y.fiscal_year) AS first_year,
           MAX(y.fiscal_year) AS last_year
    FROM yearly_financials y
    JOIN revenue_axes ra ON ra.company_id = y.company_id AND ra.axis = y.axis
    WHERE y.statement = 'Income Statement'
      AND y.normalized_label IN (
          'Cost of Revenue', 'Gross Profit', 'Operating Expenses',
          'Operating Income', 'Research and development',
          'Selling, general and administrative'
      )
    GROUP BY y.company_id, y.normalized_label, y.axis
    HAVING COUNT(DISTINCT y.member) >= 2
)
SELECT l.*
FROM lines l
WHERE NOT EXISTS (
    SELECT 1 FROM dimension_breakdowns b
    WHERE b.company_id = l.company_id
      AND b.statement = 'Income Statement'
      AND b.normalized_label = l.normalized_label
      AND b.axis = l.axis
      AND b.parent_member = ''
)
ORDER BY l.company_id, l.normalized_label, l.axis
LIMIT 50;

\echo '--- Segment amounts still shown under an axis total ---'
-- "Operating Segments - X" on ConsolidationItemsAxis: the fact has a real
-- slice the refresh did not move to (two real slices, or the filing tags the
-- slot on its own with another value). Map it with a member_label override
-- onto revenue's axis, or leave it when the value is a different measure.
SELECT
    company_id,
    normalized_label,
    axis,
    member,
    COUNT(*) AS rows,
    MIN(fiscal_year) AS first_year,
    MAX(fiscal_year) AS last_year
FROM yearly_financials
WHERE statement = 'Income Statement'
  AND axis ILIKE '%ConsolidationItemsAxis'
  AND member ~* '^operating segments? - '
GROUP BY company_id, normalized_label, axis, member
ORDER BY rows DESC, company_id
LIMIT 50;

\echo '--- Breakdowns that are earlier presentations ---'
-- Sets that share no year with the sets reaching the axis's latest year. The
-- chart draws their years on them when the current split does not add up
-- (see Breakdowns in AGENTS.md). Informational: check that each one is a
-- real older structure (AMD's Computing and Graphics, NVIDIA's GPU and
-- Tegra), not a split that only stopped adding up.
WITH sets AS (
    SELECT company_id, statement, normalized_label, axis, members, fiscal_years
    FROM dimension_breakdowns
    WHERE parent_member = ''
),
latest AS (
    SELECT company_id, statement, normalized_label, axis,
           MAX(y) AS latest_year
    FROM sets, unnest(fiscal_years) AS y
    GROUP BY company_id, statement, normalized_label, axis
),
current_years AS (
    SELECT s.company_id, s.statement, s.normalized_label, s.axis,
           array_agg(DISTINCT y) AS years
    FROM sets s
    JOIN latest l USING (company_id, statement, normalized_label, axis)
    CROSS JOIN unnest(s.fiscal_years) AS y
    WHERE l.latest_year = ANY (s.fiscal_years)
    GROUP BY s.company_id, s.statement, s.normalized_label, s.axis
)
SELECT
    s.company_id,
    s.statement,
    s.normalized_label,
    s.axis,
    s.members,
    s.fiscal_years AS hidden_years
FROM sets s
JOIN current_years c USING (company_id, statement, normalized_label, axis)
WHERE NOT (s.fiscal_years && c.years)
ORDER BY s.company_id, s.statement, s.normalized_label, s.axis
LIMIT 50;
