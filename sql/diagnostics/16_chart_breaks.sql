-- Periods a reader of the chart would call wrong: one period whose members
-- cover a different share of the line than the periods around it, a member
-- that is positive around one period and negative in it, and two members that
-- carry the same values under two names.
--
-- 04, 08, 11 and 14 see the same defects, but among every seasonal Q4, sparse
-- member and partial disclosure, so a single broken quarter does not stand
-- out there. These queries compare a period with its own neighbours, need no
-- breakdown (a defect often stops inference from finding one), and should
-- list nothing that is not explained. Run them per company:
--   make diagnose COMPANY_IDS=4 FILES=16
--
-- Only member axes a chart stacks are read: axes an override named (Product,
-- Geographical, Technology, no namespace) and segment, product or region axes. Per-share amounts and share counts are
-- left out.
--
-- The usual causes, in the order to check them:
--   * a member missing in one period: the filing named it differently
--     (Microsoft's FY2026 10-K files Gaming as XBOX; the third query lists
--     the pair), or a fact override bounded only by period caught a restated
--     reprint and the slot dropped out of dimension_normalization (Alphabet's
--     overrides 63 and 65 before they were bounded by filing period);
--   * a member missing or negative in Q4 only: the 10-K split differs from
--     its quarters' split, so Q4 subtracts nine months on another basis
--     (Microsoft's FY2023 10-K carving Dynamics out of Other);
--   * members over the line in one period: two splits mixed, a later filing's
--     new members filling a period its own filing split another way. When
--     one later filing printed every new member and covers the line on its
--     own as the neighbours do, the period carries two complete presentations
--     and is not listed: the chart draws the later one (Microsoft's fiscal
--     2027 10-Q reprinting September 2025 on new segments).

\echo '--- Periods whose members cover a different share of the line than the periods around them ---'
-- coverage = the sum of every member on the axis / the line. Summing every
-- member over-counts an axis with subtotals or two splits, but by the same
-- factor every period, so coverage is flat across a stable presentation and
-- steps when the presentation changes. A one-period spike is a member
-- missing, negative or doubled there: coverage before and after agree within
-- 2 points, this period is more than one point off and three times further
-- from them than they are from each other, and its members differ from the
-- period before or one is negative. At the line's latest period the two
-- periods before it stand in for the neighbours. A gap of two periods running
-- into a re-segmentation does not spike; 14's remainder query finds those.
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
scoped AS (
    SELECT DISTINCT granularity, company_id, statement, normalized_label, axis,
           member, period_end, value
    FROM layers
    WHERE normalized_label !~* 'per share|EPS|shares'
      AND statement <> 'Balance Sheet'
),
lines AS (
    SELECT granularity, company_id, statement, normalized_label, period_end,
           MAX(value) AS value
    FROM scoped
    WHERE axis = ''
    GROUP BY granularity, company_id, statement, normalized_label, period_end
),
members AS (
    SELECT granularity, company_id, statement, normalized_label, axis,
           period_end, SUM(value) AS total,
           array_agg(member ORDER BY member) AS present,
           array_agg(member ORDER BY member) FILTER (WHERE value < 0) AS negatives
    FROM scoped
    WHERE axis <> ''
      AND (axis NOT LIKE '%:%'
           OR axis ~* 'segment|product|geograph|region')
    GROUP BY granularity, company_id, statement, normalized_label, axis,
             period_end
),
spans AS (
    SELECT granularity, company_id, statement, normalized_label, axis,
           MIN(period_end) AS first_period, MAX(period_end) AS last_period
    FROM members
    GROUP BY granularity, company_id, statement, normalized_label, axis
),
coverage AS (
    SELECT s.granularity, s.company_id, s.statement, s.normalized_label, s.axis,
           l.period_end, l.value AS line_value,
           COALESCE(m.total, 0) AS members_total,
           COALESCE(m.present, '{}') AS present,
           m.negatives,
           COALESCE(m.total, 0) / l.value AS coverage
    FROM spans s
    JOIN lines l
        ON l.granularity = s.granularity
        AND l.company_id = s.company_id
        AND l.statement = s.statement
        AND l.normalized_label = s.normalized_label
        AND l.period_end BETWEEN s.first_period AND s.last_period
    LEFT JOIN members m
        ON m.granularity = s.granularity
        AND m.company_id = s.company_id
        AND m.statement = s.statement
        AND m.normalized_label = s.normalized_label
        AND m.axis = s.axis
        AND m.period_end = l.period_end
    WHERE l.value <> 0
),
neighbours AS (
    SELECT c.*,
           LAG(coverage) OVER w AS before,
           LAG(coverage, 2) OVER w AS before_2,
           LEAD(coverage) OVER w AS after,
           LAG(present) OVER w AS present_before
    FROM coverage c
    WINDOW w AS (
        PARTITION BY granularity, company_id, statement, normalized_label, axis
        ORDER BY period_end
    )
),
flagged AS (
    SELECT nb.*,
           ARRAY(
               SELECT unnest(present_before) EXCEPT SELECT unnest(present)
           ) AS missing,
           ARRAY(
               SELECT unnest(present) EXCEPT SELECT unnest(present_before)
           ) AS added,
           -- A segment can lose money; a revenue or cost member cannot.
           negatives IS NOT NULL
               AND normalized_label !~* 'income|profit|loss|margin|earnings|tax'
               AS turned_negative
    FROM neighbours nb
    WHERE before IS NOT NULL
      -- Neighbours without members are a sparse series, not a stable cover.
      AND before <> 0
      AND COALESCE(after, before_2) <> 0
      AND ABS(coverage - before) > 0.01
      AND ABS(coverage - before)
          > 3 * ABS(COALESCE(after, before_2) - before)
      AND ABS(COALESCE(after, before_2) - before) <= 0.02
      AND (after IS NOT NULL
           OR ABS(coverage - before) > 3 * ABS(before - before_2))
)
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    granularity,
    period_end,
    line_value,
    members_total,
    round((100 * before)::numeric, 1) AS coverage_before_pct,
    round((100 * coverage)::numeric, 1) AS coverage_pct,
    round((100 * COALESCE(after, before_2))::numeric, 1) AS coverage_around_pct,
    array_to_string(missing, '; ') AS missing_members,
    array_to_string(added, '; ') AS new_members,
    array_to_string(negatives, '; ') AS negative_members
FROM flagged f
-- Partial disclosures wobble with rounding; a defect turns a revenue or cost
-- member negative or changes the members. Members only added are a later
-- filing's new presentation beside the period's own when one filing printed
-- every new member and its members alone cover the line as the neighbours
-- do. The chart draws one presentation (later split wins), so that is no
-- break (Microsoft's fiscal 2027 first-quarter 10-Q reprinting September 2025
-- on new segments and product lines).
WHERE f.turned_negative
   OR cardinality(f.missing) > 0
   OR (cardinality(f.added) > 0 AND NOT EXISTS (
      SELECT 1
      FROM (
          SELECT DISTINCT n.filing_id, n.period, n.member, n.value
          FROM financial_facts_normalized n
          WHERE n.company_id = f.company_id
            AND n.statement = f.statement
            AND n.normalized_label = f.normalized_label
            AND n.axis = f.axis
            AND n.period_end = f.period_end
            AND NOT n.is_abstract
      ) printed
      GROUP BY printed.filing_id, printed.period
      HAVING f.added <@ array_agg(printed.member)
         AND ABS(SUM(printed.value) / f.line_value - f.before) <= 0.01
   ))
ORDER BY company_id, statement, normalized_label, axis, granularity, period_end
LIMIT 50;

\echo '--- Members that turn negative between positive periods ---'
-- A member positive in the period before and after (or before the latest) is
-- negative here. Corporate, unallocated, reconciling, elimination and hedging
-- members change sign by nature, and a segment can lose money, so those
-- members and profit, income and tax lines are left out. A negative calculated Q4 is
-- usually the 10-K and its quarters on two splits.
WITH series AS (
    SELECT 'quarterly' AS granularity, company_id, statement, normalized_label,
           axis, member, period_end, value, source_type
    FROM quarterly_financials
    WHERE value IS NOT NULL
    UNION ALL
    SELECT 'yearly', company_id, statement, normalized_label, axis, member,
           period_end, value, NULL
    FROM yearly_financials
    WHERE value IS NOT NULL
),
scoped AS (
    SELECT DISTINCT ON (granularity, company_id, statement, normalized_label,
                        axis, member, period_end) *
    FROM series
    WHERE axis <> ''
      AND (axis NOT LIKE '%:%'
           OR axis ~* 'segment|product|geograph|region')
      AND statement <> 'Balance Sheet'
      AND normalized_label !~* 'per share|EPS|shares'
      AND member !~* 'corporate|unallocated|reconcil|eliminat|hedg|intersegment'
      AND normalized_label !~* 'income|profit|loss|margin|earnings|tax'
    ORDER BY granularity, company_id, statement, normalized_label, axis, member,
             period_end, value
),
ordered AS (
    SELECT s.*,
           LAG(value) OVER w AS value_before,
           LEAD(value) OVER w AS value_after,
           LEAD(period_end) OVER w AS next_period
    FROM scoped s
    WINDOW w AS (
        PARTITION BY granularity, company_id, statement, normalized_label, axis,
                     member
        ORDER BY period_end
    )
)
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    member,
    granularity,
    period_end,
    value,
    value_before,
    value_after,
    source_type
FROM ordered
WHERE value < 0
  AND value_before > 0
  AND (value_after > 0 OR next_period IS NULL)
ORDER BY company_id, statement, normalized_label, axis, member, granularity,
         period_end
LIMIT 50;

\echo '--- Members that carry the values of another member: a rename the labels did not merge ---'
-- Two members of one line and axis that agree (within 0.1%) in every 10-K
-- year both report, with different spans, so one carries on where the other
-- stops. A later 10-K reprinting its comparatives under a new name produces
-- this (Microsoft's FY2026 10-K: XBOX equals Gaming for fiscal 2024-2025).
-- Confirm the rename in the filing, then give the facts one member: a fact
-- override bounded by filing period, or a dimension override when the old name
-- has no other meaning (see the grouping note in AGENTS.md). A subtotal with a
-- single child also matches, and two members that coincide in one year can;
-- leave those.
WITH yearly AS (
    SELECT company_id, statement, normalized_label, axis, member, fiscal_year,
           MAX(value) AS value
    FROM yearly_financials
    WHERE axis <> ''
      AND value IS NOT NULL
      AND value <> 0
      AND (axis NOT LIKE '%:%'
           OR axis ~* 'segment|product|geograph|region')
      AND statement <> 'Balance Sheet'
      AND normalized_label !~* 'per share|EPS|shares'
    GROUP BY company_id, statement, normalized_label, axis, member, fiscal_year
),
spans AS (
    SELECT company_id, statement, normalized_label, axis, member,
           MIN(fiscal_year) AS first_year, MAX(fiscal_year) AS last_year
    FROM yearly
    GROUP BY company_id, statement, normalized_label, axis, member
),
pairs AS (
    SELECT a.company_id, a.statement, a.normalized_label, a.axis,
           a.member AS member_a, b.member AS member_b,
           array_agg(a.fiscal_year ORDER BY a.fiscal_year) AS years,
           bool_and(ABS(a.value - b.value)
                    <= GREATEST(1, ABS(a.value) * 0.001)) AS equal
    FROM yearly a
    JOIN yearly b
        ON b.company_id = a.company_id
        AND b.statement = a.statement
        AND b.normalized_label = a.normalized_label
        AND b.axis = a.axis
        AND b.fiscal_year = a.fiscal_year
        AND b.member > a.member
    GROUP BY a.company_id, a.statement, a.normalized_label, a.axis,
             a.member, b.member
)
SELECT
    p.company_id,
    p.statement,
    p.normalized_label,
    p.axis,
    p.member_a,
    sa.first_year || '-' || sa.last_year AS span_a,
    p.member_b,
    sb.first_year || '-' || sb.last_year AS span_b,
    array_to_string(p.years, '; ') AS equal_years
FROM pairs p
JOIN spans sa
    ON sa.company_id = p.company_id AND sa.statement = p.statement
    AND sa.normalized_label = p.normalized_label AND sa.axis = p.axis
    AND sa.member = p.member_a
JOIN spans sb
    ON sb.company_id = p.company_id AND sb.statement = p.statement
    AND sb.normalized_label = p.normalized_label AND sb.axis = p.axis
    AND sb.member = p.member_b
WHERE p.equal
  AND (sa.first_year, sa.last_year) <> (sb.first_year, sb.last_year)
ORDER BY p.company_id, p.statement, p.normalized_label, p.axis, p.member_a
LIMIT 50;

\echo '--- Distinct members of one filing merged into one label ---'
-- One filing reports two or more qnames at different values for one period,
-- and they land on the same normalized member, so all but one value is lost
-- and the axis cannot add up. Label grouping chains members that share a
-- label; a filing whose label linkbase crosses its labels (AMD's FY2024
-- 10-K labelling country:JP "China") collapses a whole axis onto one member.
-- Rewrite that filing's facts onto their own labels, bounded by filing period.
WITH printed AS (
    SELECT n.company_id, n.statement, n.normalized_label, n.axis, n.member,
           n.period_end, n.period, n.filing_id,
           COALESCE(o.member, ff.member) AS qname, n.value
    FROM financial_facts_normalized n
    JOIN financial_facts ff ON ff.id = n.id
    LEFT JOIN financial_facts_overridden o
        ON o.id = n.id AND o.source_fact_id IS NULL
    WHERE n.axis <> ''
      AND NOT n.is_abstract
      AND n.value IS NOT NULL
      AND n.statement <> 'Balance Sheet'
      AND (n.axis NOT LIKE '%:%' OR n.axis ~* 'segment|product|geograph|region')
),
merged AS (
    SELECT company_id, statement, normalized_label, axis, member, period_end,
           filing_id,
           array_agg(DISTINCT qname ORDER BY qname) AS qnames
    FROM printed
    GROUP BY company_id, statement, normalized_label, axis, member, period_end,
             period, filing_id
    HAVING COUNT(DISTINCT qname) > 1 AND COUNT(DISTINCT value) > 1
)
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    member,
    COUNT(DISTINCT filing_id) AS filings,
    MIN(period_end) AS first_period,
    MAX(period_end) AS last_period,
    (array_agg(array_to_string(qnames, '; ') ORDER BY period_end DESC))[1]
        AS qnames_in_latest
FROM merged
GROUP BY company_id, statement, normalized_label, axis, member
ORDER BY company_id, statement, normalized_label, axis, member
LIMIT 50;
