-- Series that should be one series but are not.
--
-- Concept and dimension normalization link a renamed concept only when an exact
-- comparative-value bridge survives every guard in the chaining step. When it
-- does not, the halves stay separate and no other diagnostic notices: 03 needs
-- them to already share a normalized_label, and 04 only finds holes inside a
-- series, not a series that simply stops.
--
-- These queries emit candidates with the evidence that supports them. They do
-- not decide. Read the filing (filings.public_url) before writing an override,
-- and record the reason in the description column of the concept CSV.

\echo '--- Series that stop early, with a candidate successor ---'
-- The loose gate: two series in the same slot that never coexist, one ending
-- where the other begins. Evidence columns rank them. successor_candidates = 1
-- means the seam is unambiguous; higher means several series start that year
-- and the filing has to settle it.
WITH series AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        MIN(fiscal_year) AS first_year,
        MAX(fiscal_year) AS last_year,
        COUNT(*) AS years,
        array_agg(DISTINCT concept) AS concepts
    FROM yearly_financials
    WHERE NOT is_abstract
    GROUP BY company_id, statement, normalized_label, axis, member
),
company_span AS (
    SELECT company_id, MAX(fiscal_year) AS latest_year
    FROM yearly_financials
    GROUP BY company_id
)
SELECT
    o.company_id,
    o.statement,
    o.axis,
    o.member,
    o.normalized_label AS retired_label,
    o.last_year AS retired_last_year,
    o.years AS retired_years,
    n.normalized_label AS successor_label,
    n.first_year AS successor_first_year,
    n.years AS successor_years,
    o.concepts && n.concepts AS shares_concept,
    EXISTS (
        SELECT 1
        FROM unnest(o.concepts) oc, unnest(n.concepts) nc
        WHERE split_part(oc, ':', 1) = split_part(nc, ':', 1)
          AND left(split_part(oc, ':', 2), 12) = left(split_part(nc, ':', 2), 12)
    ) AS similar_concept_name,
    COUNT(*) OVER (
        PARTITION BY o.company_id, o.statement, o.axis, o.member, o.normalized_label
    ) AS successor_candidates,
    o.concepts AS retired_concepts,
    n.concepts AS successor_concepts
FROM series o
JOIN series n
    ON n.company_id = o.company_id
    AND n.statement = o.statement
    AND n.axis = o.axis
    AND n.member = o.member
    AND n.normalized_label <> o.normalized_label
    AND n.first_year = o.last_year + 1
JOIN company_span cs ON cs.company_id = o.company_id
WHERE o.last_year < cs.latest_year
ORDER BY
    o.concepts && n.concepts DESC,
    EXISTS (
        SELECT 1
        FROM unnest(o.concepts) oc, unnest(n.concepts) nc
        WHERE split_part(oc, ':', 1) = split_part(nc, ':', 1)
          AND left(split_part(oc, ':', 2), 12) = left(split_part(nc, ':', 2), 12)
    ) DESC,
    o.company_id, o.statement, o.normalized_label, n.normalized_label
LIMIT 50;

\echo '--- Value bridges the chaining step did not use, and why ---'
WITH base AS (
    SELECT
        ff.company_id,
        ff.statement,
        ff.form_type,
        COALESCE(ffo.concept, ff.concept) AS concept,
        ff.value,
        ff.comparative_value,
        ff.period_end,
        ff.comparative_period_end
    FROM financial_facts ff
    LEFT JOIN financial_facts_overridden ffo
        ON ffo.id = ff.id
        AND ffo.company_id = ff.company_id
    WHERE COALESCE(ffo.axis, ff.axis) = ''
      AND NOT ff.is_abstract
),
bridge AS (
    SELECT *
    FROM base
    WHERE value IS NOT NULL
      AND value <> 0
      AND comparative_value IS NOT NULL
      AND comparative_period_end IS NOT NULL
),
labels AS (
    SELECT DISTINCT ON (company_id, statement, concept)
        company_id,
        statement,
        concept,
        normalized_label
    FROM financial_facts_normalized
    WHERE NOT is_duplicate
      AND axis = ''
    ORDER BY company_id, statement, concept, period_end DESC, normalized_label
)
SELECT DISTINCT ON (f1.company_id, f1.statement, f1.concept, f2.concept)
    f1.company_id,
    f1.statement,
    f2.concept AS earlier_concept,
    l2.normalized_label AS earlier_label,
    f1.concept AS later_concept,
    l1.normalized_label AS later_label,
    f2.period_end AS bridge_from,
    f1.period_end AS bridge_to,
    CASE
        WHEN f1.comparative_value = f2.value AND f1.form_type = f2.form_type
            THEN 'exact bridge rejected by false_matches'
        WHEN f1.comparative_value = f2.value
            THEN 'exact bridge across different form types'
        WHEN abs(f1.comparative_value) = abs(f2.value)
            THEN 'magnitudes match, signs or weights differ'
        ELSE 'values within tolerance, likely restated'
    END AS why_unlinked,
    round(
        abs(f1.comparative_value - f2.value) / abs(f2.value) * 100, 4
    ) AS relative_delta_pct,
    f2.value AS earlier_value,
    f1.comparative_value AS later_comparative_value
FROM bridge f1
JOIN bridge f2
    ON f1.company_id = f2.company_id
    AND f1.statement = f2.statement
    AND f1.comparative_period_end = f2.period_end
    AND f1.concept <> f2.concept
    AND f1.period_end > f2.period_end
    AND (
        abs(f1.comparative_value) = abs(f2.value)
        OR abs(f1.comparative_value - f2.value) <= 0.005 * abs(f2.value)
    )
JOIN labels l1
    ON l1.company_id = f1.company_id
    AND l1.statement = f1.statement
    AND l1.concept = f1.concept
JOIN labels l2
    ON l2.company_id = f2.company_id
    AND l2.statement = f2.statement
    AND l2.concept = f2.concept
WHERE l1.normalized_label <> l2.normalized_label
  AND NOT EXISTS (
      SELECT 1
      FROM base fx
      WHERE fx.company_id = f1.company_id
        AND fx.statement = f1.statement
        AND fx.period_end = f1.period_end
        AND fx.concept = f2.concept
  )
  AND NOT EXISTS (
      SELECT 1
      FROM base fx
      WHERE fx.company_id = f2.company_id
        AND fx.statement = f2.statement
        AND fx.period_end = f2.period_end
        AND fx.concept = f1.concept
  )
ORDER BY
    f1.company_id, f1.statement, f1.concept, f2.concept, f1.period_end
LIMIT 50;

\echo '--- Statement slots where exactly one concept replaced exactly one other ---'
-- Restricted to 10-K so the seam is an annual boundary. A pair only qualifies
-- when its parent lost exactly one child at that period and gained exactly one
-- at the next; anything looser is a cross product of unrelated line items.
WITH slots AS (
    SELECT
        f.company_id,
        f.statement,
        p.normalized_label AS parent_label,
        f.concept,
        f.normalized_label,
        MIN(f.period_end) AS first_period,
        MAX(f.period_end) AS last_period
    FROM financial_facts_normalized f
    JOIN financial_facts_normalized p ON p.id = f.parent_id
    WHERE NOT f.is_duplicate
      AND NOT f.is_abstract
      AND f.axis = ''
      AND f.form_type = '10-K'
    GROUP BY f.company_id, f.statement, p.normalized_label, f.concept, f.normalized_label
),
span AS (
    SELECT
        company_id,
        statement,
        MIN(first_period) AS earliest_period,
        MAX(last_period) AS latest_period
    FROM slots
    GROUP BY company_id, statement
),
retired AS (
    SELECT
        s.*,
        COUNT(*) OVER (
            PARTITION BY s.company_id, s.statement, s.parent_label, s.last_period
        ) AS siblings_retired
    FROM slots s
    JOIN span sp
        ON sp.company_id = s.company_id
        AND sp.statement = s.statement
    WHERE s.last_period < sp.latest_period
),
introduced AS (
    SELECT
        s.*,
        COUNT(*) OVER (
            PARTITION BY s.company_id, s.statement, s.parent_label, s.first_period
        ) AS siblings_introduced
    FROM slots s
    JOIN span sp
        ON sp.company_id = s.company_id
        AND sp.statement = s.statement
    WHERE s.first_period > sp.earliest_period
)
SELECT
    o.company_id,
    o.statement,
    o.parent_label,
    o.concept AS retired_concept,
    o.normalized_label AS retired_label,
    o.last_period AS retired_last_period,
    n.concept AS successor_concept,
    n.normalized_label AS successor_label,
    n.first_period AS successor_first_period
FROM retired o
JOIN introduced n
    ON n.company_id = o.company_id
    AND n.statement = o.statement
    AND n.parent_label = o.parent_label
    AND n.concept <> o.concept
    AND n.normalized_label <> o.normalized_label
    AND n.first_period > o.last_period
    AND n.first_period <= o.last_period + INTERVAL '400 days'
WHERE o.siblings_retired = 1
  AND n.siblings_introduced = 1
ORDER BY
    o.company_id, o.statement, o.parent_label, o.concept, n.concept
LIMIT 50;

\echo '--- Axes where exactly one member retired and exactly one took its place ---'
-- Same one-out-one-in rule as above, applied to members of a single axis. A
-- clean swap with the sibling set otherwise unchanged is close to conclusive;
-- an axis that gained and lost several members at once is a resegmentation and
-- is deliberately excluded.
WITH dims AS (
    SELECT
        ff.company_id,
        ff.statement,
        ff.axis,
        ff.member,
        ff.member_label,
        MIN(ff.period_end) AS first_period,
        MAX(ff.period_end) AS last_period,
        COUNT(*) AS facts
    FROM financial_facts ff
    WHERE ff.axis <> ''
      AND ff.form_type = '10-K'
    GROUP BY ff.company_id, ff.statement, ff.axis, ff.member, ff.member_label
),
labeled AS (
    SELECT
        d.*,
        COALESCE(
            (
                SELECT MAX(dn.normalized_member_label)
                FROM dimension_normalization dn
                WHERE dn.company_id = d.company_id
                  AND dn.statement = d.statement
                  AND dn.axis = d.axis
                  AND dn.member = d.member
                  AND dn.member_label = d.member_label
            ),
            d.member_label
        ) AS normalized_member_label
    FROM dims d
),
span AS (
    SELECT
        company_id,
        statement,
        axis,
        MIN(first_period) AS earliest_period,
        MAX(last_period) AS latest_period
    FROM labeled
    GROUP BY company_id, statement, axis
),
retired AS (
    SELECT
        l.*,
        COUNT(*) OVER (
            PARTITION BY l.company_id, l.statement, l.axis, l.last_period
        ) AS siblings_retired
    FROM labeled l
    JOIN span sp
        ON sp.company_id = l.company_id
        AND sp.statement = l.statement
        AND sp.axis = l.axis
    WHERE l.last_period < sp.latest_period
),
introduced AS (
    SELECT
        l.*,
        COUNT(*) OVER (
            PARTITION BY l.company_id, l.statement, l.axis, l.first_period
        ) AS siblings_introduced
    FROM labeled l
    JOIN span sp
        ON sp.company_id = l.company_id
        AND sp.statement = l.statement
        AND sp.axis = l.axis
    WHERE l.first_period > sp.earliest_period
)
SELECT
    o.company_id,
    o.statement,
    o.axis,
    o.member AS retired_member,
    o.normalized_member_label AS retired_member_label,
    o.last_period AS retired_last_period,
    o.facts AS retired_facts,
    n.member AS successor_member,
    n.normalized_member_label AS successor_member_label,
    n.first_period AS successor_first_period,
    n.facts AS successor_facts
FROM retired o
JOIN introduced n
    ON n.company_id = o.company_id
    AND n.statement = o.statement
    AND n.axis = o.axis
    AND (n.member, n.member_label) IS DISTINCT FROM (o.member, o.member_label)
    AND n.normalized_member_label <> o.normalized_member_label
    AND n.first_period > o.last_period
    AND n.first_period <= o.last_period + INTERVAL '400 days'
WHERE o.siblings_retired = 1
  AND n.siblings_introduced = 1
ORDER BY
    o.company_id, o.statement, o.axis, o.member, n.member
LIMIT 50;
