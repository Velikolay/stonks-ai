-- Magnitude and sign anomalies inside otherwise continuous series.
--
-- 08 catches years whose four quarters do not sum to the 10-K. This file
-- catches years that *do* reconcile but still look wrong: a Q4 that absorbs a
-- YTD differencing bug, a year that spikes relative to its own series, or a
-- rare negative in an otherwise positive line. Like 10, these are candidates —
-- confirm against the filing before writing an override.
--
-- Scope matches 08: duration flows only. Balance Sheet, WA share counts, and
-- SoE begin/end labels are excluded.

\echo '--- Q4 whose sign disagrees with the other three quarters ---'
-- Classic residual dump: Q1-Q3 agree on sign, Q4 flips. Often a calculated Q4
-- from annual - YTD Q3 when one of the earlier quarters was mistagged.
WITH year_quarters AS (
    SELECT
        q.company_id,
        q.statement,
        q.normalized_label,
        q.axis,
        q.member,
        q.fiscal_year,
        q.fiscal_quarter,
        q.value,
        q.source_type,
        MAX(q.value) FILTER (WHERE q.fiscal_quarter = 4) OVER w AS q4_value,
        MAX(q.source_type) FILTER (WHERE q.fiscal_quarter = 4) OVER w AS q4_source,
        COUNT(*) FILTER (
            WHERE q.fiscal_quarter < 4 AND q.value > 0
        ) OVER w AS positive_early,
        COUNT(*) FILTER (
            WHERE q.fiscal_quarter < 4 AND q.value < 0
        ) OVER w AS negative_early,
        COUNT(*) FILTER (WHERE q.fiscal_quarter < 4) OVER w AS early_count
    FROM quarterly_financials q
    JOIN financial_facts_normalized ff ON ff.id = q.id
    WHERE NOT q.is_abstract
      AND q.value IS NOT NULL
      AND q.value <> 0
      AND q.statement <> 'Balance Sheet'
      AND ff.period IS NOT NULL
      AND q.concept NOT IN (
          'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
          'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'
      )
      AND q.normalized_label !~* 'beginning|ending balance|end of period'
    WINDOW w AS (
        PARTITION BY
            q.company_id, q.statement, q.normalized_label, q.axis, q.member,
            q.fiscal_year
    )
)
SELECT DISTINCT
    company_id,
    statement,
    normalized_label,
    axis,
    member,
    fiscal_year,
    q4_value,
    q4_source,
    positive_early,
    negative_early
FROM year_quarters
WHERE early_count = 3
  AND q4_value IS NOT NULL
  AND (
      (positive_early = 3 AND q4_value < 0)
      OR (negative_early = 3 AND q4_value > 0)
  )
ORDER BY company_id, statement, normalized_label, fiscal_year
LIMIT 50;

\echo '--- Q4 whose magnitude is an outlier vs Q1-Q3 in the same year ---'
-- |Q4| > 3x the largest of |Q1|,|Q2|,|Q3|. Catches a residual that reconciles
-- but dominates the year (and the repetitive case of that happening often).
WITH pivoted AS (
    SELECT
        q.company_id,
        q.statement,
        q.normalized_label,
        q.axis,
        q.member,
        q.fiscal_year,
        MAX(q.value) FILTER (WHERE q.fiscal_quarter = 1) AS q1,
        MAX(q.value) FILTER (WHERE q.fiscal_quarter = 2) AS q2,
        MAX(q.value) FILTER (WHERE q.fiscal_quarter = 3) AS q3,
        MAX(q.value) FILTER (WHERE q.fiscal_quarter = 4) AS q4,
        MAX(q.source_type) FILTER (WHERE q.fiscal_quarter = 4) AS q4_source
    FROM quarterly_financials q
    JOIN financial_facts_normalized ff ON ff.id = q.id
    WHERE NOT q.is_abstract
      AND q.value IS NOT NULL
      AND q.statement <> 'Balance Sheet'
      AND ff.period IS NOT NULL
      AND q.concept NOT IN (
          'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
          'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'
      )
      AND q.normalized_label !~* 'beginning|ending balance|end of period'
    GROUP BY
        q.company_id, q.statement, q.normalized_label, q.axis, q.member,
        q.fiscal_year
    HAVING COUNT(*) = 4
)
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    member,
    fiscal_year,
    q1,
    q2,
    q3,
    q4,
    q4_source,
    ROUND(
        ABS(q4) / NULLIF(GREATEST(ABS(q1), ABS(q2), ABS(q3)), 0),
        2
    ) AS q4_vs_max_early
FROM pivoted
WHERE q4 IS NOT NULL
  AND GREATEST(ABS(q1), ABS(q2), ABS(q3)) > 0
  AND ABS(q4) > 3 * GREATEST(ABS(q1), ABS(q2), ABS(q3))
ORDER BY q4_vs_max_early DESC
LIMIT 50;

\echo '--- Series with a repeating Q4 anomaly (sign flip or magnitude) across years ---'
-- Same series trips the Q4 checks in two or more fiscal years. One-offs can be
-- real seasonality; a repeat is usually a systematic YTD / calculated-Q4 bug.
WITH year_quarters AS (
    SELECT
        q.company_id,
        q.statement,
        q.normalized_label,
        q.axis,
        q.member,
        q.fiscal_year,
        MAX(q.value) FILTER (WHERE q.fiscal_quarter = 1) AS q1,
        MAX(q.value) FILTER (WHERE q.fiscal_quarter = 2) AS q2,
        MAX(q.value) FILTER (WHERE q.fiscal_quarter = 3) AS q3,
        MAX(q.value) FILTER (WHERE q.fiscal_quarter = 4) AS q4,
        COUNT(*) FILTER (
            WHERE q.fiscal_quarter < 4 AND q.value > 0
        ) AS positive_early,
        COUNT(*) FILTER (
            WHERE q.fiscal_quarter < 4 AND q.value < 0
        ) AS negative_early,
        COUNT(*) FILTER (WHERE q.fiscal_quarter < 4) AS early_count,
        COUNT(*) AS quarter_count
    FROM quarterly_financials q
    JOIN financial_facts_normalized ff ON ff.id = q.id
    WHERE NOT q.is_abstract
      AND q.value IS NOT NULL
      AND q.statement <> 'Balance Sheet'
      AND ff.period IS NOT NULL
      AND q.concept NOT IN (
          'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
          'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'
      )
      AND q.normalized_label !~* 'beginning|ending balance|end of period'
    GROUP BY
        q.company_id, q.statement, q.normalized_label, q.axis, q.member,
        q.fiscal_year
),
flagged AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        fiscal_year,
        CASE
            WHEN early_count = 3
              AND quarter_count = 4
              AND q4 IS NOT NULL
              AND q4 <> 0
              AND (
                  (positive_early = 3 AND q4 < 0)
                  OR (negative_early = 3 AND q4 > 0)
              )
            THEN 'sign'
            WHEN quarter_count = 4
              AND q4 IS NOT NULL
              AND GREATEST(ABS(q1), ABS(q2), ABS(q3)) > 0
              AND ABS(q4) > 3 * GREATEST(ABS(q1), ABS(q2), ABS(q3))
            THEN 'magnitude'
        END AS anomaly_kind
    FROM year_quarters
)
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    member,
    COUNT(*) AS anomalous_years,
    array_agg(fiscal_year ORDER BY fiscal_year) AS fiscal_years,
    array_agg(DISTINCT anomaly_kind) AS anomaly_kinds
FROM flagged
WHERE anomaly_kind IS NOT NULL
GROUP BY company_id, statement, normalized_label, axis, member
HAVING COUNT(*) >= 2
ORDER BY anomalous_years DESC, company_id, statement, normalized_label
LIMIT 50;

\echo '--- Yearly values that spike vs the rest of their series ---'
-- |value| > 5x the median absolute value of the series. Needs at least four
-- years so a short series does not flag its own first spike.
WITH base AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        fiscal_year,
        value
    FROM yearly_financials
    WHERE NOT is_abstract
      AND value IS NOT NULL
      AND value <> 0
      AND statement <> 'Balance Sheet'
),
stats AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        COUNT(*) AS years,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(value)) AS median_abs
    FROM base
    GROUP BY company_id, statement, normalized_label, axis, member
    HAVING COUNT(*) >= 4
)
SELECT
    b.company_id,
    b.statement,
    b.normalized_label,
    b.axis,
    b.member,
    b.fiscal_year,
    b.value,
    ROUND(s.median_abs::numeric, 2) AS series_median_abs,
    ROUND((ABS(b.value) / NULLIF(s.median_abs, 0))::numeric, 2) AS vs_median
FROM base b
JOIN stats s
    USING (company_id, statement, normalized_label, axis, member)
WHERE s.median_abs > 0
  AND ABS(b.value) > 5 * s.median_abs
ORDER BY vs_median DESC
LIMIT 50;

\echo '--- Rare negatives (or positives) inside an otherwise one-sided series ---'
-- A yearly point whose sign is shared by fewer than 20% of the series, with at
-- least five periods. Distinct from 07: that flags every consecutive flip; this
-- flags the odd one out in a stable series.
WITH signed AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        fiscal_year,
        concept,
        value,
        SIGN(value) AS value_sign,
        COUNT(*) OVER (
            PARTITION BY company_id, statement, normalized_label, axis, member
        ) AS years,
        COUNT(*) FILTER (WHERE value > 0) OVER (
            PARTITION BY company_id, statement, normalized_label, axis, member
        ) AS positive_years,
        COUNT(*) FILTER (WHERE value < 0) OVER (
            PARTITION BY company_id, statement, normalized_label, axis, member
        ) AS negative_years
    FROM yearly_financials
    WHERE NOT is_abstract
      AND value IS NOT NULL
      AND value <> 0
)
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    member,
    concept,
    fiscal_year,
    value,
    positive_years,
    negative_years
FROM signed
WHERE years >= 5
  AND (
      (value_sign < 0 AND negative_years::numeric / years < 0.2)
      OR (value_sign > 0 AND positive_years::numeric / years < 0.2)
  )
ORDER BY company_id, statement, normalized_label, fiscal_year
LIMIT 50;
