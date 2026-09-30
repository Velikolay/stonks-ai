-- Flow metrics should reconcile: four quarters must sum to the annual figure.
-- Instant stocks (period NULL on the source fact) are excluded via join.
-- Label filter still drops SoE begin/end rows that are mis-tagged as duration.
-- Weighted-average share counts are duration but non-additive, so listed
-- explicitly. EPS stays in this check (YTD-differenced for quarterly presentation).
--
-- Tolerance is 0.5% of the annual value.

\echo '--- Fiscal years where four quarters do not sum to the 10-K figure ---'
WITH quarters AS (
    SELECT
        q.company_id,
        q.statement,
        q.normalized_label,
        q.axis,
        q.member,
        q.fiscal_year,
        SUM(q.value) AS quarterly_total,
        COUNT(*) AS quarter_count,
        array_agg(DISTINCT q.source_type) AS source_types
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
    GROUP BY q.company_id, q.statement, q.normalized_label, q.axis, q.member, q.fiscal_year
)
SELECT
    y.company_id,
    y.statement,
    y.normalized_label,
    y.axis,
    y.member,
    y.fiscal_year,
    y.value AS annual_value,
    q.quarterly_total,
    y.value - q.quarterly_total AS difference,
    q.source_types
FROM yearly_financials y
JOIN quarters q
    USING (company_id, statement, normalized_label, axis, member, fiscal_year)
WHERE NOT y.is_abstract
  AND y.value IS NOT NULL
  AND q.quarter_count = 4
  AND ABS(y.value - q.quarterly_total) > GREATEST(ABS(y.value) * 0.005, 1)
ORDER BY ABS(y.value - q.quarterly_total) DESC
LIMIT 50;

\echo '--- Fiscal years that do not have exactly four quarters ---'
SELECT
    company_id,
    statement,
    normalized_label,
    axis,
    member,
    fiscal_year,
    COUNT(*) AS quarter_count,
    array_agg(fiscal_quarter ORDER BY fiscal_quarter) AS quarters
FROM quarterly_financials
WHERE NOT is_abstract
  AND statement <> 'Balance Sheet'
GROUP BY company_id, statement, normalized_label, axis, member, fiscal_year
HAVING COUNT(*) <> 4
ORDER BY company_id, statement, normalized_label, fiscal_year
LIMIT 50;
