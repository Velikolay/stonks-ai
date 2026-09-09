-- Flow metrics should reconcile: four quarters must sum to the annual figure.
-- Balance Sheet and share-count lines are point-in-time, so they are excluded.
--
-- Tolerance is 0.5% of the annual value.

\echo '--- Fiscal years where four quarters do not sum to the 10-K figure ---'
WITH quarters AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        fiscal_year,
        SUM(value) AS quarterly_total,
        COUNT(*) AS quarter_count,
        array_agg(DISTINCT source_type) AS source_types
    FROM quarterly_financials
    WHERE NOT is_abstract
      AND value IS NOT NULL
      AND statement <> 'Balance Sheet'
      AND normalized_label NOT ILIKE 'Shares Outstanding%'
    GROUP BY company_id, statement, normalized_label, axis, member, fiscal_year
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
