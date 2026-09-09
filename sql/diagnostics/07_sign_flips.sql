-- Sign instability. Normalization flips signs using the concept weight, so a
-- series that changes sign between periods usually has a weight override that
-- applies to only some of its concepts.

\echo '--- Yearly series whose sign flips between consecutive periods ---'
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
        LAG(value) OVER (
            PARTITION BY company_id, statement, normalized_label, axis, member
            ORDER BY fiscal_year
        ) AS previous_value,
        LAG(fiscal_year) OVER (
            PARTITION BY company_id, statement, normalized_label, axis, member
            ORDER BY fiscal_year
        ) AS previous_fiscal_year
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
    previous_fiscal_year,
    previous_value,
    fiscal_year,
    value
FROM signed
WHERE previous_value IS NOT NULL
  AND SIGN(value) <> SIGN(previous_value)
ORDER BY company_id, statement, normalized_label, fiscal_year
LIMIT 50;

\echo '--- Series where the same normalized label carries conflicting weights ---'
SELECT
    company_id,
    statement,
    normalized_label,
    COUNT(DISTINCT weight) AS distinct_weights,
    array_agg(DISTINCT weight) AS weights,
    array_agg(DISTINCT concept) AS concepts
FROM financial_facts_normalized
WHERE NOT is_duplicate
  AND weight IS NOT NULL
GROUP BY company_id, statement, normalized_label
HAVING COUNT(DISTINCT SIGN(weight)) > 1
ORDER BY company_id, statement, normalized_label
LIMIT 50;
