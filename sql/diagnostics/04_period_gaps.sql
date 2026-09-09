-- Holes in otherwise continuous series. A gap in the middle of a series means a
-- concept was renamed, deduped away, or the filing was never ingested.

\echo '--- Missing fiscal years inside a yearly series ---'
WITH span AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        MIN(fiscal_year) AS first_year,
        MAX(fiscal_year) AS last_year,
        COUNT(*) AS years
    FROM yearly_financials
    WHERE NOT is_abstract
    GROUP BY company_id, statement, normalized_label, axis, member
)
SELECT
    s.company_id,
    s.statement,
    s.normalized_label,
    s.axis,
    s.member,
    y.fiscal_year AS missing_fiscal_year
FROM span s
CROSS JOIN LATERAL generate_series(s.first_year, s.last_year) AS y(fiscal_year)
WHERE s.years > 1
  AND NOT EXISTS (
      SELECT 1
      FROM yearly_financials yf
      WHERE yf.company_id = s.company_id
        AND yf.statement = s.statement
        AND yf.normalized_label = s.normalized_label
        AND yf.axis = s.axis
        AND yf.member = s.member
        AND yf.fiscal_year = y.fiscal_year
  )
ORDER BY s.company_id, s.statement, s.normalized_label, y.fiscal_year
LIMIT 100;

\echo '--- Missing quarters inside a quarterly series ---'
WITH indexed AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        fiscal_year * 4 + fiscal_quarter - 1 AS quarter_index
    FROM quarterly_financials
    WHERE NOT is_abstract
),
span AS (
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        MIN(quarter_index) AS first_quarter,
        MAX(quarter_index) AS last_quarter,
        COUNT(*) AS quarters
    FROM indexed
    GROUP BY company_id, statement, normalized_label, axis, member
)
SELECT
    s.company_id,
    s.statement,
    s.normalized_label,
    s.axis,
    s.member,
    g.quarter_index / 4 AS missing_fiscal_year,
    g.quarter_index % 4 + 1 AS missing_fiscal_quarter
FROM span s
CROSS JOIN LATERAL
    generate_series(s.first_quarter, s.last_quarter) AS g(quarter_index)
WHERE s.quarters > 1
  AND NOT EXISTS (
      SELECT 1
      FROM indexed i
      WHERE i.company_id = s.company_id
        AND i.statement = s.statement
        AND i.normalized_label = s.normalized_label
        AND i.axis = s.axis
        AND i.member = s.member
        AND i.quarter_index = g.quarter_index
  )
ORDER BY
    s.company_id,
    s.statement,
    s.normalized_label,
    missing_fiscal_year,
    missing_fiscal_quarter
LIMIT 100;
