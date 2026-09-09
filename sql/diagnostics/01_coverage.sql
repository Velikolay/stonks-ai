-- Coverage: what has actually been ingested, and which periods are missing.
-- Run this first: normalization problems are often just missing filings.

\echo '--- Filings and facts per company, form and fiscal year ---'
SELECT
    c.id AS company_id,
    c.name,
    f.form_type,
    f.fiscal_year,
    COUNT(DISTINCT f.id) AS filings,
    COUNT(ff.id) AS facts
FROM companies c
JOIN filings f ON f.company_id = c.id
LEFT JOIN financial_facts ff ON ff.filing_id = f.id
GROUP BY c.id, c.name, f.form_type, f.fiscal_year
ORDER BY c.id, f.form_type, f.fiscal_year;

\echo '--- Fiscal years with no 10-K between a company''s first and last 10-K ---'
WITH span AS (
    SELECT
        company_id,
        MIN(fiscal_year) AS first_year,
        MAX(fiscal_year) AS last_year
    FROM filings
    WHERE form_type IN ('10-K', '10-K/A')
    GROUP BY company_id
)
SELECT
    s.company_id,
    y.fiscal_year AS missing_fiscal_year
FROM span s
CROSS JOIN LATERAL generate_series(s.first_year, s.last_year) AS y(fiscal_year)
WHERE NOT EXISTS (
    SELECT 1
    FROM filings f
    WHERE f.company_id = s.company_id
      AND f.form_type IN ('10-K', '10-K/A')
      AND f.fiscal_year = y.fiscal_year
)
ORDER BY s.company_id, y.fiscal_year;

\echo '--- Filings that produced no facts (parser or source data problem) ---'
SELECT
    f.company_id,
    f.number AS accession_number,
    f.form_type,
    f.fiscal_period_end,
    f.public_url
FROM filings f
LEFT JOIN financial_facts ff ON ff.filing_id = f.id
WHERE ff.id IS NULL
ORDER BY f.company_id, f.fiscal_period_end;
