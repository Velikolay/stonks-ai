-- Dangling hierarchy pointers. parent_id and abstract_id may be synthetic hashes,
-- so a pointer with no matching row means the synthetic parent was never created
-- or was dropped by dedup.

\echo '--- Normalized facts whose parent_id has no matching row ---'
SELECT
    ff.company_id,
    ff.statement,
    ff.concept,
    ff.normalized_label,
    ff.period_end,
    ff.parent_id
FROM financial_facts_normalized ff
WHERE ff.parent_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM financial_facts_normalized p WHERE p.id = ff.parent_id
  )
ORDER BY ff.company_id, ff.statement, ff.period_end
LIMIT 50;

\echo '--- Normalized facts whose abstract_id has no matching row ---'
SELECT
    ff.company_id,
    ff.statement,
    ff.concept,
    ff.normalized_label,
    ff.period_end,
    ff.abstract_id
FROM financial_facts_normalized ff
WHERE ff.abstract_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM financial_facts_normalized a WHERE a.id = ff.abstract_id
  )
ORDER BY ff.company_id, ff.statement, ff.period_end
LIMIT 50;

\echo '--- Yearly rows whose parent was dropped from the yearly table ---'
SELECT
    yf.company_id,
    yf.statement,
    yf.normalized_label,
    yf.fiscal_year,
    yf.parent_id
FROM yearly_financials yf
WHERE yf.parent_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM yearly_financials p WHERE p.id = yf.parent_id)
ORDER BY yf.company_id, yf.statement, yf.fiscal_year
LIMIT 50;

\echo '--- Quarterly rows whose parent was dropped from the quarterly table ---'
SELECT
    qf.company_id,
    qf.statement,
    qf.normalized_label,
    qf.fiscal_year,
    qf.fiscal_quarter,
    qf.parent_id
FROM quarterly_financials qf
WHERE qf.parent_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM quarterly_financials p WHERE p.id = qf.parent_id)
ORDER BY qf.company_id, qf.statement, qf.fiscal_year, qf.fiscal_quarter
LIMIT 50;
