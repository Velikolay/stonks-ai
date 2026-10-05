-- Balance Sheet structural identities.
--
-- 05 checks every parent against its direct children. This file checks the
-- accounting identities the presentation tree is supposed to satisfy, even when
-- intermediate nodes are missing or lines hang off the wrong total:
--
--   Total Assets      = Current Assets      + Non-current Assets
--   Total Liabilities = Current Liabilities + Non-current Liabilities
--   Total Assets      = Total Liabilities   + Equity (incl. NCI / mezzanine)
--
-- Required section abstracts (Current Assets, Non-current Assets, Current
-- liabilities, Non-current Liabilities) are also reported when absent.
-- Tolerance is 0.5% of the left-hand side.

\echo '--- Total Assets <> Current + Non-current ---'
WITH yearly AS (
    SELECT
        company_id,
        fiscal_year,
        normalized_label,
        value
    FROM yearly_financials
    WHERE statement = 'Balance Sheet'
      AND axis = ''
      AND NOT is_abstract
      AND value IS NOT NULL
      AND normalized_label IN (
          'Total Assets',
          'Total Current Assets',
          'Total Non Current Assets'
      )
),
pivoted AS (
    SELECT
        company_id,
        fiscal_year,
        MAX(value) FILTER (WHERE normalized_label = 'Total Assets') AS total_assets,
        MAX(value) FILTER (WHERE normalized_label = 'Total Current Assets')
            AS current_assets,
        MAX(value) FILTER (WHERE normalized_label = 'Total Non Current Assets')
            AS noncurrent_assets
    FROM yearly
    GROUP BY company_id, fiscal_year
)
SELECT
    company_id,
    fiscal_year,
    total_assets,
    current_assets,
    noncurrent_assets,
    COALESCE(current_assets, 0) + COALESCE(noncurrent_assets, 0) AS parts_sum,
    total_assets
        - (COALESCE(current_assets, 0) + COALESCE(noncurrent_assets, 0)) AS difference
FROM pivoted
WHERE total_assets IS NOT NULL
  AND (
      current_assets IS NULL
      OR noncurrent_assets IS NULL
      OR ABS(
          total_assets
          - (COALESCE(current_assets, 0) + COALESCE(noncurrent_assets, 0))
      ) > GREATEST(ABS(total_assets) * 0.005, 1)
  )
ORDER BY ABS(
    COALESCE(
        total_assets
        - (COALESCE(current_assets, 0) + COALESCE(noncurrent_assets, 0)),
        total_assets
    )
) DESC
LIMIT 50;

\echo '--- Total Liabilities <> Current + Non-current ---'
WITH yearly AS (
    SELECT
        company_id,
        fiscal_year,
        normalized_label,
        value
    FROM yearly_financials
    WHERE statement = 'Balance Sheet'
      AND axis = ''
      AND NOT is_abstract
      AND value IS NOT NULL
      AND normalized_label IN (
          'Total Liabilities',
          'Total Current Liabilities',
          'Total Non Current Liabilities'
      )
),
pivoted AS (
    SELECT
        company_id,
        fiscal_year,
        MAX(value) FILTER (WHERE normalized_label = 'Total Liabilities')
            AS total_liabilities,
        MAX(value) FILTER (WHERE normalized_label = 'Total Current Liabilities')
            AS current_liabilities,
        MAX(value) FILTER (WHERE normalized_label = 'Total Non Current Liabilities')
            AS noncurrent_liabilities
    FROM yearly
    GROUP BY company_id, fiscal_year
)
SELECT
    company_id,
    fiscal_year,
    total_liabilities,
    current_liabilities,
    noncurrent_liabilities,
    COALESCE(current_liabilities, 0)
        + COALESCE(noncurrent_liabilities, 0) AS parts_sum,
    total_liabilities
        - (
            COALESCE(current_liabilities, 0)
            + COALESCE(noncurrent_liabilities, 0)
        ) AS difference
FROM pivoted
WHERE total_liabilities IS NOT NULL
  AND (
      current_liabilities IS NULL
      OR noncurrent_liabilities IS NULL
      OR ABS(
          total_liabilities
          - (
              COALESCE(current_liabilities, 0)
              + COALESCE(noncurrent_liabilities, 0)
          )
      ) > GREATEST(ABS(total_liabilities) * 0.005, 1)
  )
ORDER BY ABS(
    COALESCE(
        total_liabilities
        - (
            COALESCE(current_liabilities, 0)
            + COALESCE(noncurrent_liabilities, 0)
        ),
        total_liabilities
    )
) DESC
LIMIT 50;

\echo '--- Total Assets <> Total Liabilities + Equity (incl. NCI / mezzanine) ---'
-- Credit side is Prefer the BS total when present; otherwise sum Liabilities +
-- Total Equity including NCI + Redeemable NCI (mezzanine).
WITH yearly AS (
    SELECT
        company_id,
        fiscal_year,
        normalized_label,
        value
    FROM yearly_financials
    WHERE statement = 'Balance Sheet'
      AND axis = ''
      AND NOT is_abstract
      AND value IS NOT NULL
      AND normalized_label IN (
          'Total Assets',
          'Total Liabilities',
          'Total Equity including NCI',
          'Total Equity',
          'Redeemable non-controlling interests',
          'Total liabilities, redeemable non-controlling interests and equity'
      )
),
pivoted AS (
    SELECT
        company_id,
        fiscal_year,
        MAX(value) FILTER (WHERE normalized_label = 'Total Assets') AS total_assets,
        MAX(value) FILTER (WHERE normalized_label = 'Total Liabilities')
            AS total_liabilities,
        COALESCE(
            MAX(value) FILTER (
                WHERE normalized_label = 'Total Equity including NCI'
            ),
            MAX(value) FILTER (WHERE normalized_label = 'Total Equity')
        ) AS total_equity,
        COALESCE(
            MAX(value) FILTER (
                WHERE normalized_label = 'Redeemable non-controlling interests'
            ),
            0
        ) AS redeemable_nci,
        MAX(value) FILTER (
            WHERE normalized_label
                = 'Total liabilities, redeemable non-controlling interests and equity'
        ) AS l_mezz_equity
    FROM yearly
    GROUP BY company_id, fiscal_year
),
credited AS (
    SELECT
        company_id,
        fiscal_year,
        total_assets,
        COALESCE(
            l_mezz_equity,
            COALESCE(total_liabilities, 0)
                + COALESCE(total_equity, 0)
                + redeemable_nci
        ) AS credit_total,
        total_liabilities,
        total_equity,
        redeemable_nci,
        l_mezz_equity
    FROM pivoted
)
SELECT
    company_id,
    fiscal_year,
    total_assets,
    credit_total,
    total_assets - credit_total AS difference,
    total_liabilities,
    total_equity,
    redeemable_nci,
    l_mezz_equity
FROM credited
WHERE total_assets IS NOT NULL
  AND ABS(total_assets - credit_total)
      > GREATEST(ABS(total_assets) * 0.005, 1)
ORDER BY ABS(total_assets - credit_total) DESC
LIMIT 50;

\echo '--- Required Balance Sheet section abstracts missing ---'
WITH companies AS (
    SELECT DISTINCT company_id, fiscal_year
    FROM yearly_financials
    WHERE statement = 'Balance Sheet'
      AND NOT is_abstract
      AND normalized_label = 'Total Assets'
),
required(abstract_label) AS (
    VALUES
        ('Current Assets'),
        ('Non-current Assets'),
        ('Current liabilities'),
        ('Non-current Liabilities')
),
present AS (
    SELECT DISTINCT company_id, fiscal_year, normalized_label
    FROM yearly_financials
    WHERE statement = 'Balance Sheet'
      AND is_abstract
      AND normalized_label IN (
          SELECT abstract_label FROM required
      )
)
SELECT
    c.company_id,
    c.fiscal_year,
    r.abstract_label AS missing_abstract
FROM companies c
CROSS JOIN required r
LEFT JOIN present p
    ON p.company_id = c.company_id
    AND p.fiscal_year = c.fiscal_year
    AND p.normalized_label = r.abstract_label
WHERE p.normalized_label IS NULL
ORDER BY c.company_id, c.fiscal_year, r.abstract_label
LIMIT 50;
