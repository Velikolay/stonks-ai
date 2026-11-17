-- The filing whose fiscal period a fact belongs in.
--
-- A column for an earlier year is stored on the filing that printed it.
-- Fiscal placement follows the filing that closed on that period_end, in the
-- same form family, so a 2015 10-K column for 2013 lands in fiscal 2013.
-- The source filing is the fallback when that period was never ingested.
CREATE OR REPLACE FUNCTION filing_for_period(
    p_company_id integer,
    p_source_filing_id integer,
    p_period_end date
) RETURNS integer
LANGUAGE sql
STABLE
AS $$
    SELECT COALESCE(
        (
            SELECT o.id
            FROM filings src
            JOIN filings o
                ON o.company_id = p_company_id
                AND o.fiscal_period_end = p_period_end
                AND o.form_type = CASE
                    WHEN src.form_type IN ('10-K', '10-K/A') THEN '10-K'
                    WHEN src.form_type IN ('10-Q', '10-Q/A') THEN '10-Q'
                    ELSE src.form_type
                END
            WHERE src.id = p_source_filing_id
            ORDER BY o.id
            LIMIT 1
        ),
        p_source_filing_id
    );
$$;
