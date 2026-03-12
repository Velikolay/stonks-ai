CREATE OR REPLACE PROCEDURE refresh_yearly_financials(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE tmp_yearly_financials_new ON COMMIT DROP AS
    WITH all_filings_data AS (
        SELECT
            ff.company_id,
            ff.filing_id,
            ff.id,
            ff.parent_id,
            ff.concept,
            ff.label,
            ff.normalized_label,
            CASE
                WHEN ff.weight * FIRST_VALUE(ff.weight) OVER w < 0 THEN -1 * ff.value
                ELSE ff.value
            END AS value,
            ff.unit,
            ff.axis,
            ff.member,
            ff.statement,
            ff.period_end,
            ff.is_abstract,
            ff.is_synthetic,
            f.fiscal_year,
            FIRST_VALUE(ff.abstract_id) OVER w AS latest_abstract_id,
            FIRST_VALUE(ff.position) OVER w AS latest_position,
            FIRST_VALUE(ff.weight) OVER w AS latest_weight
        FROM financial_facts_normalized ff
        JOIN filings f
            ON ff.company_id = f.company_id
            AND ff.filing_id = f.id
        WHERE
            ff.company_id = ANY(company_ids)
            AND ff.form_type = '10-K'
            AND COALESCE(ff.is_duplicate, false) = false
        WINDOW w AS (
            PARTITION BY ff.company_id, ff.statement, ff.normalized_label, ff.axis, ff.member
            ORDER BY ff.period_end DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    )
    SELECT
        id,
        parent_id,
        company_id,
        filing_id,
        concept,
        label,
        normalized_label,
        value,
        unit,
        latest_weight AS weight,
        axis,
        member,
        statement,
        latest_abstract_id AS abstract_id,
        period_end,
        fiscal_year,
        latest_position AS position,
        is_abstract,
        is_synthetic,
        '10-K' AS source_type
    FROM all_filings_data;

    DELETE FROM yearly_financials yf
    WHERE
        yf.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_yearly_financials_new t
            WHERE t.id = yf.id
        );

    INSERT INTO yearly_financials (
        id,
        parent_id,
        company_id,
        filing_id,
        concept,
        label,
        normalized_label,
        value,
        unit,
        weight,
        axis,
        member,
        statement,
        abstract_id,
        period_end,
        fiscal_year,
        position,
        is_abstract,
        is_synthetic,
        source_type
    )
    SELECT
        id,
        parent_id,
        company_id,
        filing_id,
        concept,
        label,
        normalized_label,
        value,
        unit,
        weight,
        axis,
        member,
        statement,
        abstract_id,
        period_end,
        fiscal_year,
        position,
        is_abstract,
        is_synthetic,
        source_type
    FROM tmp_yearly_financials_new
    ON CONFLICT (id) DO UPDATE
    SET
        parent_id = EXCLUDED.parent_id,
        company_id = EXCLUDED.company_id,
        filing_id = EXCLUDED.filing_id,
        concept = EXCLUDED.concept,
        label = EXCLUDED.label,
        normalized_label = EXCLUDED.normalized_label,
        value = EXCLUDED.value,
        unit = EXCLUDED.unit,
        weight = EXCLUDED.weight,
        axis = EXCLUDED.axis,
        member = EXCLUDED.member,
        statement = EXCLUDED.statement,
        abstract_id = EXCLUDED.abstract_id,
        period_end = EXCLUDED.period_end,
        fiscal_year = EXCLUDED.fiscal_year,
        position = EXCLUDED.position,
        is_abstract = EXCLUDED.is_abstract,
        is_synthetic = EXCLUDED.is_synthetic,
        source_type = EXCLUDED.source_type;
END;
$$;
