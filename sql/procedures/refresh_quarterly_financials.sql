CREATE OR REPLACE PROCEDURE refresh_quarterly_financials(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE tmp_quarterly_financials_new ON COMMIT DROP AS
    WITH ordered_filings AS (
        SELECT
            f.*,
            ROW_NUMBER() OVER (
                PARTITION BY company_id
                ORDER BY fiscal_period_end, id
            ) AS seq
        FROM filings f
        WHERE
            f.company_id = ANY(company_ids)
            AND f.form_type IN ('10-K', '10-K/A', '10-Q')
    ),
    filings_cte AS (
        SELECT
            o.id,
            o.company_id,
            o.fiscal_year,
            o.fiscal_quarter,
            CASE
                WHEN o.form_type IN ('10-K', '10-K/A') THEN o.id
                ELSE (
                    SELECT k.id
                    FROM ordered_filings k
                    WHERE k.company_id = o.company_id
                        AND k.form_type IN ('10-K', '10-K/A')
                        AND k.seq > o.seq
                    ORDER BY k.seq
                    LIMIT 1
                )
            END AS fiscal_tag
        FROM ordered_filings o
    ),
    all_filings_data AS (
        SELECT
            ff.company_id,
            ff.filing_id,
            ff.id,
            ff.parent_id,
            ff.label,
            ff.normalized_label,
            CASE
                WHEN ff.weight * FIRST_VALUE(ff.weight) OVER w < 0 THEN -1 * ff.value
                ELSE ff.value
            END AS value,
            ff.unit,
            ff.statement,
            ff.concept,
            ff.axis,
            ff.member,
            ff.period_end,
            ff.period,
            ff.is_abstract,
            ff.is_synthetic,
            ff.form_type AS source_type,
            f.fiscal_year,
            f.fiscal_quarter,
            f.fiscal_tag,
            FIRST_VALUE(ff.abstract_id) OVER w AS latest_abstract_id,
            FIRST_VALUE(ff.position) OVER w AS latest_position,
            FIRST_VALUE(ff.weight) OVER w AS latest_weight
        FROM financial_facts_normalized ff
        JOIN filings_cte f
            ON ff.company_id = f.company_id
            AND ff.filing_id = f.id
        WHERE ff.company_id = ANY(company_ids)
          AND ff.is_duplicate = false
        WINDOW w AS (
            PARTITION BY ff.company_id, ff.statement, ff.normalized_label, ff.axis, ff.member
            ORDER BY ff.period_end DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ),
    quarterly_filings_raw AS (
        SELECT
            company_id,
            filing_id,
            id,
            parent_id,
            fiscal_year,
            fiscal_quarter,
            fiscal_tag,
            label,
            normalized_label,
            value,
            unit,
            statement,
            concept,
            axis,
            member,
            latest_abstract_id AS abstract_id,
            latest_weight AS weight,
            latest_position AS position,
            period_end,
            period,
            is_abstract,
            is_synthetic,
            source_type
        FROM all_filings_data
        WHERE source_type = '10-Q'
    ),
    quarterly_filings_with_prev AS (
        SELECT
            q.*,
            CASE
                WHEN (period_end - LAG(period_end) OVER w) BETWEEN 80 AND 100
                THEN LAG(value) OVER w
                ELSE NULL
            END AS prev_value
        FROM quarterly_filings_raw q
        WINDOW w AS (
            PARTITION BY company_id, statement, normalized_label, axis, member
            ORDER BY period_end
        )
    ),
    quarterly_filings AS (
        SELECT
            company_id,
            filing_id,
            id,
            parent_id,
            fiscal_year,
            fiscal_quarter,
            fiscal_tag,
            label,
            normalized_label,
            CASE
                WHEN period = 'Q' THEN value
                WHEN period = 'YTD' AND prev_value IS NULL THEN value
                WHEN period = 'YTD' AND prev_value IS NOT NULL THEN value - prev_value
                ELSE value
            END AS value,
            weight,
            unit,
            statement,
            concept,
            axis,
            member,
            abstract_id,
            period_end,
            position,
            is_abstract,
            is_synthetic,
            source_type
        FROM quarterly_filings_with_prev
    ),
    annual_filings AS (
        SELECT
            company_id,
            filing_id,
            id,
            parent_id,
            fiscal_year,
            fiscal_quarter,
            fiscal_tag,
            label,
            value,
            latest_weight AS weight,
            unit,
            statement,
            concept,
            axis,
            member,
            latest_abstract_id AS abstract_id,
            period_end,
            normalized_label,
            latest_position AS position,
            is_abstract,
            is_synthetic,
            source_type
        FROM all_filings_data
        WHERE source_type = '10-K'
    ),
    quarterly_aggregation AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            axis,
            member,
            fiscal_tag,
            SUM(value) AS value
        FROM quarterly_filings
        GROUP BY
            company_id,
            statement,
            normalized_label,
            axis,
            member,
            fiscal_tag
    ),
    missing_quarters AS (
        SELECT
            a.company_id,
            a.filing_id,
            a.id,
            a.parent_id,
            a.fiscal_year,
            a.fiscal_quarter,
            a.concept,
            a.label,
            a.value - q.value AS value,
            a.unit,
            a.weight,
            a.axis,
            a.member,
            a.statement,
            a.abstract_id,
            a.period_end,
            a.normalized_label,
            a.position,
            a.is_abstract,
            a.is_synthetic,
            'calculated' AS source_type
        FROM annual_filings a
        JOIN quarterly_aggregation q
            ON q.company_id = a.company_id
            AND q.statement = a.statement
            AND q.normalized_label = a.normalized_label
            AND q.axis = a.axis
            AND q.member = a.member
            AND q.fiscal_tag = a.fiscal_tag
        WHERE
            a.statement != 'Balance Sheet'
            AND a.normalized_label NOT ILIKE 'Shares Outstanding%'
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
        weight,
        unit,
        axis,
        member,
        statement,
        abstract_id,
        period_end,
        fiscal_year,
        fiscal_quarter,
        position,
        is_abstract,
        is_synthetic,
        source_type
    FROM quarterly_filings

    UNION ALL

    SELECT
        id,
        parent_id,
        company_id,
        filing_id,
        concept,
        label,
        normalized_label,
        value,
        weight,
        unit,
        axis,
        member,
        statement,
        abstract_id,
        period_end,
        fiscal_year,
        fiscal_quarter,
        position,
        is_abstract,
        is_synthetic,
        source_type
    FROM annual_filings
    WHERE
        statement = 'Balance Sheet'
        OR normalized_label ILIKE 'Shares Outstanding%'

    UNION ALL

    SELECT
        id,
        parent_id,
        company_id,
        filing_id,
        concept,
        label,
        normalized_label,
        value,
        weight,
        unit,
        axis,
        member,
        statement,
        abstract_id,
        period_end,
        fiscal_year,
        fiscal_quarter,
        position,
        is_abstract,
        is_synthetic,
        source_type
    FROM missing_quarters;

    DELETE FROM quarterly_financials qf
    WHERE
        qf.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_quarterly_financials_new t
            WHERE t.id = qf.id
        );

    INSERT INTO quarterly_financials (
        id,
        parent_id,
        company_id,
        filing_id,
        concept,
        label,
        normalized_label,
        value,
        weight,
        unit,
        axis,
        member,
        statement,
        abstract_id,
        period_end,
        fiscal_year,
        fiscal_quarter,
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
        weight,
        unit,
        axis,
        member,
        statement,
        abstract_id,
        period_end,
        fiscal_year,
        fiscal_quarter,
        position,
        is_abstract,
        is_synthetic,
        source_type
    FROM tmp_quarterly_financials_new
    ON CONFLICT (id) DO UPDATE
    SET
        parent_id = EXCLUDED.parent_id,
        company_id = EXCLUDED.company_id,
        filing_id = EXCLUDED.filing_id,
        concept = EXCLUDED.concept,
        label = EXCLUDED.label,
        normalized_label = EXCLUDED.normalized_label,
        value = EXCLUDED.value,
        weight = EXCLUDED.weight,
        unit = EXCLUDED.unit,
        axis = EXCLUDED.axis,
        member = EXCLUDED.member,
        statement = EXCLUDED.statement,
        abstract_id = EXCLUDED.abstract_id,
        period_end = EXCLUDED.period_end,
        fiscal_year = EXCLUDED.fiscal_year,
        fiscal_quarter = EXCLUDED.fiscal_quarter,
        position = EXCLUDED.position,
        is_abstract = EXCLUDED.is_abstract,
        is_synthetic = EXCLUDED.is_synthetic,
        source_type = EXCLUDED.source_type;
END;
$$;
