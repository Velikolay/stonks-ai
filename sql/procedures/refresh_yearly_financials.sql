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
            -- value is the instance amount. print_sign is the print
            -- instruction; display is value * print_sign. Weight is a
            -- linkbase coefficient, not a display-sign convention; rewriting history
            -- when weight flips (e.g. MSFT FY2014 expenses) invents false sign flips.
            ff.value,
            ff.weight,
            ff.print_sign,
            ff.unit,
            ff.axis,
            ff.member,
            ff.statement,
            ff.period_end,
            ff.is_abstract,
            ff.is_synthetic,
            CASE
                WHEN ff.form_type IN ('20-F', '20-F/A') THEN '20-F'
                ELSE '10-K'
            END AS source_type,
            -- Zero on the filing that closed period_end. Otherwise that
            -- filing was never ingested; shift by the calendar gap.
            f.fiscal_year - (
                EXTRACT(YEAR FROM f.fiscal_period_end)::int
                - EXTRACT(YEAR FROM ff.period_end)::int
            ) AS fiscal_year,
            FIRST_VALUE(ff.abstract_id) OVER w AS latest_abstract_id,
            FIRST_VALUE(ff.position) OVER w AS latest_position
        FROM financial_facts_normalized ff
        JOIN filings f
            ON f.id = ff.fiscal_filing_id
        WHERE
            ff.company_id = ANY(company_ids)
            AND ff.form_type IN ('10-K', '10-K/A', '20-F', '20-F/A')
            AND ff.is_duplicate = false
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
        weight,
        print_sign,
        axis,
        member,
        statement,
        latest_abstract_id AS abstract_id,
        period_end,
        fiscal_year,
        latest_position AS position,
        is_abstract,
        is_synthetic,
        source_type
    FROM all_filings_data;

    -- Rows in financial_rewrites with granularity yearly. Quarterly rows
    -- are applied by refresh_quarterly_financials. delete removes a member
    -- for one period, set replaces its value, and add copies a sibling on
    -- that axis. When the axis has no row yet, add copies the undimensioned
    -- line so the first member of an axis can be introduced.
    DELETE FROM tmp_yearly_financials_new t
    USING financial_rewrites o
    WHERE
        o.granularity = 'yearly'
        AND o.action = 'delete'
        AND o.company_id = ANY(company_ids)
        AND t.company_id = o.company_id
        AND t.statement = o.statement
        AND t.normalized_label = o.normalized_label
        AND t.axis = o.axis
        AND t.member = o.member
        AND t.period_end = o.period_end;

    UPDATE tmp_yearly_financials_new t
    SET
        value = o.value,
        is_synthetic = true,
        source_type = 'calculated'
    FROM financial_rewrites o
    WHERE
        o.granularity = 'yearly'
        AND o.action = 'set'
        AND o.company_id = ANY(company_ids)
        AND t.company_id = o.company_id
        AND t.statement = o.statement
        AND t.normalized_label = o.normalized_label
        AND t.axis = o.axis
        AND t.member = o.member
        AND t.period_end = o.period_end;

    INSERT INTO tmp_yearly_financials_new (
        id, parent_id, company_id, filing_id, concept, label, normalized_label,
        value, unit, weight, print_sign, axis, member, statement, abstract_id,
        period_end, fiscal_year, position, is_abstract, is_synthetic, source_type
    )
    SELECT
        abs(hashtextextended(
            'financial-rewrite|yearly|' || o.company_id::text || '|'
            || o.statement || '|' || o.normalized_label || '|' || o.axis
            || '|' || o.member || '|' || o.period_end::text,
            0
        )),
        s.parent_id,
        o.company_id,
        s.filing_id,
        s.concept,
        s.label,
        o.normalized_label,
        o.value,
        s.unit,
        s.weight,
        s.print_sign,
        o.axis,
        o.member,
        o.statement,
        s.abstract_id,
        o.period_end,
        s.fiscal_year,
        s.position,
        false,
        true,
        'calculated'
    FROM financial_rewrites o
    JOIN LATERAL (
        SELECT *
        FROM tmp_yearly_financials_new t
        WHERE
            t.company_id = o.company_id
            AND t.statement = o.statement
            AND t.normalized_label = o.normalized_label
            AND t.period_end = o.period_end
            AND NOT t.is_abstract
            AND (t.axis = o.axis OR t.axis = '')
        ORDER BY CASE WHEN t.axis = o.axis THEN 0 ELSE 1 END, t.member
        LIMIT 1
    ) s ON true
    WHERE
        o.granularity = 'yearly'
        AND o.action = 'add'
        AND o.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_yearly_financials_new t
            WHERE
                t.company_id = o.company_id
                AND t.statement = o.statement
                AND t.normalized_label = o.normalized_label
                AND t.axis = o.axis
                AND t.member = o.member
                AND t.period_end = o.period_end
        );

    -- A row whose unique key changed is deleted and reinserted: two rows can
    -- swap keys (a member relabelled), which an in-place upsert would reject.
    -- A row whose unique key changed is deleted and reinserted: two rows can
    -- swap keys (a member relabelled), which an in-place upsert would reject.
    DELETE FROM yearly_financials yf
    WHERE
        yf.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_yearly_financials_new t
            WHERE
                t.id = yf.id
                AND t.company_id = yf.company_id
                AND t.statement = yf.statement
                AND t.concept = yf.concept
                AND t.normalized_label = yf.normalized_label
                AND t.period_end = yf.period_end
                AND t.axis = yf.axis
                AND t.member = yf.member
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
        print_sign,
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
        print_sign,
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
        print_sign = EXCLUDED.print_sign,
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
