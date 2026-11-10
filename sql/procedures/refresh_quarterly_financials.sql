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
            AND f.form_type IN ('10-K', '10-K/A', '10-Q', '10-Q/A')
    ),
    filings_cte AS (
        SELECT
            o.id,
            o.company_id,
            o.fiscal_year,
            o.fiscal_quarter,
            CASE
                WHEN o.form_type = '10-K' THEN o.id
                ELSE (
                    SELECT k.id
                    FROM ordered_filings k
                    WHERE k.company_id = o.company_id
                        AND k.form_type = '10-K'
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
            -- value is the instance amount. print_sign is the print
            -- instruction; display is value * print_sign. Weight is a
            -- linkbase coefficient, not a display-sign convention; rewriting history
            -- when weight flips invents false sign flips in the presentation layer.
            ff.value,
            ff.weight,
            ff.print_sign,
            ff.unit,
            ff.statement,
            ff.concept,
            ff.axis,
            ff.member,
            ff.period_end,
            ff.period,
            ff.comparative_period_end,
            ff.comparative_value,
            ff.is_abstract,
            ff.is_synthetic,
            ff.form_type AS source_type,
            f.fiscal_year,
            f.fiscal_quarter,
            f.fiscal_tag,
            FIRST_VALUE(ff.abstract_id) OVER w AS latest_abstract_id,
            FIRST_VALUE(ff.position) OVER w AS latest_position
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
            weight,
            print_sign,
            latest_position AS position,
            period_end,
            period,
            comparative_period_end,
            comparative_value,
            is_abstract,
            is_synthetic,
            source_type,
            false AS is_comparative_backfill
        FROM all_filings_data
        WHERE source_type IN ('10-Q', '10-Q/A')
    ),
    series_start AS (
        SELECT DISTINCT ON (company_id, statement, normalized_label, axis, member) *
        FROM quarterly_filings_raw
        ORDER BY company_id, statement, normalized_label, axis, member, period_end
    ),
    -- The period before a series opens, taken from the next year's comparative.
    -- It only feeds the differencing below and is filtered from the output.
    comparative_backfill AS (
        SELECT DISTINCT ON (s.company_id, s.statement, s.normalized_label, s.axis, s.member)
            s.company_id,
            n.filing_id,
            abs(hashtextextended(
                'quarterly-prior|' || s.company_id::text || '|' || s.statement
                || '|' || s.normalized_label || '|' || s.axis || '|' || s.member
                || '|' || n.comparative_period_end::text,
                0
            )) AS id,
            s.parent_id,
            s.fiscal_year,
            n.fiscal_quarter,
            s.fiscal_tag,
            s.label,
            s.normalized_label,
            n.comparative_value AS value,
            s.unit,
            s.statement,
            s.concept,
            s.axis,
            s.member,
            s.abstract_id,
            s.weight,
            s.print_sign,
            s.position,
            n.comparative_period_end AS period_end,
            n.period,
            NULL::date AS comparative_period_end,
            NULL::numeric AS comparative_value,
            false AS is_abstract,
            true AS is_synthetic,
            'calculated' AS source_type,
            true AS is_comparative_backfill
        FROM series_start s
        JOIN quarterly_filings_raw n
            ON n.company_id = s.company_id
            AND n.statement = s.statement
            AND n.normalized_label = s.normalized_label
            AND n.axis = s.axis
            AND n.member = s.member
            AND n.comparative_value IS NOT NULL
            AND (s.period_end - n.comparative_period_end) BETWEEN 80 AND 100
        WHERE s.period = 'YTD'
        ORDER BY
            s.company_id, s.statement, s.normalized_label, s.axis, s.member,
            n.period_end
    ),
    quarterly_filings_with_prev AS (
        SELECT
            q.*,
            k.fiscal_period_end AS annual_period_end,
            -- Stocks (period NULL) and weighted-average share counts are not
            -- additive across sub-periods; never YTD-difference them. EPS stays
            -- on the normal YTD→discrete path.
            (
                q.period IS NULL
                OR q.concept IN (
                    'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
                    'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'
                )
            ) AS is_non_additive,
            LAG(q.period_end) OVER w AS prev_period_end,
            CASE
                WHEN (q.period_end - LAG(q.period_end) OVER w) BETWEEN 80 AND 100
                THEN LAG(q.value) OVER w
                ELSE NULL
            END AS prev_value
        FROM (
            SELECT * FROM quarterly_filings_raw
            UNION ALL
            SELECT * FROM comparative_backfill
        ) q
        LEFT JOIN filings k ON k.id = q.fiscal_tag
        WINDOW w AS (
            PARTITION BY q.company_id, q.statement, q.normalized_label, q.axis, q.member
            ORDER BY q.period_end
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
                WHEN period = 'YTD' AND NOT is_non_additive AND prev_value IS NOT NULL
                THEN value - prev_value
                ELSE value
            END AS value,
            weight,
            print_sign,
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
            source_type,
            is_comparative_backfill,
            -- No earlier quarter: this YTD cannot be reduced to one quarter,
            -- so it is not shown. A later gap stays.
            (
                period = 'YTD'
                AND prev_period_end IS NULL
                AND NOT is_non_additive
            ) AS drop_ytd,
            -- Raw nine-month total on the quarter before the 10-K. Used for Q4
            -- when the dropped YTD means the discrete quarters are incomplete.
            -- value here is the pre-difference amount.
            CASE
                WHEN period = 'YTD'
                    AND NOT is_non_additive
                    AND COALESCE(unit, '') <> 'usdPerShare'
                    AND annual_period_end IS NOT NULL
                    AND (annual_period_end - period_end) BETWEEN 80 AND 100
                THEN value
            END AS q3_ytd
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
            weight,
            print_sign,
            unit,
            statement,
            concept,
            axis,
            member,
            latest_abstract_id AS abstract_id,
            period_end,
            period,
            normalized_label,
            latest_position AS position,
            is_abstract,
            is_synthetic,
            source_type
        FROM all_filings_data
        WHERE source_type IN ('10-K', '10-K/A')
    ),
    quarterly_aggregation AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            axis,
            member,
            fiscal_tag,
            -- Three discrete quarters sum to Q1–Q3. Otherwise subtract the
            -- nine-month total. Per-share rows have no q3_ytd, so no Q4.
            CASE
                WHEN COUNT(DISTINCT fiscal_quarter) FILTER (WHERE NOT drop_ytd) = 3
                THEN SUM(value) FILTER (WHERE NOT drop_ytd)
                ELSE MAX(q3_ytd)
            END AS value
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
            a.print_sign,
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
            -- Instant stocks are not annual−Σquarters flows.
            AND a.period IS NOT NULL
            AND a.concept NOT IN (
                'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
                'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'
            )
            -- Abstracts have no value but children point at the 10-K header.
            AND (q.value IS NOT NULL OR a.is_abstract)
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
        print_sign,
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
    WHERE NOT drop_ytd
        AND NOT is_comparative_backfill

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
        print_sign,
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
        OR concept IN (
            'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
            'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'
        )
        -- Year-end stocks (period NULL): pass through as Q4 like the BS.
        -- Skip "beginning *" — those are year-start values on the 10-K, not Q4.
        OR (
            period IS NULL
            AND COALESCE(normalized_label, '') !~* 'beginning'
            AND COALESCE(label, '') !~* 'beginning'
        )

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
        print_sign,
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
        print_sign,
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
        print_sign,
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
        print_sign = EXCLUDED.print_sign,
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
