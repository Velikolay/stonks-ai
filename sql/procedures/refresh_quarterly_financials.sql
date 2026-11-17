CREATE OR REPLACE PROCEDURE refresh_quarterly_financials(company_ids int[])
LANGUAGE plpgsql
-- CTEs carry no row statistics. A merge join on (company, statement) then
-- filters the remaining keys pair by pair, which is quadratic in facts.
SET enable_mergejoin = off
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
            o.fiscal_period_end,
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
            -- A prior-year column is stored on the filing that printed it and
            -- placed in the fiscal period that closed on period_end.
            -- Zero on the filing that closed period_end. Otherwise that
            -- filing was never ingested; shift by the calendar gap.
            f.fiscal_year - (
                EXTRACT(YEAR FROM f.fiscal_period_end)::int
                - EXTRACT(YEAR FROM ff.period_end)::int
            ) AS fiscal_year,
            f.fiscal_quarter,
            -- With no filing of its own, the source filing's 10-K is another
            -- year; leave the row out of the Q4 residual.
            CASE WHEN f.fiscal_period_end = ff.period_end THEN f.fiscal_tag END
                AS fiscal_tag,
            FIRST_VALUE(ff.abstract_id) OVER w AS latest_abstract_id,
            FIRST_VALUE(ff.position) OVER w AS latest_position
        FROM financial_facts_normalized ff
        JOIN filings_cte f
            ON f.company_id = ff.company_id
            AND f.id = ff.fiscal_filing_id
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
    -- Year-to-date total kept beside a published quarter of the same date.
    -- It is a duplicate of that quarter, so it is not shown; Q4 still
    -- subtracts it from the annual figure. It must be the same line of the
    -- same filing as the quarter, since one filing can print two lines under
    -- one label.
    ytd_anchor AS (
        SELECT DISTINCT ON (
            ff.company_id, ff.statement, ff.normalized_label, ff.axis, ff.member,
            ff.period_end, ff.filing_id, ff.concept
        )
            ff.company_id,
            ff.statement,
            ff.normalized_label,
            ff.axis,
            ff.member,
            ff.period_end,
            ff.filing_id,
            ff.concept,
            ff.value
        FROM financial_facts_normalized ff
        WHERE
            ff.company_id = ANY(company_ids)
            AND ff.is_duplicate = true
            AND ff.period = 'YTD'
            AND NOT ff.is_abstract
        ORDER BY
            ff.company_id, ff.statement, ff.normalized_label, ff.axis, ff.member,
            ff.period_end, ff.filing_id, ff.concept, ff.id DESC
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
            -- A year-to-date total after a published quarter subtracts that
            -- quarter's year-to-date total, not the quarter.
            CASE
                WHEN (q.period_end - LAG(q.period_end) OVER w) BETWEEN 80 AND 100
                THEN LAG(COALESCE(prev_ytd.value, q.value)) OVER w
                ELSE NULL
            END AS prev_value
        FROM (
            SELECT * FROM quarterly_filings_raw
            UNION ALL
            SELECT * FROM comparative_backfill
        ) q
        LEFT JOIN filings k ON k.id = q.fiscal_tag
        LEFT JOIN ytd_anchor prev_ytd
            ON q.period = 'Q'
            AND prev_ytd.company_id = q.company_id
            AND prev_ytd.statement = q.statement
            AND prev_ytd.normalized_label = q.normalized_label
            AND prev_ytd.axis = q.axis
            AND prev_ytd.member = q.member
            AND prev_ytd.period_end = q.period_end
            AND prev_ytd.filing_id = q.filing_id
            AND prev_ytd.concept = q.concept
        WINDOW w AS (
            PARTITION BY q.company_id, q.statement, q.normalized_label, q.axis, q.member
            ORDER BY q.period_end
        )
    ),
    quarterly_filings AS (
        SELECT
            q.company_id,
            q.filing_id,
            q.id,
            q.parent_id,
            q.fiscal_year,
            q.fiscal_quarter,
            q.fiscal_tag,
            q.label,
            q.normalized_label,
            CASE
                WHEN q.period = 'YTD' AND NOT q.is_non_additive AND q.prev_value IS NOT NULL
                THEN q.value - q.prev_value
                ELSE q.value
            END AS value,
            q.weight,
            q.print_sign,
            q.unit,
            q.statement,
            q.concept,
            q.axis,
            q.member,
            q.abstract_id,
            q.period_end,
            q.position,
            q.is_abstract,
            q.is_synthetic,
            q.source_type,
            q.is_comparative_backfill,
            -- No earlier quarter: this YTD cannot be reduced to one quarter,
            -- so it is not shown. A later gap stays.
            (
                q.period = 'YTD'
                AND q.prev_period_end IS NULL
                AND NOT q.is_non_additive
            ) AS drop_ytd,
            -- Raw nine-month total on the quarter before the 10-K. A published
            -- quarter keeps that total on its duplicate row. Otherwise value
            -- here is the pre-difference amount.
            CASE
                WHEN q.period = 'Q'
                    AND a.value IS NOT NULL
                    AND NOT q.is_non_additive
                    AND COALESCE(q.unit, '') <> 'usdPerShare'
                    AND q.annual_period_end IS NOT NULL
                    AND (q.annual_period_end - q.period_end) BETWEEN 80 AND 100
                THEN a.value
                WHEN q.period = 'YTD'
                    AND NOT q.is_non_additive
                    AND COALESCE(q.unit, '') <> 'usdPerShare'
                    AND q.annual_period_end IS NOT NULL
                    AND (q.annual_period_end - q.period_end) BETWEEN 80 AND 100
                THEN q.value
            END AS q3_ytd,
            (
                q.period = 'Q'
                AND a.value IS NOT NULL
                AND NOT q.is_non_additive
                AND COALESCE(q.unit, '') <> 'usdPerShare'
                AND q.annual_period_end IS NOT NULL
                AND (q.annual_period_end - q.period_end) BETWEEN 80 AND 100
            ) AS prefer_ytd_anchor
        FROM quarterly_filings_with_prev q
        LEFT JOIN ytd_anchor a
            ON a.company_id = q.company_id
            AND a.statement = q.statement
            AND a.normalized_label = q.normalized_label
            AND a.axis = q.axis
            AND a.member = q.member
            AND a.period_end = q.period_end
            AND a.filing_id = q.filing_id
            AND a.concept = q.concept
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
            q.company_id,
            q.statement,
            q.normalized_label,
            q.axis,
            q.member,
            q.fiscal_tag,
            -- A backfilled comparative was printed a year later; it is not
            -- when the quarters were filed.
            MAX(src.filing_date) FILTER (WHERE NOT q.is_comparative_backfill)
                AS filed,
            -- Three discrete quarters sum to Q1–Q3. A published quarter keeps
            -- the nine-month total for Q4, because the earlier quarters can be
            -- on the previous classification. Otherwise subtract that total
            -- when a quarter is missing. Per-share rows have no q3_ytd.
            CASE
                WHEN COUNT(DISTINCT q.fiscal_quarter) FILTER (WHERE NOT q.drop_ytd) = 3
                    AND NOT COALESCE(bool_or(q.prefer_ytd_anchor), false)
                THEN SUM(q.value) FILTER (WHERE NOT q.drop_ytd)
                ELSE MAX(q.q3_ytd)
            END AS value
        FROM quarterly_filings q
        JOIN filings src ON src.id = q.filing_id
        GROUP BY
            q.company_id,
            q.statement,
            q.normalized_label,
            q.axis,
            q.member,
            q.fiscal_tag
    ),
    -- Every 10-K that reports a member for the year, including later 10-Ks
    -- that recast the year and lost to the year's own 10-K in dedup.
    annual_members AS (
        SELECT
            a.*,
            f.fiscal_year,
            f.fiscal_quarter,
            f.id AS fiscal_tag,
            src.filing_date,
            src.form_type = '10-K/A' AS is_amendment
        FROM financial_facts_normalized a
        JOIN filings src ON src.id = a.filing_id
        JOIN filings f
            ON f.id = a.fiscal_filing_id
            AND f.fiscal_period_end = a.period_end
        WHERE a.company_id = ANY(company_ids)
            AND a.form_type IN ('10-K', '10-K/A')
            AND a.period = 'YTD'
            AND a.axis <> ''
            AND NOT a.is_abstract
    ),
    quarter_members AS (
        SELECT company_id, statement, normalized_label, axis, fiscal_tag, COUNT(*) AS members
        FROM quarterly_aggregation
        WHERE value IS NOT NULL
        GROUP BY company_id, statement, normalized_label, axis, fiscal_tag
    ),
    -- Every 10-Q that printed the nine months before a 10-K, including
    -- reprints a year later under that year's members.
    nine_months AS (
        SELECT DISTINCT ON (
            ff.company_id, ff.statement, ff.normalized_label, ff.axis, ff.member,
            ff.filing_id
        )
            ff.company_id,
            ff.statement,
            ff.normalized_label,
            ff.axis,
            ff.member,
            ff.filing_id,
            src.filing_date,
            f.fiscal_tag,
            ff.value
        FROM financial_facts_normalized ff
        JOIN filings src ON src.id = ff.filing_id
        JOIN filings_cte f
            ON f.id = ff.fiscal_filing_id
            AND f.fiscal_period_end = ff.period_end
        JOIN filings k ON k.id = f.fiscal_tag
        WHERE ff.company_id = ANY(company_ids)
            AND ff.form_type IN ('10-Q', '10-Q/A')
            AND ff.period = 'YTD'
            AND ff.axis <> ''
            AND NOT ff.is_abstract
            AND (k.fiscal_period_end - ff.period_end) BETWEEN 80 AND 100
        ORDER BY
            ff.company_id, ff.statement, ff.normalized_label, ff.axis, ff.member,
            ff.filing_id, ff.is_duplicate, ff.id
    ),
    -- A 10-K's year minus the nine months of a 10-Q with the same members,
    -- the nearest one filed, so a recast year pairs with a 10-Q recast the
    -- same way and not with quarters on the earlier members.
    nine_month_split AS MATERIALIZED (
        SELECT DISTINCT ON (
            a.company_id, a.statement, a.normalized_label, a.axis, a.fiscal_tag,
            a.filing_id
        )
            a.company_id,
            a.statement,
            a.normalized_label,
            a.axis,
            a.fiscal_tag,
            a.filing_id,
            n.filing_id AS nine_month_filing_id
        FROM (
            SELECT
                company_id, statement, normalized_label, axis, fiscal_tag,
                filing_id, filing_date,
                array_agg(DISTINCT member ORDER BY member) AS members
            FROM annual_members
            GROUP BY
                company_id, statement, normalized_label, axis, fiscal_tag,
                filing_id, filing_date
        ) a
        JOIN (
            SELECT
                company_id, statement, normalized_label, axis, fiscal_tag,
                filing_id, filing_date,
                array_agg(DISTINCT member ORDER BY member) AS members
            FROM nine_months
            GROUP BY
                company_id, statement, normalized_label, axis, fiscal_tag,
                filing_id, filing_date
        ) n
            ON n.company_id = a.company_id
            AND n.statement = a.statement
            AND n.normalized_label = a.normalized_label
            AND n.axis = a.axis
            AND n.fiscal_tag = a.fiscal_tag
            AND n.members = a.members
        ORDER BY
            a.company_id, a.statement, a.normalized_label, a.axis, a.fiscal_tag,
            a.filing_id, ABS(n.filing_date - a.filing_date), n.filing_id
    ),
    -- Q4 subtracts the quarters from one 10-K's split of the year. Prefer an
    -- amendment, then the year's own 10-K, then a 10-K on the split the
    -- quarters were filed on: every member has quarters and every member
    -- with quarters is in it. Then the first 10-K filed after the quarters,
    -- then the latest before them. A member that 10-K leaves out comes from
    -- the next one in that order.
    annual_split AS (
        SELECT
            company_id, statement, normalized_label, axis, fiscal_tag, filing_id,
            ROW_NUMBER() OVER (
                PARTITION BY company_id, statement, normalized_label, axis, fiscal_tag
                ORDER BY
                    is_amendment DESC,
                    filing_id = fiscal_tag DESC,
                    matches_quarters DESC NULLS LAST,
                    filing_date >= quarters_filed DESC,
                    CASE WHEN filing_date >= quarters_filed THEN filing_date END ASC,
                    filing_date DESC,
                    filing_id DESC
            ) AS split_rank
        FROM (
            SELECT
                a.company_id,
                a.statement,
                a.normalized_label,
                a.axis,
                a.fiscal_tag,
                a.filing_id,
                a.filing_date,
                a.is_amendment,
                bool_and(q.value IS NOT NULL OR a.value = 0)
                    AND COUNT(DISTINCT q.member) FILTER (WHERE q.value IS NOT NULL)
                        = MAX(qm.members)
                    AS matches_quarters,
                MAX(MAX(q.filed)) OVER (
                    PARTITION BY a.company_id, a.statement, a.normalized_label,
                        a.axis, a.fiscal_tag
                ) AS quarters_filed
            FROM annual_members a
            LEFT JOIN quarterly_aggregation q
                ON q.company_id = a.company_id
                AND q.statement = a.statement
                AND q.normalized_label = a.normalized_label
                AND q.axis = a.axis
                AND q.member = a.member
                AND q.fiscal_tag = a.fiscal_tag
            LEFT JOIN quarter_members qm
                ON qm.company_id = a.company_id
                AND qm.statement = a.statement
                AND qm.normalized_label = a.normalized_label
                AND qm.axis = a.axis
                AND qm.fiscal_tag = a.fiscal_tag
            GROUP BY
                a.company_id, a.statement, a.normalized_label, a.axis,
                a.fiscal_tag, a.filing_id, a.filing_date, a.is_amendment
        ) candidates
    ),
    missing_quarters AS (
        (SELECT DISTINCT ON (
            q.company_id, q.statement, q.normalized_label, q.axis, q.member,
            q.fiscal_tag
        )
            a.company_id,
            a.filing_id,
            a.id,
            a.parent_id,
            a.fiscal_year,
            a.fiscal_quarter,
            a.concept,
            a.label,
            a.value - COALESCE(nm.value, q.value) AS value,
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
        FROM quarterly_aggregation q
        JOIN annual_split split
            ON split.company_id = q.company_id
            AND split.statement = q.statement
            AND split.normalized_label = q.normalized_label
            AND split.axis = q.axis
            AND split.fiscal_tag = q.fiscal_tag
        JOIN annual_members a
            ON a.filing_id = split.filing_id
            AND a.company_id = q.company_id
            AND a.statement = q.statement
            AND a.normalized_label = q.normalized_label
            AND a.axis = q.axis
            AND a.member = q.member
            AND a.fiscal_tag = q.fiscal_tag
        LEFT JOIN nine_month_split nms
            ON nms.filing_id = split.filing_id
            AND nms.company_id = q.company_id
            AND nms.statement = q.statement
            AND nms.normalized_label = q.normalized_label
            AND nms.axis = q.axis
            AND nms.fiscal_tag = q.fiscal_tag
        LEFT JOIN nine_months nm
            ON nm.filing_id = nms.nine_month_filing_id
            AND nm.company_id = q.company_id
            AND nm.statement = q.statement
            AND nm.normalized_label = q.normalized_label
            AND nm.axis = q.axis
            AND nm.member = q.member
            AND nm.fiscal_tag = q.fiscal_tag
        WHERE
            q.value IS NOT NULL
            AND a.statement != 'Balance Sheet'
            AND a.concept NOT IN (
                'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
                'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'
            )
        ORDER BY
            q.company_id, q.statement, q.normalized_label, q.axis, q.member,
            q.fiscal_tag, split.split_rank, a.is_duplicate, a.id)

        UNION ALL

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
            COALESCE(a.axis, '') = ''
            AND a.statement != 'Balance Sheet'
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

    -- Rows in financial_rewrites with granularity quarterly. Yearly rows
    -- are applied by refresh_yearly_financials. delete removes a member
    -- for one period, set replaces its value, and add copies a sibling on
    -- that axis. When the axis has no row yet, add copies the undimensioned
    -- line so the first member of an axis can be introduced.
    DELETE FROM tmp_quarterly_financials_new t
    USING financial_rewrites o
    WHERE
        o.granularity = 'quarterly'
        AND o.action = 'delete'
        AND o.company_id = ANY(company_ids)
        AND t.company_id = o.company_id
        AND t.statement = o.statement
        AND t.normalized_label = o.normalized_label
        AND t.axis = o.axis
        AND t.member = o.member
        AND t.period_end = o.period_end;

    UPDATE tmp_quarterly_financials_new t
    SET
        value = o.value,
        is_synthetic = true,
        source_type = 'calculated'
    FROM financial_rewrites o
    WHERE
        o.granularity = 'quarterly'
        AND o.action = 'set'
        AND o.company_id = ANY(company_ids)
        AND t.company_id = o.company_id
        AND t.statement = o.statement
        AND t.normalized_label = o.normalized_label
        AND t.axis = o.axis
        AND t.member = o.member
        AND t.period_end = o.period_end;

    INSERT INTO tmp_quarterly_financials_new (
        id, parent_id, company_id, filing_id, concept, label, normalized_label,
        value, weight, print_sign, unit, axis, member, statement, abstract_id,
        period_end, fiscal_year, fiscal_quarter, position, is_abstract,
        is_synthetic, source_type
    )
    SELECT
        abs(hashtextextended(
            'financial-rewrite|quarterly|' || o.company_id::text || '|'
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
        s.weight,
        s.print_sign,
        s.unit,
        o.axis,
        o.member,
        o.statement,
        s.abstract_id,
        o.period_end,
        s.fiscal_year,
        s.fiscal_quarter,
        s.position,
        false,
        true,
        'calculated'
    FROM financial_rewrites o
    JOIN LATERAL (
        SELECT *
        FROM tmp_quarterly_financials_new t
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
        o.granularity = 'quarterly'
        AND o.action = 'add'
        AND o.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_quarterly_financials_new t
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
    DELETE FROM quarterly_financials qf
    WHERE
        qf.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_quarterly_financials_new t
            WHERE
                t.id = qf.id
                AND t.company_id = qf.company_id
                AND t.statement = qf.statement
                AND t.concept = qf.concept
                AND t.normalized_label = qf.normalized_label
                AND t.period_end = qf.period_end
                AND t.axis = qf.axis
                AND t.member = qf.member
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
