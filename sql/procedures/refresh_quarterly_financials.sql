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
            -- Stocks (period NULL), period-end shares outstanding, and
            -- weighted-average share counts are not additive across
            -- sub-periods; never YTD-difference them. A company extension of
            -- the period-end count is still a balance. Per-share amounts are
            -- also not additive, but a filing that only stored the
            -- year-to-date column still reduces on this path until that
            -- filing is re-ingested with its printed quarter.
            (
                q.period IS NULL
                OR q.concept IN (
                    'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
                    'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'
                )
                OR (
                    COALESCE(q.unit, '') = 'shares'
                    AND q.concept ILIKE '%SharesOutstanding%'
                    AND q.concept NOT ILIKE '%WeightedAverage%'
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
            q.is_non_additive,
            -- A YTD that was not reduced to one quarter is not shown. That is
            -- the opening of a series, and a later Q2 or Q3 whose prior
            -- quarter is missing: the amount is still the six- or nine-month
            -- total. An unreduced Q1 is that quarter. A discrete quarter
            -- after a gap stays. An abstract has no amount, so this flag does
            -- not remove it: lines point at the header of the latest filing.
            (
                q.period = 'YTD'
                AND NOT q.is_non_additive
                AND (
                    q.prev_period_end IS NULL
                    OR (
                        q.prev_value IS NULL
                        AND q.fiscal_quarter IN (2, 3)
                    )
                )
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
    -- The next year's 10-Q comparatives of this year's undimensioned
    -- income-statement quarters. A published quarter's comparative is that
    -- quarter; a year-to-date one subtracts the cumulative comparative of
    -- the quarter before it in the same filing year.
    comparative_quarters AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            period_end,
            CASE
                WHEN quarter_value IS NOT NULL THEN quarter_value
                WHEN fiscal_quarter = 1 THEN cumulative
                WHEN (period_end - LAG(period_end) OVER w) BETWEEN 80 AND 100
                THEN cumulative - LAG(cumulative) OVER w
            END AS value
        FROM (
            SELECT
                c.*,
                COALESCE(
                    c.ytd_value,
                    SUM(c.quarter_value) OVER (
                        PARTITION BY c.company_id, c.statement, c.normalized_label,
                            c.fiscal_year
                        ORDER BY c.period_end
                    )
                ) AS cumulative
            FROM (
                SELECT
                    company_id,
                    statement,
                    normalized_label,
                    fiscal_year,
                    fiscal_quarter,
                    comparative_period_end AS period_end,
                    MAX(comparative_value) FILTER (WHERE period = 'Q') AS quarter_value,
                    MAX(comparative_value) FILTER (WHERE period = 'YTD') AS ytd_value
                FROM quarterly_filings_raw
                WHERE statement = 'Income Statement'
                    AND axis = ''
                    AND NOT is_abstract
                    AND comparative_value IS NOT NULL
                    AND (period_end - comparative_period_end) BETWEEN 340 AND 380
                GROUP BY
                    company_id, statement, normalized_label, fiscal_year,
                    fiscal_quarter, comparative_period_end
            ) c
        ) c
        WINDOW w AS (
            PARTITION BY company_id, statement, normalized_label, fiscal_year
            ORDER BY period_end
        )
    ),
    -- A line's own first three quarters beside their comparatives. Share
    -- counts and per-share amounts do not add up to the year, so they are
    -- left out. CTEs carry no row estimates, so rows are paired by grouping
    -- rather than joined.
    quarter_pairs AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            period_end,
            MAX(fiscal_tag) AS fiscal_tag,
            MAX(fiscal_quarter) AS fiscal_quarter,
            SUM(own_value) AS own_value,
            SUM(comparative_value) AS comparative_value
        FROM (
            SELECT
                company_id,
                statement,
                normalized_label,
                period_end,
                fiscal_tag,
                fiscal_quarter,
                value AS own_value,
                NULL::numeric AS comparative_value
            FROM quarterly_filings
            WHERE statement = 'Income Statement'
                AND axis = ''
                AND fiscal_quarter IN (1, 2, 3)
                AND fiscal_tag IS NOT NULL
                AND value IS NOT NULL
                AND NOT is_abstract
                AND NOT drop_ytd
                AND NOT is_comparative_backfill
                AND NOT is_non_additive
                AND COALESCE(unit, '') <> 'usdPerShare'

            UNION ALL

            SELECT
                company_id,
                statement,
                normalized_label,
                period_end,
                NULL::int,
                NULL::int,
                NULL::numeric,
                value
            FROM comparative_quarters
            WHERE value IS NOT NULL
        ) p
        GROUP BY company_id, statement, normalized_label, period_end
        HAVING COUNT(own_value) = 1 AND COUNT(comparative_value) = 1
    ),
    -- Per year: the three quarter pairs, this year's 10-K, and the next
    -- 10-K's comparative of it.
    quarter_basis AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            fiscal_tag,
            SUM(own_value) AS own_sum,
            SUM(comparative_value) AS comp_sum,
            MAX(annual) AS annual,
            MAX(next_annual) AS next_annual,
            GREATEST(ABS(MAX(annual)) / 1000, 1) AS tolerance
        FROM (
            SELECT
                company_id,
                statement,
                normalized_label,
                fiscal_tag,
                fiscal_quarter,
                own_value,
                comparative_value,
                NULL::numeric AS annual,
                NULL::numeric AS next_annual
            FROM quarter_pairs

            UNION ALL

            SELECT
                company_id,
                statement,
                normalized_label,
                fiscal_tag,
                NULL,
                NULL,
                NULL,
                value,
                next_annual
            FROM (
                SELECT
                    company_id,
                    statement,
                    normalized_label,
                    fiscal_tag,
                    value,
                    CASE
                        WHEN LEAD(comparative_period_end) OVER w = period_end
                        THEN LEAD(comparative_value) OVER w
                    END AS next_annual
                FROM all_filings_data
                WHERE source_type IN ('10-K', '10-K/A')
                    AND statement = 'Income Statement'
                    AND axis = ''
                    AND period = 'YTD'
                    AND NOT is_abstract
                WINDOW w AS (
                    PARTITION BY company_id, statement, normalized_label
                    ORDER BY period_end
                )
            ) a
            WHERE fiscal_tag IS NOT NULL AND value IS NOT NULL
        ) y
        GROUP BY company_id, statement, normalized_label, fiscal_tag
        HAVING COUNT(DISTINCT fiscal_quarter) = 3
            AND COUNT(fiscal_quarter) = 3
            AND COUNT(annual) = 1
    ),
    -- Lines whose year moves onto the next year's comparatives. A line whose
    -- own nine months overshoot the 10-K by more than 5% would reverse the
    -- year in Q4. Other lines of that year follow when the next 10-K
    -- reprinted this one unchanged, so subtotals stay on one basis. Either
    -- way the comparatives must fit: the 10-K minus them keeps its sign.
    comparative_basis AS (
        SELECT company_id, statement, normalized_label, fiscal_tag
        FROM (
            SELECT
                b.*,
                bool_or(b.overshoots) OVER (
                    PARTITION BY b.company_id, b.statement, b.fiscal_tag
                ) AS year_overshoots
            FROM (
                SELECT
                    qb.*,
                    SIGN(qb.own_sum) = SIGN(qb.annual)
                        AND ABS(qb.own_sum) > ABS(qb.annual) * 1.05 AS overshoots
                FROM quarter_basis qb
                WHERE SIGN(qb.annual - qb.comp_sum) = SIGN(qb.annual)
                    OR ABS(qb.annual - qb.comp_sum) <= qb.tolerance
            ) b
        ) b
        WHERE overshoots
            OR (
                year_overshoots
                AND ABS(next_annual - annual) <= tolerance
                AND ABS(own_sum - comp_sum) > tolerance
            )
    ),
    restated_quarters AS MATERIALIZED (
        SELECT
            p.company_id,
            p.statement,
            p.normalized_label,
            p.period_end,
            p.comparative_value
        FROM comparative_basis b
        JOIN quarter_pairs p
            ON p.company_id = b.company_id
            AND p.statement = b.statement
            AND p.normalized_label = b.normalized_label
            AND p.fiscal_tag = b.fiscal_tag
    ),
    quarterly_filings_restated AS (
        SELECT q.*, r.comparative_value AS restated_value
        FROM quarterly_filings q
        LEFT JOIN restated_quarters r
            ON r.company_id = q.company_id
            AND r.statement = q.statement
            AND r.normalized_label = q.normalized_label
            AND r.period_end = q.period_end
            AND q.axis = ''
            AND NOT q.is_abstract
            AND NOT q.drop_ytd
            AND NOT q.is_comparative_backfill
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
            -- Three discrete quarters sum to Q1–Q3. A restatement takes the
            -- next year's comparatives, which fit this 10-K, so Q4 subtracts
            -- those. A published quarter otherwise keeps the nine-month total
            -- for Q4, because the earlier quarters can be on the previous
            -- classification. Otherwise subtract that total when a quarter is
            -- missing. Per-share rows have no q3_ytd.
            CASE
                WHEN COUNT(q.restated_value) = 3 THEN SUM(q.restated_value)
                WHEN COUNT(DISTINCT q.fiscal_quarter) FILTER (WHERE NOT q.drop_ytd) = 3
                    AND NOT COALESCE(bool_or(q.prefer_ytd_anchor), false)
                THEN SUM(q.value) FILTER (WHERE NOT q.drop_ytd)
                ELSE MAX(q.q3_ytd)
            END AS value
        FROM quarterly_filings_restated q
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
            ff.filing_id, f.fiscal_tag
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
            ff.filing_id, f.fiscal_tag, ff.is_duplicate, ff.id
    ),
    -- A 10-K's year minus the nine months of a 10-Q on the 10-K's split. A
    -- 10-Q a year later is on it when the first 10-K after that 10-Q prints
    -- the year as this 10-K did: no shared member at another value beyond
    -- rounding. Its reprint is then used when all its members are the
    -- 10-K's, which covers a 10-K that moved a line out of another.
    -- Otherwise a 10-Q with the same members, the year's own first, so a
    -- 10-K that recast its year pairs with the 10-Q that reprinted the nine
    -- months that way and not with quarters filed on the earlier members.
    nine_month_candidates AS (
        SELECT
            a.company_id,
            a.statement,
            a.normalized_label,
            a.axis,
            a.fiscal_tag,
            a.filing_id,
            a.filing_date,
            n.filing_id AS nine_month_filing_id,
            n.filing_date AS nine_month_filing_date,
            n.members = a.members AS same_members,
            next_annual.filing_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM annual_members own
                    JOIN annual_members later
                        ON later.filing_id = next_annual.filing_id
                        AND later.company_id = own.company_id
                        AND later.statement = own.statement
                        AND later.normalized_label = own.normalized_label
                        AND later.axis = own.axis
                        AND later.member = own.member
                        AND later.fiscal_tag = own.fiscal_tag
                    WHERE own.filing_id = a.filing_id
                        AND own.company_id = a.company_id
                        AND own.statement = a.statement
                        AND own.normalized_label = a.normalized_label
                        AND own.axis = a.axis
                        AND own.fiscal_tag = a.fiscal_tag
                        AND ABS(own.value - later.value) > CASE
                            WHEN later.value <> 0 AND later.value % 1000000 = 0
                                THEN 500000
                            WHEN later.value <> 0 AND later.value % 1000 = 0
                                THEN 500
                            ELSE 0
                        END
                ) AS on_annual_split
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
            AND n.members <@ a.members
        LEFT JOIN LATERAL (
            SELECT b.filing_id
            FROM annual_members b
            WHERE b.company_id = a.company_id
                AND b.statement = a.statement
                AND b.normalized_label = a.normalized_label
                AND b.axis = a.axis
                AND b.fiscal_tag = a.fiscal_tag
                AND b.filing_date > n.filing_date
                AND n.filing_date > a.filing_date
            ORDER BY b.filing_date, b.filing_id
            LIMIT 1
        ) next_annual ON TRUE
    ),
    nine_month_split AS MATERIALIZED (
        SELECT DISTINCT ON (
            company_id, statement, normalized_label, axis, fiscal_tag, filing_id
        )
            company_id,
            statement,
            normalized_label,
            axis,
            fiscal_tag,
            filing_id,
            nine_month_filing_id
        FROM nine_month_candidates
        WHERE same_members OR on_annual_split
        ORDER BY
            company_id, statement, normalized_label, axis, fiscal_tag, filing_id,
            on_annual_split DESC,
            nine_month_filing_date < filing_date DESC,
            ABS(nine_month_filing_date - filing_date), nine_month_filing_id
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
    -- A member of the year's chosen 10-K takes its Q4 from nine months on
    -- the same members even when its quarters are not shown, because they
    -- were only printed by a later 10-Q that recast the year.
    missing_quarters AS (
        (SELECT DISTINCT ON (
            a.company_id, a.statement, a.normalized_label, a.axis, a.member,
            a.fiscal_tag
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
        FROM annual_split split
        JOIN annual_members a
            ON a.filing_id = split.filing_id
            AND a.company_id = split.company_id
            AND a.statement = split.statement
            AND a.normalized_label = split.normalized_label
            AND a.axis = split.axis
            AND a.fiscal_tag = split.fiscal_tag
        LEFT JOIN quarterly_aggregation q
            ON q.company_id = a.company_id
            AND q.statement = a.statement
            AND q.normalized_label = a.normalized_label
            AND q.axis = a.axis
            AND q.member = a.member
            AND q.fiscal_tag = a.fiscal_tag
        LEFT JOIN nine_month_split nms
            ON nms.filing_id = split.filing_id
            AND nms.company_id = a.company_id
            AND nms.statement = a.statement
            AND nms.normalized_label = a.normalized_label
            AND nms.axis = a.axis
            AND nms.fiscal_tag = a.fiscal_tag
        LEFT JOIN nine_months nm
            ON nm.filing_id = nms.nine_month_filing_id
            AND nm.company_id = a.company_id
            AND nm.statement = a.statement
            AND nm.normalized_label = a.normalized_label
            AND nm.axis = a.axis
            AND nm.member = a.member
            AND nm.fiscal_tag = a.fiscal_tag
        WHERE
            (q.value IS NOT NULL OR (split.split_rank = 1 AND nm.value IS NOT NULL))
            AND a.statement != 'Balance Sheet'
            AND a.concept NOT IN (
                'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic',
                'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'
            )
            -- Per-share amounts are not annual−Σquarters. A mid-year stock
            -- split leaves the early quarters on another share basis.
            AND COALESCE(a.unit, '') NOT IN ('usdPerShare', 'usdeps')
        ORDER BY
            a.company_id, a.statement, a.normalized_label, a.axis, a.member,
            a.fiscal_tag, split.split_rank, a.is_duplicate, a.id)

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
            AND COALESCE(a.unit, '') NOT IN ('usdPerShare', 'usdeps')
            AND q.value IS NOT NULL
            AND NOT a.is_abstract
    )
    SELECT
        id,
        parent_id,
        company_id,
        filing_id,
        concept,
        label,
        normalized_label,
        COALESCE(restated_value, value) AS value,
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
        CASE WHEN restated_value IS NOT NULL THEN 'calculated' ELSE source_type END
            AS source_type
    FROM quarterly_filings_restated
    WHERE (NOT drop_ytd OR is_abstract)
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
        -- A section header filed only on the 10-K. Lines point at it, and a
        -- 10-Q never carries that row.
        OR is_abstract
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
