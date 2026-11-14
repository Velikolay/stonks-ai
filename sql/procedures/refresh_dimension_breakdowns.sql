CREATE OR REPLACE PROCEDURE refresh_dimension_breakdowns(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    -- Breakdowns: sets of members on one axis that add up exactly to another
    -- row in the same 10-K period, either a member or the undimensioned line.
    -- Only rows that survive dedup take part, so a breakdown holds on the
    -- values the presentation tables show. Per-share amounts are not additive.
    CREATE TEMP TABLE tmp_breakdown_rows ON COMMIT DROP AS
    SELECT
        ff.id,
        ff.company_id,
        ff.statement,
        ff.normalized_label,
        ff.axis,
        ff.member,
        ff.period_end,
        f.fiscal_year,
        ff.value
    FROM financial_facts_normalized ff
    JOIN filings f
        ON f.id = ff.filing_id
    WHERE
        ff.company_id = ANY(company_ids)
        AND ff.form_type IN ('10-K', '10-K/A')
        AND NOT ff.is_duplicate
        AND NOT ff.is_abstract
        AND ff.value IS NOT NULL
        AND COALESCE(ff.unit, '') NOT ILIKE '%pershare%';
    ANALYZE tmp_breakdown_rows;

    CREATE TEMP TABLE tmp_dimension_breakdowns_new ON COMMIT DROP AS
    WITH members AS (
        -- A zero member can join any set, so it would make every match ambiguous.
        SELECT *
        FROM tmp_breakdown_rows
        WHERE axis <> '' AND value <> 0
    ),
    totals AS (
        SELECT *
        FROM tmp_breakdown_rows
        WHERE axis = ''
    ),
    axis_groups AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            axis,
            period_end,
            fiscal_year,
            array_agg(member ORDER BY member) AS names,
            array_agg(value ORDER BY member) AS amounts
        FROM members
        GROUP BY company_id, statement, normalized_label, axis, period_end, fiscal_year
    ),
    targets AS (
        -- A member equal to the undimensioned value copies the statement line;
        -- it is not a parent of its siblings.
        SELECT
            m.id,
            m.company_id,
            m.statement,
            m.normalized_label,
            m.axis,
            m.period_end,
            m.fiscal_year,
            m.member AS parent_member,
            m.value
        FROM members m
        WHERE NOT EXISTS (
            SELECT 1
            FROM totals t
            WHERE
                t.company_id = m.company_id
                AND t.statement = m.statement
                AND t.normalized_label = m.normalized_label
                AND t.period_end = m.period_end
                AND t.value = m.value
        )
        UNION ALL
        SELECT
            t.id,
            g.company_id,
            g.statement,
            g.normalized_label,
            g.axis,
            g.period_end,
            g.fiscal_year,
            '' AS parent_member,
            t.value
        FROM axis_groups g
        JOIN totals t
            ON t.company_id = g.company_id
            AND t.statement = g.statement
            AND t.normalized_label = g.normalized_label
            AND t.period_end = g.period_end
    ),
    subsets AS MATERIALIZED (
        -- Wider groups are skipped because subset enumeration is 2^n.
        SELECT
            g.company_id,
            g.statement,
            g.normalized_label,
            g.axis,
            g.period_end,
            g.fiscal_year,
            s.members,
            s.members_total
        FROM (
            SELECT *
            FROM axis_groups
            WHERE cardinality(names) BETWEEN 2 AND 16
        ) g
        CROSS JOIN LATERAL generate_series(1, (1 << cardinality(g.names)) - 1) AS mask
        CROSS JOIN LATERAL (
            SELECT
                array_agg(g.names[i] ORDER BY i) AS members,
                SUM(g.amounts[i]) AS members_total
            FROM generate_subscripts(g.names, 1) AS i
            WHERE mask & (1 << (i - 1)) <> 0
        ) s
        WHERE cardinality(s.members) >= 2
    ),
    ties AS (
        -- Members with the same value in the same period, e.g. a region and a
        -- country that happen to coincide.
        SELECT
            x.company_id,
            x.statement,
            x.normalized_label,
            x.axis,
            x.period_end,
            x.member,
            y.member AS twin
        FROM members x
        JOIN members y
            ON y.company_id = x.company_id
            AND y.statement = x.statement
            AND y.normalized_label = x.normalized_label
            AND y.axis = x.axis
            AND y.period_end = x.period_end
            AND y.value = x.value
            AND y.member <> x.member
    ),
    matches AS (
        SELECT
            t.company_id,
            t.statement,
            t.normalized_label,
            t.axis,
            t.period_end,
            t.fiscal_year,
            t.parent_member,
            s.members,
            -- Swapping a member for its twin gives the same sum, so this
            -- period cannot tell the two sets apart.
            EXISTS (
                SELECT 1
                FROM ties x
                WHERE
                    x.company_id = t.company_id
                    AND x.statement = t.statement
                    AND x.normalized_label = t.normalized_label
                    AND x.axis = t.axis
                    AND x.period_end = t.period_end
                    AND x.member = ANY(s.members)
                    AND NOT (x.twin = ANY(s.members))
                    AND x.twin <> t.parent_member
            ) AS is_ambiguous
        FROM subsets s
        JOIN targets t
            ON t.company_id = s.company_id
            AND t.statement = s.statement
            AND t.normalized_label = s.normalized_label
            AND t.axis = s.axis
            AND t.period_end = s.period_end
            AND t.value = s.members_total
            AND NOT (t.parent_member = ANY(s.members))
    ),
    kept AS (
        -- A single matching 10-K can be a coincidence, so the same set must
        -- match in at least two years, not counting ambiguous ones. It then
        -- holds in every year where it adds up.
        SELECT m.*
        FROM matches m
        JOIN (
            SELECT company_id, statement, normalized_label, axis, parent_member, members
            FROM matches
            GROUP BY company_id, statement, normalized_label, axis, parent_member, members
            HAVING COUNT(DISTINCT fiscal_year) FILTER (WHERE NOT is_ambiguous) >= 2
        ) e
            USING (company_id, statement, normalized_label, axis, parent_member, members)
    ),
    immediate AS (
        -- When Products = iPhone + iPad, the line equals both {Products,
        -- Services} and {iPhone, iPad, Services}. Drop a set, in that period,
        -- when swapping some of its members for their own parent yields
        -- another kept set for the same parent.
        SELECT k.*
        FROM kept k
        WHERE NOT EXISTS (
            SELECT 1
            FROM kept q
            JOIN kept p
                ON p.company_id = k.company_id
                AND p.statement = k.statement
                AND p.normalized_label = k.normalized_label
                AND p.axis = k.axis
                AND p.period_end = k.period_end
                AND p.parent_member = k.parent_member
            WHERE
                q.company_id = k.company_id
                AND q.statement = k.statement
                AND q.normalized_label = k.normalized_label
                AND q.axis = k.axis
                AND q.period_end = k.period_end
                AND q.parent_member <> k.parent_member
                AND NOT (q.parent_member = ANY(k.members))
                AND q.members <@ k.members
                AND q.parent_member = ANY(p.members)
                AND array_remove(p.members, q.parent_member) <@ k.members
                AND NOT (array_remove(p.members, q.parent_member) && q.members)
                AND cardinality(p.members)
                    = cardinality(k.members) - cardinality(q.members) + 1
        )
    ),
    overrides AS (
        SELECT
            o.company_id,
            o.normalized_label,
            o.axis,
            o.parent_member,
            COALESCE(
                (SELECT array_agg(m ORDER BY m) FROM unnest(o.members) AS m),
                '{}'
            ) AS members
        FROM dimension_breakdown_overrides o
        WHERE o.company_id = ANY(company_ids)
    ),
    inferred AS (
        -- Override rows for a parent on an axis are its complete list of
        -- breakdowns, so they replace everything inferred for it. A row with
        -- no members leaves the parent without breakdowns.
        SELECT
            i.company_id,
            i.statement,
            i.normalized_label,
            i.axis,
            i.parent_member,
            i.members,
            i.fiscal_year
        FROM immediate i
        WHERE NOT EXISTS (
            SELECT 1
            FROM overrides o
            WHERE
                o.company_id = i.company_id
                AND o.axis = i.axis
                AND o.parent_member = i.parent_member
                AND (
                    o.normalized_label IS NULL
                    OR o.normalized_label = i.normalized_label
                )
        )
    ),
    declared AS (
        -- A declared breakdown skips the two-year rule and the expansion rule,
        -- but like any other it only lands where its members add up.
        SELECT
            t.company_id,
            t.statement,
            t.normalized_label,
            t.axis,
            t.parent_member,
            o.members,
            t.fiscal_year
        FROM overrides o
        JOIN axis_groups g
            ON g.company_id = o.company_id
            AND g.axis = o.axis
            AND (o.normalized_label IS NULL OR o.normalized_label = g.normalized_label)
            AND g.names @> o.members
        JOIN targets t
            ON t.company_id = g.company_id
            AND t.statement = g.statement
            AND t.normalized_label = g.normalized_label
            AND t.axis = g.axis
            AND t.period_end = g.period_end
            AND t.parent_member = o.parent_member
        WHERE
            cardinality(o.members) >= 2
            AND t.value = (
                SELECT SUM(u.amount)
                FROM unnest(g.names, g.amounts) AS u(name, amount)
                WHERE u.name = ANY(o.members)
            )
    ),
    exact AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            axis,
            parent_member,
            members,
            fiscal_year,
            'inferred' AS source
        FROM inferred
        UNION
        SELECT
            company_id,
            statement,
            normalized_label,
            axis,
            parent_member,
            members,
            fiscal_year,
            'override' AS source
        FROM declared
    ),
    exact_years AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            axis,
            parent_member,
            members,
            -- Prefer override when the same set was both inferred and declared.
            CASE
                WHEN bool_or(source = 'override') THEN 'override'
                ELSE 'inferred'
            END AS source,
            array_agg(DISTINCT fiscal_year ORDER BY fiscal_year) AS fiscal_years
        FROM exact
        GROUP BY company_id, statement, normalized_label, axis, parent_member, members
    )
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        parent_member,
        members,
        fiscal_years,
        source
    FROM exact_years;

    DELETE FROM dimension_breakdowns
    WHERE company_id = ANY(company_ids);

    INSERT INTO dimension_breakdowns (
        company_id,
        statement,
        normalized_label,
        axis,
        parent_member,
        members,
        fiscal_years,
        source
    )
    SELECT
        company_id,
        statement,
        normalized_label,
        axis,
        parent_member,
        members,
        fiscal_years,
        source
    FROM tmp_dimension_breakdowns_new;
END;
$$;
