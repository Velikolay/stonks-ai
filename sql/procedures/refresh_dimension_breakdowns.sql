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
    -- A later filing reprints earlier members rounded to millions, so members
    -- coarser than their parent may miss it by half a unit each, up to 0.1%.
    --
    -- A fact stored with one dimension, or with none, is a flat list. Subset
    -- search of that list stays capped at 16 members. A fact stored with
    -- several dimensions is a cell of a cross-tab: it leaves the flat search,
    -- and its breakdown is the full set of siblings that share every other
    -- dimension and add up to the fact with exactly those dimensions. That
    -- set is not enumerated and is not capped.
    CREATE TEMP TABLE tmp_breakdown_rows ON COMMIT DROP AS
    SELECT
        ff.id,
        ff.company_id,
        ff.statement,
        ff.normalized_label,
        ff.axis,
        ff.member,
        ff.period_end,
        f.fiscal_year - (
            EXTRACT(YEAR FROM f.fiscal_period_end)::int
            - EXTRACT(YEAR FROM ff.period_end)::int
        ) AS fiscal_year,
        ff.value,
        CASE
            WHEN ff.value = 0 THEN 1
            WHEN ff.value % 1000000 = 0 THEN 1000000
            WHEN ff.value % 1000 = 0 THEN 1000
            ELSE 1
        END AS rounding
    FROM financial_facts_normalized ff
    JOIN filings f
        ON f.id = ff.fiscal_filing_id
    WHERE
        ff.company_id = ANY(company_ids)
        AND ff.form_type IN ('10-K', '10-K/A', '20-F', '20-F/A')
        AND NOT ff.is_duplicate
        AND NOT ff.is_abstract
        AND ff.value IS NOT NULL
        AND COALESCE(ff.unit, '') NOT ILIKE '%pershare%';
    ANALYZE tmp_breakdown_rows;

    -- One varying dimension at a time. The other dimensions are the parent
    -- context, and the children are kept only when the whole set adds up.
    CREATE TEMP TABLE tmp_context_breakdowns ON COMMIT DROP AS
    WITH dims AS (
        SELECT
            d.fact_id,
            d.ordinal,
            d.axis AS dim_axis,
            d.member AS dim_member
        FROM financial_fact_dimensions d
        JOIN tmp_breakdown_rows r
            ON r.id = d.fact_id
        WHERE NOT d.is_default
    ),
    signatures AS (
        SELECT
            fact_id,
            string_agg(
                dim_axis || E'\x1f' || dim_member,
                E'\x1e' ORDER BY dim_axis, dim_member, ordinal
            ) AS signature,
            COUNT(*) AS dim_count
        FROM dims
        GROUP BY fact_id
    ),
    facts AS (
        SELECT
            r.id,
            r.company_id,
            r.statement,
            r.normalized_label,
            r.axis,
            r.member,
            r.period_end,
            r.fiscal_year,
            r.value,
            r.rounding,
            s.signature,
            s.dim_count
        FROM tmp_breakdown_rows r
        JOIN signatures s
            ON s.fact_id = r.id
        WHERE r.axis <> '' AND r.value <> 0
    ),
    children AS (
        SELECT
            f.company_id,
            f.statement,
            f.normalized_label,
            f.axis AS display_axis,
            f.member AS display_member,
            f.period_end,
            f.fiscal_year,
            f.value,
            f.rounding,
            dropped.dim_axis AS varying_axis,
            string_agg(
                kept.dim_axis || E'\x1f' || kept.dim_member,
                E'\x1e' ORDER BY kept.dim_axis, kept.dim_member, kept.ordinal
            ) AS parent_signature
        FROM facts f
        JOIN dims dropped
            ON dropped.fact_id = f.id
        JOIN dims kept
            ON kept.fact_id = f.id
            AND kept.ordinal <> dropped.ordinal
        WHERE f.dim_count > 1
        GROUP BY
            f.company_id,
            f.statement,
            f.normalized_label,
            f.axis,
            f.member,
            f.period_end,
            f.fiscal_year,
            f.value,
            f.rounding,
            f.id,
            dropped.ordinal,
            dropped.dim_axis
    ),
    parents AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            axis AS display_axis,
            member AS display_member,
            period_end,
            fiscal_year,
            value,
            rounding,
            signature
        FROM facts
    ),
    unique_parents AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            period_end,
            signature,
            MIN(display_axis) AS display_axis,
            MIN(display_member) AS display_member,
            MIN(fiscal_year) AS fiscal_year,
            MIN(value) AS value,
            MIN(rounding) AS rounding
        FROM parents
        GROUP BY
            company_id, statement, normalized_label, period_end, signature
        HAVING
            COUNT(DISTINCT display_axis) = 1
            AND COUNT(DISTINCT display_member) = 1
            AND COUNT(DISTINCT value) = 1
    ),
    grouped AS (
        SELECT
            p.company_id,
            p.statement,
            p.normalized_label,
            p.display_axis AS axis,
            p.display_member AS parent_member,
            p.fiscal_year,
            p.value AS parent_value,
            p.rounding AS parent_rounding,
            array_agg(c.display_member ORDER BY c.display_member) AS members,
            SUM(c.value) AS members_total,
            MAX(c.rounding) AS members_rounding
        FROM children c
        JOIN unique_parents p
            ON p.company_id = c.company_id
            AND p.statement = c.statement
            AND p.normalized_label = c.normalized_label
            AND p.period_end = c.period_end
            AND p.signature = c.parent_signature
        WHERE
            c.display_axis = p.display_axis
            AND c.display_member <> p.display_member
        GROUP BY
            p.company_id,
            p.statement,
            p.normalized_label,
            p.display_axis,
            p.display_member,
            p.period_end,
            p.fiscal_year,
            p.value,
            p.rounding,
            c.varying_axis
        HAVING
            COUNT(*) >= 2
            AND COUNT(*) = COUNT(DISTINCT c.display_member)
    ),
    matched AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            axis,
            parent_member,
            members,
            fiscal_year
        FROM grouped
        WHERE ABS(parent_value - members_total) <= CASE
            WHEN members_rounding > parent_rounding THEN LEAST(
                cardinality(members) * members_rounding / 2,
                ABS(parent_value) / 1000
            )
            ELSE 0
        END
    )
    SELECT
        m.company_id,
        m.statement,
        m.normalized_label,
        m.axis,
        m.parent_member,
        m.members,
        m.fiscal_year
    FROM matched m
    WHERE (
        SELECT COUNT(DISTINCT other.fiscal_year)
        FROM matched other
        WHERE
            other.company_id = m.company_id
            AND other.statement = m.statement
            AND other.normalized_label = m.normalized_label
            AND other.axis = m.axis
            AND other.parent_member = m.parent_member
            AND other.members = m.members
    ) >= 2;
    ANALYZE tmp_context_breakdowns;

    CREATE TEMP TABLE tmp_dimension_breakdowns_new ON COMMIT DROP AS
    WITH reported AS (
        -- Every dimensional member, including cross-tab cells. Declared
        -- breakdowns match against this list. Subset search uses the flat one.
        SELECT *
        FROM tmp_breakdown_rows
        WHERE axis <> '' AND value <> 0
    ),
    multi_facts AS (
        SELECT
            r.company_id,
            r.statement,
            r.normalized_label,
            r.axis,
            r.period_end,
            r.id
        FROM reported r
        WHERE (
            SELECT COUNT(*)
            FROM financial_fact_dimensions d
            WHERE d.fact_id = r.id AND NOT d.is_default
        ) > 1
    ),
    shared_members AS (
        -- A member present on every cross-tab cell of one axis and period is
        -- the table's total, not a second cut. Netflix tags every region with
        -- Streaming. Dropping that member leaves the regions as one flat list.
        -- A real cross-tab has no such member.
        SELECT
            m.company_id,
            m.statement,
            m.normalized_label,
            m.axis,
            m.period_end,
            d.member
        FROM multi_facts m
        JOIN financial_fact_dimensions d
            ON d.fact_id = m.id AND NOT d.is_default
        GROUP BY
            m.company_id, m.statement, m.normalized_label, m.axis, m.period_end,
            d.member
        HAVING COUNT(DISTINCT m.id) = (
            SELECT COUNT(*)
            FROM multi_facts others
            WHERE
                others.company_id = m.company_id
                AND others.statement = m.statement
                AND others.normalized_label = m.normalized_label
                AND others.axis = m.axis
                AND others.period_end = m.period_end
        )
        AND COUNT(DISTINCT m.id) >= 2
        -- A member that is itself a row on this axis is a parent in the tree,
        -- not the table total. Its children stay out of the flat list.
        AND NOT EXISTS (
            SELECT 1
            FROM reported parent
            WHERE
                parent.company_id = m.company_id
                AND parent.statement = m.statement
                AND parent.normalized_label = m.normalized_label
                AND parent.axis = m.axis
                AND parent.period_end = m.period_end
                AND (
                    SELECT COUNT(*)
                    FROM financial_fact_dimensions pd
                    WHERE pd.fact_id = parent.id AND NOT pd.is_default
                ) = 1
                AND EXISTS (
                    SELECT 1
                    FROM financial_fact_dimensions pd
                    WHERE
                        pd.fact_id = parent.id
                        AND NOT pd.is_default
                        AND pd.member = d.member
                )
        )
    ),
    members AS (
        -- A zero member can join any set, so it would make every match ambiguous.
        -- A cell stays out of this list when more than one of its dimensions is
        -- a real slice. The axis default and a member shared by every cell do
        -- not count, and neither does a fact toward the 16-member cap.
        SELECT *
        FROM reported
        WHERE (
            SELECT COUNT(*)
            FROM financial_fact_dimensions d
            WHERE d.fact_id = reported.id
                AND NOT d.is_default
                AND NOT EXISTS (
                    SELECT 1
                    FROM shared_members s
                    WHERE
                        s.company_id = reported.company_id
                        AND s.statement = reported.statement
                        AND s.normalized_label = reported.normalized_label
                        AND s.axis = reported.axis
                        AND s.period_end = reported.period_end
                        AND s.member = d.member
                )
        ) <= 1
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
            array_agg(value ORDER BY member) AS amounts,
            array_agg(rounding ORDER BY member) AS roundings
        FROM members
        GROUP BY company_id, statement, normalized_label, axis, period_end, fiscal_year
    ),
    reported_groups AS (
        SELECT
            company_id,
            statement,
            normalized_label,
            axis,
            period_end,
            fiscal_year,
            array_agg(member ORDER BY member) AS names,
            array_agg(value ORDER BY member) AS amounts,
            array_agg(rounding ORDER BY member) AS roundings
        FROM reported
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
            m.value,
            m.rounding
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
            t.value,
            t.rounding
        FROM axis_groups g
        JOIN totals t
            ON t.company_id = g.company_id
            AND t.statement = g.statement
            AND t.normalized_label = g.normalized_label
            AND t.period_end = g.period_end
    ),
    reported_targets AS (
        -- Declared breakdowns name members the flat search left out.
        SELECT
            m.id,
            m.company_id,
            m.statement,
            m.normalized_label,
            m.axis,
            m.period_end,
            m.fiscal_year,
            m.member AS parent_member,
            m.value,
            m.rounding
        FROM reported m
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
            t.value,
            t.rounding
        FROM reported_groups g
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
            s.members_total,
            s.members_rounding
        FROM (
            SELECT *
            FROM axis_groups
            WHERE cardinality(names) BETWEEN 2 AND 16
        ) g
        CROSS JOIN LATERAL generate_series(1, (1 << cardinality(g.names)) - 1) AS mask
        CROSS JOIN LATERAL (
            SELECT
                array_agg(g.names[i] ORDER BY i) AS members,
                SUM(g.amounts[i]) AS members_total,
                MAX(g.roundings[i]) AS members_rounding
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
            AND ABS(t.value - s.members_total) <= CASE
                WHEN s.members_rounding > t.rounding THEN LEAST(
                    cardinality(s.members) * s.members_rounding / 2,
                    ABS(t.value) / 1000
                )
                ELSE 0
            END
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
        JOIN reported_groups g
            ON g.company_id = o.company_id
            AND g.axis = o.axis
            AND (o.normalized_label IS NULL OR o.normalized_label = g.normalized_label)
            AND g.names @> o.members
        JOIN reported_targets t
            ON t.company_id = g.company_id
            AND t.statement = g.statement
            AND t.normalized_label = g.normalized_label
            AND t.axis = g.axis
            AND t.period_end = g.period_end
            AND t.parent_member = o.parent_member
        WHERE
            cardinality(o.members) >= 2
            AND (
                SELECT ABS(t.value - SUM(u.amount)) <= CASE
                    WHEN MAX(u.rounding) > t.rounding THEN LEAST(
                        cardinality(o.members) * MAX(u.rounding) / 2,
                        ABS(t.value) / 1000
                    )
                    ELSE 0
                END
                FROM unnest(g.names, g.amounts, g.roundings)
                    AS u(name, amount, rounding)
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
            'inferred' AS source
        FROM tmp_context_breakdowns c
        WHERE NOT EXISTS (
            SELECT 1
            FROM overrides o
            WHERE
                o.company_id = c.company_id
                AND o.axis = c.axis
                AND o.parent_member = c.parent_member
                AND (
                    o.normalized_label IS NULL
                    OR o.normalized_label = c.normalized_label
                )
        )
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
