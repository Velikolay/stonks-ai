CREATE OR REPLACE PROCEDURE refresh_financial_facts_normalized(company_ids int[])
LANGUAGE plpgsql
AS $$
BEGIN
    IF company_ids IS NULL OR array_length(company_ids, 1) IS NULL THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE tmp_financial_facts_normalized_new ON COMMIT DROP AS
    WITH RECURSIVE financial_facts_base AS (
        SELECT
            ff.id,
            ff.filing_id,
            ff.company_id,
            ff.form_type,
            COALESCE(ffo.concept, ff.concept) AS concept,
            ff.label,
            ff.is_abstract,
            ff.value,
            ff.comparative_value,
            COALESCE(ffo.weight, ff.weight) AS weight,
            ff.print_sign,
            ff.unit,
            COALESCE(ffo.axis, ff.axis) AS axis,
            COALESCE(ffo.member, ff.member) AS member,
            COALESCE(ffo.member_label, ff.member_label) AS member_label,
            ff.statement,
            ff.period_end,
            ff.comparative_period_end,
            ff.period,
            ff.position,
            ff.parent_id,
            ff.abstract_id,
            ffo.fact_override_id
        FROM financial_facts ff
        LEFT JOIN financial_facts_overridden ffo
            ON ffo.id = ff.id
            AND ffo.company_id = ff.company_id
        WHERE ff.company_id = ANY(company_ids)
    ),
    conflicting_fact_keys AS (
        SELECT unnest(array_agg(id)) AS id
        FROM financial_facts_base
        GROUP BY company_id, statement, concept, label, axis, member, member_label, period_end, form_type
        HAVING COUNT(DISTINCT value) > 1
    ),
    financial_facts_overridden_cte AS (
        SELECT ffb.*
        FROM financial_facts_base ffb
        WHERE ffb.id NOT IN (SELECT id FROM conflicting_fact_keys)
    ),
    hierarchy_normalization_cte AS (
        SELECT *
        FROM hierarchy_normalization
        WHERE company_id = ANY(company_ids)
    ),
    normalized_facts AS (
        SELECT
            ff.id,
            ff.company_id,
            ff.filing_id,
            ff.form_type,
            ff.concept,
            ff.label,
            COALESCE(cno.normalized_label, cn.normalized_label, ff.label) AS normalized_label,
            ff.is_abstract,
            CASE
                WHEN ff.weight * COALESCE(cno.weight, cn.weight, ff.weight) < 0
                THEN -ff.value
                ELSE ff.value
            END AS value,
            CASE
                WHEN ff.weight * COALESCE(cno.weight, cn.weight, ff.weight) < 0
                THEN -ff.comparative_value
                ELSE ff.comparative_value
            END AS comparative_value,
            COALESCE(cno.weight, cn.weight, ff.weight) AS weight,
            ff.print_sign,
            COALESCE(cno.unit, cn.unit, ff.unit) AS unit,
            COALESCE(dn.normalized_axis_label, ff.axis) AS axis,
            COALESCE(dn.normalized_member_label, ff.member_label) AS member,
            ff.statement,
            ff.period_end,
            ff.comparative_period_end,
            ff.period,
            ff.position,
            CASE
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NOT NULL THEN
                    ffp.id
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    abs(hashtextextended(
                        ff.statement || '|' || COALESCE(cno.parent_concept, hn.parent_concept_source) || '|'
                        || ff.filing_id::text || '|' || ff.company_id::text,
                        0
                    ))
                ELSE
                    ff.parent_id
            END AS parent_id,
            CASE
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    COALESCE(cno.parent_concept, hn.parent_concept_source)
                ELSE
                    NULL
            END AS parent_concept,
            CASE
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NOT NULL THEN
                    ffa.id
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                    abs(hashtextextended(
                        ff.statement || '|' || cno.abstract_concept || '|'
                        || ff.filing_id::text || '|' || ff.company_id::text,
                        0
                    ))
                ELSE
                    ff.abstract_id
            END AS abstract_id,
            CASE
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                    cno.abstract_concept
                ELSE
                    NULL
            END AS abstract_concept,
            FALSE AS is_synthetic
        FROM financial_facts_overridden_cte ff
        LEFT JOIN LATERAL (
            SELECT *
            FROM concept_normalization_overrides o
            WHERE
                o.statement = ff.statement
                AND o.concept = ff.concept
                AND (o.company_id = ff.company_id OR o.is_global = TRUE)
            ORDER BY (o.company_id = ff.company_id) DESC
            LIMIT 1
        ) cno ON TRUE
        LEFT JOIN hierarchy_normalization_cte hn
            ON hn.company_id = ff.company_id
            AND hn.statement = ff.statement
            AND hn.concept = ff.concept
        LEFT JOIN financial_facts_overridden_cte ffp
            ON ffp.company_id = ff.company_id
            AND ffp.filing_id = ff.filing_id
            AND ffp.statement = ff.statement
            AND ffp.concept = COALESCE(cno.parent_concept, hn.parent_concept)
        LEFT JOIN financial_facts_overridden_cte ffa
            ON ffa.company_id = ff.company_id
            AND ffa.filing_id = ff.filing_id
            AND ffa.statement = ff.statement
            AND ffa.concept = cno.abstract_concept
        LEFT JOIN concept_normalization cn
            ON ff.company_id = cn.company_id
            AND ff.statement = cn.statement
            AND ff.concept = cn.concept
        LEFT JOIN dimension_normalization dn
            ON ff.company_id = dn.company_id
            AND ff.statement = dn.statement
            AND COALESCE(cno.normalized_label, cn.normalized_label, ff.label) = dn.normalized_label
            AND ff.axis = dn.axis
            AND ff.member = dn.member
            AND ff.member_label = dn.member_label

        UNION

        SELECT
            new.id AS id,
            f.company_id,
            f.filing_id,
            f.form_type,
            cno.concept,
            cno.normalized_label AS label,
            cno.normalized_label,
            cno.is_abstract,
            0 AS value,
            0 AS comparative_value,
            cno.weight,
            NULL::numeric AS print_sign,
            cno.unit,
            '' AS axis,
            '' AS member,
            cno.statement,
            f.period_end,
            f.comparative_period_end,
            f.period,
            99 AS position,
            CASE
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NOT NULL THEN
                    ffp.id
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    abs(hashtextextended(
                        f.statement || '|' || COALESCE(cno.parent_concept, hn.parent_concept_source) || '|'
                        || f.filing_id::text || '|' || f.company_id::text,
                        0
                    ))
                ELSE
                    NULL
            END AS parent_id,
            CASE
                WHEN COALESCE(cno.parent_concept, hn.parent_concept) IS NOT NULL AND ffp.id IS NULL THEN
                    COALESCE(cno.parent_concept, hn.parent_concept_source)
                ELSE
                    NULL
            END AS parent_concept,
            CASE
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NOT NULL THEN
                    ffa.id
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                    abs(hashtextextended(
                        f.statement || '|' || cno.abstract_concept || '|'
                        || f.filing_id::text || '|' || f.company_id::text,
                        0
                    ))
                ELSE
                    NULL
            END AS abstract_id,
            CASE
                WHEN cno.abstract_concept IS NOT NULL AND ffa.id IS NULL THEN
                    cno.abstract_concept
                ELSE
                    NULL
            END AS abstract_concept,
            TRUE AS is_synthetic
        FROM normalized_facts f
        JOIN LATERAL (
            VALUES
                (f.parent_concept, f.parent_id),
                (f.abstract_concept, f.abstract_id)
        ) AS new(concept, id)
            ON new.concept IS NOT NULL
        JOIN LATERAL (
            SELECT *
            FROM concept_normalization_overrides o
            WHERE
                o.statement = f.statement
                AND o.concept = new.concept
                AND (o.company_id = f.company_id OR o.is_global = TRUE)
            ORDER BY (o.company_id = f.company_id) DESC
            LIMIT 1
        ) cno ON TRUE
        LEFT JOIN hierarchy_normalization_cte hn
            ON hn.company_id = f.company_id
            AND hn.statement = f.statement
            AND hn.concept = new.concept
        LEFT JOIN financial_facts_overridden_cte ffp
            ON ffp.company_id = f.company_id
            AND ffp.filing_id = f.filing_id
            AND ffp.statement = f.statement
            AND ffp.concept = COALESCE(cno.parent_concept, hn.parent_concept)
        LEFT JOIN financial_facts_overridden_cte ffa
            ON ffa.company_id = f.company_id
            AND ffa.filing_id = f.filing_id
            AND ffa.statement = f.statement
            AND ffa.concept = cno.abstract_concept
    ),
    synthetic_rollup AS (
        SELECT
            nf.id,
            nf.parent_id,
            nf.value * nf.weight AS contrib_value,
            nf.comparative_value * nf.weight AS contrib_comparative_value
        FROM normalized_facts nf
        WHERE
            NOT nf.is_abstract
            AND NOT nf.is_synthetic

        UNION ALL

        SELECT
            nf.id,
            nf.parent_id,
            sr.contrib_value * COALESCE(nf.weight, 1) AS contrib_value,
            sr.contrib_comparative_value * COALESCE(nf.weight, 1) AS contrib_comparative_value
        FROM synthetic_rollup sr
        JOIN normalized_facts nf
            ON nf.id = sr.parent_id
        WHERE
            NOT nf.is_abstract
            AND nf.is_synthetic
    ),
    rolled_up_facts AS (
        SELECT
            nf.id,
            nf.company_id,
            nf.filing_id,
            nf.form_type,
            nf.concept,
            nf.label,
            nf.normalized_label,
            nf.is_abstract,
            CASE
                WHEN nf.is_synthetic THEN SUM(sr.contrib_value)
                ELSE nf.value
            END AS value,
            CASE
                WHEN nf.is_synthetic THEN SUM(sr.contrib_comparative_value)
                ELSE nf.comparative_value
            END AS comparative_value,
            nf.weight,
            nf.print_sign,
            nf.unit,
            nf.axis,
            nf.member,
            nf.statement,
            nf.period_end,
            nf.comparative_period_end,
            nf.period,
            nf.position,
            nf.parent_id,
            nf.abstract_id,
            nf.is_synthetic
        FROM normalized_facts nf
        LEFT JOIN synthetic_rollup sr
            ON sr.id = nf.id
        GROUP BY
            nf.id,
            nf.company_id,
            nf.filing_id,
            nf.form_type,
            nf.concept,
            nf.label,
            nf.normalized_label,
            nf.is_abstract,
            nf.value,
            nf.comparative_value,
            nf.weight,
            nf.print_sign,
            nf.unit,
            nf.axis,
            nf.member,
            nf.statement,
            nf.period_end,
            nf.comparative_period_end,
            nf.period,
            nf.position,
            nf.parent_id,
            nf.abstract_id,
            nf.is_synthetic
    ),
    distinct_rolled_up_facts AS (
        SELECT DISTINCT ON (
            company_id, statement, normalized_label, axis, member, value, period_end
        )
            *
        FROM rolled_up_facts
        ORDER BY company_id, statement, normalized_label, axis, member, value, period_end, id
    ),
    chain_walk AS (
        SELECT
            r.id,
            r.id AS chain_root_id,
            r.company_id,
            r.statement,
            r.concept,
            r.normalized_label,
            r.axis,
            r.member,
            r.period_end,
            r.value,
            r.comparative_value,
            r.comparative_period_end,
            1::bigint AS depth
        FROM rolled_up_facts r
        WHERE r.comparative_value IS NOT NULL
          AND r.comparative_period_end IS NOT NULL

        UNION ALL

        SELECT
            COALESCE(r_value.id, r_concept.id) AS id,
            c.chain_root_id,
            COALESCE(r_value.company_id, r_concept.company_id) AS company_id,
            COALESCE(r_value.statement, r_concept.statement) AS statement,
            COALESCE(r_value.concept, r_concept.concept) AS concept,
            COALESCE(r_value.normalized_label, r_concept.normalized_label) AS normalized_label,
            COALESCE(r_value.axis, r_concept.axis) AS axis,
            COALESCE(r_value.member, r_concept.member) AS member,
            COALESCE(r_value.period_end, r_concept.period_end) AS period_end,
            COALESCE(r_value.value, r_concept.value) AS value,
            COALESCE(r_value.comparative_value, r_concept.comparative_value) AS comparative_value,
            COALESCE(r_value.comparative_period_end, r_concept.comparative_period_end) AS comparative_period_end,
            c.depth + 1
        FROM chain_walk c
        -- use value match with priority
        LEFT JOIN distinct_rolled_up_facts r_value
            ON r_value.company_id = c.company_id
            AND r_value.statement = c.statement
            AND r_value.normalized_label = c.normalized_label
            AND r_value.axis = c.axis
            AND r_value.member = c.member
            AND r_value.value = c.comparative_value
            AND r_value.period_end = c.comparative_period_end
        -- fallback to concept match if value is not found
        LEFT JOIN distinct_rolled_up_facts r_concept
            ON r_concept.company_id = c.company_id
            AND r_concept.statement = c.statement
            AND r_concept.normalized_label = c.normalized_label
            AND r_concept.axis = c.axis
            AND r_concept.member = c.member
            AND r_concept.concept = c.concept
            AND r_concept.period_end = c.comparative_period_end
            AND r_value.id IS NULL

        WHERE r_value.id IS NOT NULL
        OR r_concept.id IS NOT NULL
    ),
    chain_sizes AS (
        SELECT chain_root_id, MAX(depth) AS chain_size
        FROM chain_walk
        GROUP BY chain_root_id
    ),
    chain_sizes_per_id AS (
        SELECT
            r.id,
            id_chain.chain_size
        FROM rolled_up_facts r
        LEFT JOIN (
            SELECT cw.id, MAX(cs.chain_size) AS chain_size
            FROM chain_walk cw
            JOIN chain_sizes cs ON cs.chain_root_id = cw.chain_root_id
            GROUP BY cw.id
        ) id_chain ON id_chain.id = r.id
    ),
    deduped AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.company_id, r.statement, r.normalized_label,
                             r.axis, r.member, r.period_end
                ORDER BY
                    -- prefer amendments over original filings
                    CASE WHEN r.form_type IN ('10-K/A', '10-Q/A') THEN 0 ELSE 1 END,
                    -- prefer longer chains
                    COALESCE(cs.chain_size, 1) DESC,
                    r.id ASC
            ) AS rn
        FROM rolled_up_facts r
        LEFT JOIN chain_sizes_per_id cs ON cs.id = r.id
    ),
    deduped_by_id AS (
        SELECT DISTINCT ON (id)
            id,
            company_id,
            filing_id,
            form_type,
            concept,
            label,
            normalized_label,
            is_abstract,
            value,
            comparative_value,
            weight,
            print_sign,
            unit,
            axis,
            member,
            statement,
            period_end,
            comparative_period_end,
            period,
            position,
            parent_id,
            abstract_id,
            is_synthetic,
            rn > 1 AS is_duplicate
        FROM deduped
        ORDER BY id, is_duplicate ASC
    )
    SELECT *, NULL::jsonb AS breakdowns FROM deduped_by_id;

    -- Breakdowns: sets of members on one axis that add up exactly to another
    -- row in the same period, either a member or the undimensioned line. Only
    -- rows that survive dedup take part, so a breakdown holds on the values the
    -- presentation tables show. Per-share amounts are not additive. A known set
    -- that falls short in some period is attached there as partial.
    CREATE TEMP TABLE tmp_breakdown_rows ON COMMIT DROP AS
    SELECT
        id,
        company_id,
        statement,
        normalized_label,
        axis,
        member,
        period_end,
        form_type IN ('10-K', '10-K/A') AS is_annual,
        value
    FROM tmp_financial_facts_normalized_new
    WHERE
        NOT is_duplicate
        AND NOT is_abstract
        AND value IS NOT NULL
        AND COALESCE(unit, '') NOT ILIKE '%pershare%';
    ANALYZE tmp_breakdown_rows;

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
            array_agg(member ORDER BY member) AS names,
            array_agg(value ORDER BY member) AS amounts
        FROM members
        GROUP BY company_id, statement, normalized_label, axis, period_end
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
            m.is_annual,
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
            t.is_annual,
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
            t.id,
            t.company_id,
            t.statement,
            t.normalized_label,
            t.axis,
            t.period_end,
            t.is_annual,
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
        -- match in at least two, not counting ambiguous ones. It then holds in
        -- every period where it adds up.
        SELECT m.*
        FROM matches m
        JOIN (
            SELECT company_id, statement, normalized_label, axis, parent_member, members
            FROM matches
            GROUP BY company_id, statement, normalized_label, axis, parent_member, members
            HAVING COUNT(*) FILTER (WHERE is_annual AND NOT is_ambiguous) >= 2
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
            i.id,
            i.company_id,
            i.statement,
            i.normalized_label,
            i.axis,
            i.parent_member,
            i.members
        FROM immediate i
        WHERE NOT EXISTS (
            SELECT 1
            FROM overrides o
            WHERE
                o.company_id = i.company_id
                AND o.axis = i.axis
                AND o.parent_member = i.parent_member
                AND (o.normalized_label IS NULL OR o.normalized_label = i.normalized_label)
        )
    ),
    declared AS (
        -- A declared breakdown skips the two-10-K rule and the expansion rule,
        -- but like any other it only lands where its members add up.
        SELECT
            t.id,
            t.company_id,
            t.statement,
            t.normalized_label,
            t.axis,
            t.parent_member,
            o.members
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
        SELECT * FROM inferred
        UNION
        SELECT * FROM declared
    ),
    partial AS (
        -- A set that adds up elsewhere but falls short in this period, so a
        -- split stays continuous across years. The members present (at least
        -- two, none negative) are below the parent; the rest is unallocated.
        SELECT t.id, t.axis, array_agg(u.name ORDER BY u.name) AS members
        FROM (
            SELECT DISTINCT
                company_id, statement, normalized_label, axis, parent_member, members
            FROM exact
        ) k
        JOIN targets t
            ON t.company_id = k.company_id
            AND t.statement = k.statement
            AND t.normalized_label = k.normalized_label
            AND t.axis = k.axis
            AND t.parent_member = k.parent_member
        JOIN axis_groups g
            ON g.company_id = t.company_id
            AND g.statement = t.statement
            AND g.normalized_label = t.normalized_label
            AND g.axis = t.axis
            AND g.period_end = t.period_end
        CROSS JOIN LATERAL unnest(g.names, g.amounts) AS u(name, amount)
        WHERE
            u.name = ANY(k.members)
            AND t.value > 0
        GROUP BY t.id, t.axis, t.value, k.members
        HAVING
            COUNT(*) >= 2
            AND MIN(u.amount) >= 0
            AND SUM(u.amount) < t.value
    ),
    breakdowns AS (
        SELECT
            id,
            jsonb_agg(
                jsonb_build_object(
                    'axis', axis, 'members', to_jsonb(members), 'partial', partial
                )
                ORDER BY partial, axis, cardinality(members), members
            ) AS breakdowns
        FROM (
            SELECT DISTINCT id, axis, members, false AS partial FROM exact
            UNION
            SELECT id, axis, members, true FROM partial
        ) b
        GROUP BY id
    )
    UPDATE tmp_financial_facts_normalized_new t
    SET breakdowns = b.breakdowns
    FROM breakdowns b
    WHERE t.id = b.id;

    DELETE FROM financial_facts_normalized ff
    WHERE
        ff.company_id = ANY(company_ids)
        AND NOT EXISTS (
            SELECT 1
            FROM tmp_financial_facts_normalized_new t
            WHERE t.id = ff.id
        );

    INSERT INTO financial_facts_normalized (
        id,
        company_id,
        filing_id,
        form_type,
        concept,
        label,
        normalized_label,
        is_abstract,
        value,
        comparative_value,
        weight,
        print_sign,
        unit,
        axis,
        member,
        statement,
        period_end,
        comparative_period_end,
        period,
        position,
        parent_id,
        abstract_id,
        is_synthetic,
        is_duplicate,
        breakdowns
    )
    SELECT
        id,
        company_id,
        filing_id,
        form_type,
        concept,
        label,
        normalized_label,
        is_abstract,
        value,
        comparative_value,
        weight,
        print_sign,
        unit,
        axis,
        member,
        statement,
        period_end,
        comparative_period_end,
        period,
        position,
        parent_id,
        abstract_id,
        is_synthetic,
        is_duplicate,
        breakdowns
    FROM tmp_financial_facts_normalized_new
    ON CONFLICT (id) DO UPDATE
    SET
        company_id = EXCLUDED.company_id,
        filing_id = EXCLUDED.filing_id,
        form_type = EXCLUDED.form_type,
        concept = EXCLUDED.concept,
        label = EXCLUDED.label,
        normalized_label = EXCLUDED.normalized_label,
        is_abstract = EXCLUDED.is_abstract,
        value = EXCLUDED.value,
        comparative_value = EXCLUDED.comparative_value,
        weight = EXCLUDED.weight,
        print_sign = EXCLUDED.print_sign,
        unit = EXCLUDED.unit,
        axis = EXCLUDED.axis,
        member = EXCLUDED.member,
        statement = EXCLUDED.statement,
        period_end = EXCLUDED.period_end,
        comparative_period_end = EXCLUDED.comparative_period_end,
        period = EXCLUDED.period,
        position = EXCLUDED.position,
        parent_id = EXCLUDED.parent_id,
        abstract_id = EXCLUDED.abstract_id,
        is_synthetic = EXCLUDED.is_synthetic,
        is_duplicate = EXCLUDED.is_duplicate,
        breakdowns = EXCLUDED.breakdowns;
END;
$$;
