"""Stage 4: infer canonical axis and member labels, then apply dimension overrides.

Reads the overridden facts (clones included), ``concept_normalization`` and
the concept and dimension overrides; writes ``dimension_normalization``, one
row per ``(line, axis, member, member label)``.

* Grouping: members of one line that share a member name, a member label or
  an override label (ignoring case) are one member; the override, else the
  latest label, names it.
* Chaining: on the Product and Geographical axes, a member whose comparative
  value equals another member's value for that earlier period, and which
  never coexists with it, is a renamed member.

Both kinds of group are then merged wherever they share a member, and each
merged group takes one label: company override, global override, member over
axis override, then the latest.
"""

from __future__ import annotations

import polars as pl

from ..inputs import CompanyData
from ..io.schemas import DIMENSION_NORMALIZATION
from ..rules import ordering
from ..rules.facts import ConflictRule, drop_conflicting, with_fact_overrides
from ..rules.graph import component_frame
from ..rules.ids import md5_hex

LINE = ["company_id", "statement", "normalized_label"]
MEMBER = [*LINE, "axis", "member", "member_label"]
#: The cuts chaining links across years; other axes keep their own members.
CHAINED_AXES = ("Product", "Geographical")
OVERRIDE_COLUMNS = ["normalized_axis_label", "normalized_member_label"]
STATE = ["overridden", "override_priority", "override_level"]


def md5_expr(parts: list[pl.Expr], source: str) -> pl.Expr:
    """``md5(p1 || '|' || ... || '|' || source)``; a NULL part gives NULL."""
    return pl.concat_str([*parts, pl.lit(source)], separator="|").map_elements(
        md5_hex, return_dtype=pl.String, skip_nulls=True
    )


def override_rank() -> list[ordering.Order]:
    """Company before global before none; member before axis override."""
    return [
        ordering.asc(
            pl.when(pl.col("override_priority") == "company")
            .then(1)
            .when(pl.col("override_priority") == "global")
            .then(2)
            .otherwise(3)
        ),
        ordering.asc(
            pl.when(pl.col("override_level") == "member")
            .then(1)
            .when(pl.col("override_level") == "axis")
            .then(2)
            .otherwise(3)
        ),
    ]


def dimension_facts(data: CompanyData) -> pl.DataFrame:
    """Dimensioned facts with their concept's normalized label.

    The label precedence here puts an override ahead of the inferred label.
    """
    facts = drop_conflicting(
        with_fact_overrides(data, include_clones=True), ConflictRule.ANY_FILING
    ).filter(pl.col("axis") != "")
    overrides = data.table("concept_normalization_overrides")
    company = overrides.select(
        "company_id",
        "statement",
        "concept",
        pl.col("normalized_label").alias("c_label"),
    )
    global_ = overrides.filter(pl.col("is_global")).select(
        "statement", "concept", pl.col("normalized_label").alias("g_label")
    )
    inferred = data.table("concept_normalization").select(
        "company_id",
        "statement",
        "concept",
        pl.col("normalized_label").alias("n_label"),
    )
    labelled = (
        facts.join(inferred, on=["company_id", "statement", "concept"], how="left")
        .join(company, on=["company_id", "statement", "concept"], how="left")
        .join(global_, on=["statement", "concept"], how="left")
    )
    return labelled.with_columns(
        pl.coalesce("c_label", "g_label", "n_label", "label").alias("normalized_label")
    ).drop("c_label", "g_label", "n_label")


def member_overrides(members: pl.DataFrame, overrides: pl.DataFrame) -> pl.DataFrame:
    """The winning dimension override per member, left-joined.

    An override matches on axis and, when set, member and member label. The
    company's row beats a global one, then the more specific, then the newest.
    """
    rules = overrides.select(
        pl.col("id").alias("dno_id"),
        pl.col("company_id").alias("dno_company_id"),
        "axis",
        pl.col("member").alias("dno_member"),
        pl.col("member_label").alias("dno_member_label"),
        pl.col("is_global").alias("dno_is_global"),
        "normalized_axis_label",
        "normalized_member_label",
        pl.col("updated_at").alias("dno_updated_at"),
    )
    keys = ["company_id", "axis", "member", "member_label"]
    combos = members.select(keys).unique()
    pairs = combos.join(rules, on="axis", how="inner").filter(
        ((pl.col("dno_company_id") == pl.col("company_id")) | pl.col("dno_is_global"))
        & (pl.col("dno_member").is_null() | (pl.col("dno_member") == pl.col("member")))
        & (
            pl.col("dno_member_label").is_null()
            | (pl.col("dno_member_label") == pl.col("member_label"))
        )
    )
    best = ordering.distinct_on(
        pairs,
        keys,
        [
            ordering.desc(pl.col("dno_company_id") == pl.col("company_id")),
            ordering.desc(pl.col("dno_member").is_not_null()),
            ordering.desc(pl.col("dno_member_label").is_not_null()),
            ordering.desc("dno_updated_at"),
        ],
    ).select(*keys, "dno_id", "dno_is_global", *OVERRIDE_COLUMNS)
    return members.join(best, on=keys, how="left")


def member_base(facts: pl.DataFrame, overrides: pl.DataFrame) -> pl.DataFrame:
    """One row per member of a line: its override and its latest period."""
    latest = ordering.distinct_on(facts, MEMBER, [ordering.desc("period_end")]).select(
        *MEMBER, "period_end"
    )
    with_override = member_overrides(latest, overrides)
    return with_override.with_columns(
        pl.col("dno_id").is_not_null().alias("overridden"),
        pl.when(~pl.col("dno_is_global"))
        .then(pl.lit("company"))
        .when(pl.col("dno_is_global"))
        .then(pl.lit("global"))
        .alias("override_priority"),
        pl.when(pl.col("normalized_member_label").is_not_null())
        .then(pl.lit("member"))
        .when(pl.col("normalized_axis_label").is_not_null())
        .then(pl.lit("axis"))
        .alias("override_level"),
        member_id().alias("id"),
    ).drop("dno_id", "dno_is_global")


def member_id() -> pl.Expr:
    """md5 of the member and its override labels, as the grouping node id."""
    parts = [
        *(pl.col(c).cast(pl.String) for c in MEMBER),
        pl.col("normalized_axis_label").fill_null(""),
        pl.col("normalized_member_label").fill_null(""),
    ]
    return md5_expr(parts, "grouping")


def member_groups(base: pl.DataFrame) -> pl.DataFrame:
    """Members that share a name, label or override label, as one group each.

    The same label on two axes that overrides sent to different cuts stays
    two members. Each row gets its group and its candidate canonical labels.
    """
    keys = (
        pl.concat(
            [
                base.select("id", *LINE, pl.col(c).str.to_lowercase().alias("key"))
                for c in ("member", "member_label", "normalized_member_label")
            ]
        )
        .drop_nulls("key")
        .unique()
    )
    axes = base.select("id", "normalized_axis_label")
    edges = (
        keys.join(keys, on=[*LINE, "key"], suffix="_2")
        .join(axes, on="id")
        .join(axes, left_on="id_2", right_on="id", suffix="_2")
        .filter(
            pl.col("normalized_axis_label").is_null()
            | pl.col("normalized_axis_label_2").is_null()
            | (pl.col("normalized_axis_label") == pl.col("normalized_axis_label_2"))
        )
    )
    groups = component_frame(base["id"], edges["id"], edges["id_2"], "id", "group_id")
    return base.join(groups, on="id").with_columns(
        pl.coalesce("normalized_axis_label", "axis").alias("canonical_axis"),
        pl.coalesce("normalized_member_label", "member_label").alias(
            "canonical_member"
        ),
        pl.col("period_end").max().over("group_id").alias("group_max_period_end"),
    )


def grouping(rows: pl.DataFrame) -> pl.DataFrame:
    """Each group labelled by its best member: override first, then the latest.

    Members last reported in the same period tie; the first in text order
    wins.
    """
    canonical = ordering.distinct_on(
        rows,
        ["group_id"],
        [
            *override_rank(),
            ordering.desc("period_end"),
            ordering.asc(ordering.by_text("canonical_axis")),
            ordering.asc(ordering.by_text("canonical_member")),
        ],
    ).select(
        "group_id",
        pl.col("canonical_axis").alias("c_axis"),
        pl.col("canonical_member").alias("c_member"),
        "group_max_period_end",
        *(pl.col(c).alias(f"c_{c}") for c in STATE),
    )
    return (
        rows.select(*MEMBER, "group_id")
        .join(canonical, on="group_id")
        .select(
            *MEMBER,
            pl.col("c_axis").alias("normalized_axis_label"),
            pl.col("c_member").alias("normalized_member_label"),
            "group_id",
            "group_max_period_end",
            pl.lit("grouping").alias("source"),
            *(pl.col(f"c_{c}").alias(c) for c in STATE),
        )
    )


def chainable_facts(facts: pl.DataFrame, grouped: pl.DataFrame) -> pl.DataFrame:
    """Product and Geographical facts with their grouped labels and weighted values."""
    joined = facts.join(
        grouped.select(
            *MEMBER,
            pl.col("normalized_axis_label").alias("g_axis"),
            pl.col("normalized_member_label").alias("g_member"),
            "group_max_period_end",
            *(pl.col(c).alias(f"g_{c}") for c in STATE),
        ),
        on=MEMBER,
        how="left",
    )
    hit = pl.col("period_end") <= pl.col("group_max_period_end")
    labelled = joined.with_columns(
        pl.coalesce(pl.when(hit).then("g_axis"), "axis").alias("normalized_axis_label"),
        pl.coalesce(pl.when(hit).then("g_member"), "member_label").alias(
            "normalized_member_label"
        ),
        (pl.col("value") * pl.col("weight")).alias("normalized_value"),
        (pl.col("comparative_value") * pl.col("weight")).alias(
            "normalized_comparative_value"
        ),
        pl.when(hit)
        .then("g_overridden")
        .otherwise(False)
        .fill_null(False)
        .alias("overridden"),
        pl.when(hit).then("g_override_priority").alias("override_priority"),
        pl.when(hit).then("g_override_level").alias("override_level"),
    )
    return labelled.filter(pl.col("normalized_axis_label").is_in(CHAINED_AXES))


def same_period_pairs(facts: pl.DataFrame) -> pl.DataFrame:
    """Pairs of members of a line that were both reported for some period."""
    members = facts.select(
        *LINE, "period_end", "normalized_axis_label", "normalized_member_label"
    ).unique()
    pairs = members.join(members, on=[*LINE, "period_end"], suffix="2").filter(
        ~(
            (pl.col("normalized_axis_label") == pl.col("normalized_axis_label2"))
            & (pl.col("normalized_member_label") == pl.col("normalized_member_label2"))
        )
    )
    return pairs.select(
        *LINE,
        pl.col("normalized_axis_label").alias("normalized_axis_label1"),
        pl.col("normalized_member_label").alias("normalized_member_label1"),
        "normalized_axis_label2",
        "normalized_member_label2",
    ).unique()


SIDE = [
    "concept",
    "normalized_label",
    "period_end",
    "axis",
    "member",
    "member_label",
    "normalized_axis_label",
    "normalized_member_label",
    *STATE,
]


def matches(facts: pl.DataFrame) -> pl.DataFrame:
    """Bridges from a later member's comparative to an earlier member's value.

    The two must be different members (or, when overrides only named the
    axis, different member labels) and must never share a period.
    """
    later = facts.select(
        "company_id",
        "statement",
        "form_type",
        pl.col("normalized_label").alias("line"),
        pl.col("comparative_period_end").alias("period_end2"),
        pl.col("normalized_comparative_value").alias("bridge"),
        *(pl.col(c).alias(f"{c}1") for c in SIDE),
    )
    earlier = facts.filter(pl.col("normalized_value") != 0).select(
        "company_id",
        "statement",
        "form_type",
        pl.col("normalized_label").alias("line"),
        pl.col("normalized_value").alias("bridge"),
        *(pl.col(c).alias(f"{c}2") for c in SIDE),
    )
    pairs = later.join(
        earlier,
        on=["company_id", "statement", "form_type", "line", "period_end2", "bridge"],
    )
    differs = (pl.col("normalized_axis_label1") != pl.col("normalized_axis_label2")) | (
        pl.col("normalized_member_label1") != pl.col("normalized_member_label2")
    )
    same_axis_new_member = (
        pl.col("normalized_axis_label1") == pl.col("normalized_axis_label2")
    ) & (pl.col("normalized_member_label1") != pl.col("normalized_member_label2"))
    level1, level2 = pl.col("override_level1"), pl.col("override_level2")
    allowed = (
        ((~pl.col("overridden1") | ~pl.col("overridden2")) & differs)
        | ((level1 == "axis") & (level2 == "axis") & same_axis_new_member)
        | ((level1 == "axis") & (level2 == "member") & same_axis_new_member)
        | ((level1 == "member") & (level2 == "axis") & same_axis_new_member)
    )
    pairs = pairs.filter(
        allowed.fill_null(False) & (pl.col("period_end1") > pl.col("period_end2"))
    )
    spp = same_period_pairs(facts).rename({"normalized_label": "line"})
    return pairs.join(
        spp,
        on=[
            "company_id",
            "statement",
            "line",
            "normalized_axis_label1",
            "normalized_member_label1",
            "normalized_axis_label2",
            "normalized_member_label2",
        ],
        how="anti",
    ).rename({"normalized_label1": "label1", "normalized_label2": "label2"})


#: The columns that identify a chain node; a step must start from an equal one.
NODE = [
    "concept",
    "label",
    "axis",
    "member",
    "member_label",
    "normalized_axis_label",
    "normalized_member_label",
]


def chaining(matched: pl.DataFrame) -> pl.DataFrame:
    """Rename chains of members, each labelled by the best node of its chain.

    Every match starts a chain at its later member and steps to an earlier
    member through matches that start on or before the chain's current period.
    """

    def side(n: str) -> list[pl.Expr]:
        return [pl.col(f"{c}{n}").alias(c) for c in (*NODE, *STATE)]

    roots = matched.select(
        "company_id",
        "statement",
        *side("1"),
        pl.col("period_end1").alias("current_period"),
        pl.col("period_end1").alias("root_period"),
    ).with_columns(chain_group_id().alias("group_id"))
    # A step leads from a match's later member to its earlier one.
    steps = matched.select(
        "company_id",
        "statement",
        *(pl.col(f"{c}1").alias(c) for c in NODE),
        "period_end1",
        *(pl.col(f"{c}2").alias(f"to_{c}") for c in (*NODE, *STATE)),
        pl.col("period_end2").alias("to_current_period"),
    ).unique()

    carry = ["company_id", "statement", "root_period", "group_id"]
    reached = [roots.unique()]
    frontier = reached[0]
    while frontier.height:
        nxt = (
            frontier.select(*carry, *NODE, pl.col("current_period").alias("at"))
            .join(steps, on=["company_id", "statement", *NODE])
            .filter(pl.col("period_end1") <= pl.col("at"))
            .select(
                *(
                    (
                        pl.col(f"to_{c}").alias(c)
                        if f"to_{c}" in steps.columns
                        else pl.col(c)
                    )
                    for c in roots.columns
                )
            )
            .unique()
        )
        nxt = nxt.join(
            pl.concat(reached), on=roots.columns, how="anti", nulls_equal=True
        )
        reached.append(nxt)
        frontier = nxt
    chain = pl.concat(reached)

    group_normalized = ordering.distinct_on(
        chain, ["group_id"], [*override_rank(), ordering.desc("current_period")]
    ).select(
        "group_id",
        *(pl.col(c).alias(f"gn_{c}") for c in [*OVERRIDE_COLUMNS, *STATE]),
    )
    chosen = ordering.distinct_on(
        chain.rename({"label": "normalized_label"}),
        MEMBER,
        [
            ordering.asc(ordering.by_text("normalized_axis_label")),
            ordering.asc(ordering.by_text("normalized_member_label")),
            ordering.desc("root_period"),
        ],
    )
    return chosen.join(group_normalized, on="group_id").select(
        *MEMBER,
        *(pl.col(f"gn_{c}").alias(c) for c in OVERRIDE_COLUMNS),
        "group_id",
        pl.col("root_period").alias("group_max_period_end"),
        pl.lit("chaining").alias("source"),
        *(pl.col(f"gn_{c}").alias(c) for c in STATE),
    )


def chain_group_id() -> pl.Expr:
    """md5 of the root node, as the chain's group id."""
    parts = [pl.col(c).cast(pl.String) for c in ("company_id", "statement", *NODE)]
    return md5_expr(parts, "chaining")


def group_components(grouped: pl.DataFrame, chained: pl.DataFrame) -> pl.DataFrame:
    """Grouping and chaining rows, each with the component of groups it is in.

    Two groups of one line that share a member are one component, named by
    the smallest group id.
    """
    base = pl.concat([grouped, chained], how="vertical_relaxed")
    members = base.select(*MEMBER, "group_id").unique()
    edges = members.join(members, on=MEMBER, suffix="_2").filter(
        pl.col("group_id") != pl.col("group_id_2")
    )

    # Groups are per line, so a node is (line, group).
    def node(group: str) -> pl.Expr:
        return pl.concat_str([*LINE, group], separator="\x1f")

    nodes = base.select(node("group_id").alias("node"), *LINE, "group_id").unique()
    edge_nodes = edges.select(
        node("group_id").alias("a"), node("group_id_2").alias("b")
    )
    comp = component_frame(
        nodes["node"], edge_nodes["a"], edge_nodes["b"], "node", "comp"
    )
    component_of = nodes.join(comp, on="node").select(
        *LINE,
        "group_id",
        pl.col("comp").str.split("\x1f").list.last().alias("component_id"),
    )
    return base.join(component_of, on=[*LINE, "group_id"])


def merge_groups(components: pl.DataFrame) -> pl.DataFrame:
    """One label per component: override first, then the latest group.

    Groups reaching the same period tie: a chain beats a grouping, then the
    first label in text order.
    """
    canonical = ordering.distinct_on(
        components,
        [*LINE, "component_id"],
        [
            *override_rank(),
            ordering.desc("group_max_period_end"),
            ordering.desc(pl.col("source") == "chaining"),
            ordering.asc(ordering.by_text("normalized_axis_label")),
            ordering.asc(ordering.by_text("normalized_member_label")),
        ],
    ).select(*LINE, "component_id", *OVERRIDE_COLUMNS, "source", *STATE)
    members = components.select(*MEMBER, "component_id").unique()
    result = ordering.distinct_on(
        members.join(canonical, on=[*LINE, "component_id"]), MEMBER, ["component_id"]
    )
    return result.select(
        *MEMBER,
        *OVERRIDE_COLUMNS,
        pl.col("component_id").alias("group_id"),
        "source",
        *STATE,
    )


def run(data: CompanyData) -> pl.DataFrame:
    facts = data.keep("facts", dimension_facts(data))
    base = data.keep(
        "member_base",
        member_base(facts, data.table("dimension_normalization_overrides")),
    )
    grouped = grouping(data.keep("member_groups", member_groups(base)))
    chainable = chainable_facts(facts, grouped)
    matched = data.keep("matches", matches(chainable))
    chained = data.keep("chaining", chaining(matched))
    components = data.keep("components", group_components(grouped, chained))
    return merge_groups(components).cast(DIMENSION_NORMALIZATION.dtypes)
