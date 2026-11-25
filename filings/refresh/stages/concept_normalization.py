"""Stage 2: infer a canonical label per concept, then apply concept overrides.

Reads the overridden facts and ``concept_normalization_overrides``; writes
``concept_normalization``. Only undimensioned facts take part. Two signals
select a concept:

* Grouping: one concept filed under several labels, one label per filing.
  The latest label wins.
* Chaining: a period's comparative value equals another concept's value for
  that earlier period, and neither concept exists on the other's period. The
  two are one renamed line, labelled from the latest period of the chain.

A concept neither signal selects has no row; later stages fall back to its
filed label and apply its override directly.
"""

from __future__ import annotations

from difflib import SequenceMatcher

import polars as pl

from ..inputs import CompanyData
from ..io.schemas import CONCEPT_NORMALIZATION
from ..rules import sql
from ..rules.facts import ConflictRule, drop_conflicting, with_fact_overrides
from ..rules.ids import md5_hex

COMPANY_SLOT = ("company_id", "statement")


def group_id(company_id: int, statement: str, concept: str, source: str) -> str:
    return md5_hex(f"{company_id}|{statement}|{concept}|{source}")


def group_id_expr(concept: str, source: str) -> pl.Expr:
    return pl.struct("company_id", "statement", concept).map_elements(
        lambda r: group_id(r["company_id"], r["statement"], r[concept], source),
        return_dtype=pl.String,
    )


def grouping(facts: pl.DataFrame) -> pl.DataFrame:
    """Concepts filed under several labels, never two in one filing.

    The label of the latest period wins.
    """
    undimensioned = facts.filter(pl.col("axis") == "")
    latest = sql.sort(undimensioned, [sql.desc("period_end")])
    grouped = latest.group_by(*COMPANY_SLOT, "concept", maintain_order=True).agg(
        pl.col("label").first().alias("normalized_label"),
        pl.col("period_end").max().alias("group_max_period_end"),
        pl.col("label").n_unique().alias("labels"),
        pl.struct("filing_id", "label").n_unique().alias("filing_labels"),
        pl.col("filing_id").n_unique().alias("filings"),
    )
    selected = grouped.filter(
        (pl.col("labels") > 1) & (pl.col("filing_labels") == pl.col("filings"))
    )
    return selected.select(
        *COMPANY_SLOT,
        "concept",
        "normalized_label",
        group_id_expr("concept", "grouping").alias("group_id"),
        "group_max_period_end",
        pl.lit("grouping").alias("source"),
    )


def labelled_facts(facts: pl.DataFrame, grouped: pl.DataFrame) -> pl.DataFrame:
    """Undimensioned facts with the grouping label and weighted values."""
    undimensioned = facts.filter(pl.col("axis") == "")
    joined = undimensioned.join(
        grouped.select(
            *COMPANY_SLOT,
            "concept",
            "group_max_period_end",
            pl.col("normalized_label").alias("g_label"),
        ),
        on=[*COMPANY_SLOT, "concept"],
        how="left",
    )
    in_group = pl.col("period_end") <= pl.col("group_max_period_end")
    return joined.with_columns(
        pl.when(in_group)
        .then(pl.col("g_label"))
        .otherwise(pl.col("label"))
        .alias("normalized_label"),
        (pl.col("value") * pl.col("weight")).alias("normalized_value"),
        (pl.col("comparative_value") * pl.col("weight")).alias(
            "normalized_comparative_value"
        ),
    ).drop("g_label", "group_max_period_end")


def candidate_matches(
    labelled: pl.DataFrame, facts: pl.DataFrame, data: CompanyData | None = None
) -> pl.DataFrame:
    """Pairs where a later fact's comparative equals an earlier fact of another concept.

    Zero never bridges: every line that did not occur that year shares it. A
    pair whose concepts coexist on either period is not a rename.
    """
    later = labelled.select(
        *COMPANY_SLOT,
        "form_type",
        pl.col("concept").alias("concept1"),
        pl.col("period_end").alias("period_end1"),
        pl.col("comparative_period_end").alias("period_end2"),
        pl.col("normalized_comparative_value").alias("bridge"),
        pl.col("normalized_label").alias("label1"),
        pl.col("id").alias("id1"),
    )
    earlier = labelled.filter(pl.col("normalized_value") != 0).select(
        *COMPANY_SLOT,
        "form_type",
        pl.col("concept").alias("concept2"),
        pl.col("period_end").alias("period_end2"),
        pl.col("normalized_value").alias("bridge"),
        pl.col("normalized_label").alias("label2"),
    )
    pairs = later.join(
        earlier, on=[*COMPANY_SLOT, "form_type", "period_end2", "bridge"], how="inner"
    ).filter(
        (pl.col("concept1") != pl.col("concept2"))
        & (pl.col("period_end1") > pl.col("period_end2"))
    )

    present = facts.select(*COMPANY_SLOT, "period_end", "concept").unique()
    pairs = pairs.join(
        present,
        left_on=[*COMPANY_SLOT, "period_end1", "concept2"],
        right_on=[*COMPANY_SLOT, "period_end", "concept"],
        how="anti",
    ).join(
        present,
        left_on=[*COMPANY_SLOT, "period_end2", "concept1"],
        right_on=[*COMPANY_SLOT, "period_end", "concept"],
        how="anti",
    )
    if data is not None:
        data.keep("bridges", pairs)
    keys = [*COMPANY_SLOT, "concept1", "concept2", "period_end1", "period_end2"]
    # Two facts of one concept and period (a beginning and an ending balance)
    # can bridge the same pair; the later-printed fact's label is kept.
    return sql.distinct_on(pairs, keys, [sql.desc("id1")]).select(
        *keys, "label1", "label2"
    )


def false_matches(candidates: pl.DataFrame) -> pl.DataFrame:
    """Concept pairs the evidence rules out entirely.

    Overlapping: one of the pair's periods is the other end of another match
    of the same pair, a chain of three periods. Mirrored: the same periods
    matched with the concepts swapped.
    """
    pair = [*COMPANY_SLOT, "concept1", "concept2"]
    as_first = candidates.select(*pair, pl.col("period_end1").alias("p"))
    as_second = candidates.select(*pair, pl.col("period_end2").alias("p"))
    overlapping = as_first.join(as_second, on=[*pair, "p"], how="semi").select(pair)
    swapped = candidates.select(
        *COMPANY_SLOT,
        pl.col("concept2").alias("concept1"),
        pl.col("concept1").alias("concept2"),
        "period_end1",
        "period_end2",
    )
    mirrored = candidates.join(
        swapped, on=[*pair, "period_end1", "period_end2"], how="semi"
    ).select(pair)
    return pl.concat([overlapping, mirrored]).unique()


def own_labels(labelled: pl.DataFrame) -> pl.DataFrame:
    """Each concept's label in its latest period, as filed or grouped."""
    latest = sql.sort(labelled, [sql.desc("period_end"), sql.desc("id")])
    return latest.group_by(*COMPANY_SLOT, "concept").agg(
        pl.col("normalized_label").first().alias("own_label")
    )


def _similarity(a: str | None, b: str | None) -> float:
    if a is None or b is None:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def chaining(
    candidates: pl.DataFrame,
    labels: pl.DataFrame,
    data: CompanyData | None = None,
) -> pl.DataFrame:
    """Concepts linked by rename chains, labelled from the chain's latest root.

    Every match starts a chain at its later concept. A chain steps to an
    earlier concept through any match that starts on or before the chain's
    current period. A concept takes the chain whose root period is latest.
    """
    pair = [*COMPANY_SLOT, "concept1", "concept2"]
    matches = candidates.join(false_matches(candidates), on=pair, how="anti")

    walk = matches.select(
        *COMPANY_SLOT,
        pl.col("concept1").alias("concept"),
        pl.col("label1").alias("normalized_label"),
        pl.col("period_end1").alias("current_period"),
        pl.col("period_end1").alias("root_period"),
        pl.col("concept1").alias("root_concept"),
    ).unique()
    reached = [walk]
    frontier = walk
    steps = matches.select(
        *COMPANY_SLOT, "concept1", "concept2", "period_end1", "period_end2"
    )
    while frontier.height:
        nxt = (
            frontier.join(
                steps,
                left_on=[*COMPANY_SLOT, "concept"],
                right_on=[*COMPANY_SLOT, "concept1"],
            )
            .filter(pl.col("period_end1") <= pl.col("current_period"))
            .select(
                *COMPANY_SLOT,
                pl.col("concept2").alias("concept"),
                "normalized_label",
                pl.col("period_end2").alias("current_period"),
                "root_period",
                "root_concept",
            )
            .unique()
        )
        nxt = nxt.join(pl.concat(reached), on=nxt.columns, how="anti")
        reached.append(nxt)
        frontier = nxt

    walked = pl.concat(reached).join(labels, on=[*COMPANY_SLOT, "concept"], how="left")
    walked = walked.with_columns(
        pl.struct("normalized_label", "own_label")
        .map_elements(
            lambda r: _similarity(r["normalized_label"], r["own_label"]),
            return_dtype=pl.Float64,
        )
        .alias("similarity")
    )
    if data is not None:
        data.keep("chain_walk", walked)
    # Two chains whose roots share the latest period tie, usually because two
    # lines had the same amount that year (net income equal to comprehensive
    # income). The SQL picked one arbitrarily; here the chain whose label is
    # closest to the concept's own label wins.
    chained = sql.distinct_on(
        walked,
        [*COMPANY_SLOT, "concept"],
        [
            sql.desc("root_period"),
            sql.desc("similarity"),
            sql.asc(sql.collate("root_concept")),
            sql.asc(sql.collate("normalized_label")),
        ],
    )
    return chained.select(
        *COMPANY_SLOT,
        "concept",
        "normalized_label",
        group_id_expr("root_concept", "chaining").alias("group_id"),
        pl.col("root_period").alias("group_max_period_end"),
        pl.lit("chaining").alias("source"),
    )


def inferred(chained: pl.DataFrame, grouped: pl.DataFrame) -> pl.DataFrame:
    """Chaining beats grouping for a concept both selected."""
    both = pl.concat(
        [
            chained.with_columns(pl.lit(1).alias("priority")),
            grouped.with_columns(pl.lit(2).alias("priority")),
        ],
        how="vertical_relaxed",
    )
    return sql.distinct_on(both, [*COMPANY_SLOT, "concept"], ["priority"]).drop(
        "priority"
    )


def _group_agreed(column: str, label: str) -> pl.Expr:
    """``column`` across the group, when every overridden member agrees on ``label``."""
    agree = pl.col(label).min() == pl.col(label).max()
    if column == label:
        value = pl.col(column).max()
    elif column.endswith("unit"):
        value = pl.col(column).sort_by(sql.collate(column), nulls_last=False).last()
    else:
        value = pl.col(column).max()
    return pl.when(agree).then(value)


def apply_overrides(rows: pl.DataFrame, overrides: pl.DataFrame) -> pl.DataFrame:
    """Overrides win; a group inherits one when its overridden members agree.

    Members that disagree prove the inferred group merged unrelated concepts,
    so the rest keep their inferred label instead of an arbitrary one.
    """
    keep = ["statement", "concept", "normalized_label", "weight", "unit"]
    company = overrides.filter(~pl.col("is_global")).select(
        "company_id",
        *(
            pl.col(c).alias(f"c_{c}") if c not in ("statement", "concept") else c
            for c in keep
        ),
    )
    global_ = overrides.filter(
        pl.col("is_global") & (pl.col("company_id") == 0)
    ).select(
        *(
            pl.col(c).alias(f"g_{c}") if c not in ("statement", "concept") else c
            for c in keep
        )
    )
    joined = rows.join(
        company, on=["company_id", "statement", "concept"], how="left"
    ).join(global_, on=["statement", "concept"], how="left")
    group = [*COMPANY_SLOT, "group_id"]
    joined = joined.with_columns(
        _group_agreed("c_normalized_label", "c_normalized_label")
        .over(group)
        .alias("cg_label"),
        _group_agreed("c_weight", "c_normalized_label").over(group).alias("cg_weight"),
        _group_agreed("c_unit", "c_normalized_label").over(group).alias("cg_unit"),
        _group_agreed("g_normalized_label", "g_normalized_label")
        .over(group)
        .alias("gg_label"),
        _group_agreed("g_weight", "g_normalized_label").over(group).alias("gg_weight"),
    )
    label = pl.coalesce(
        "c_normalized_label", "g_normalized_label", "cg_label", "gg_label"
    )
    return joined.select(
        *COMPANY_SLOT,
        "concept",
        pl.coalesce(label, "normalized_label").alias("normalized_label"),
        pl.coalesce("c_weight", "g_weight", "cg_weight", "gg_weight").alias("weight"),
        # A global override names no currency; only a company one replaces the unit.
        pl.coalesce("c_unit", "cg_unit").alias("unit"),
        "group_id",
        "source",
        label.is_not_null().alias("overridden"),
    )


def run(data: CompanyData) -> pl.DataFrame:
    facts = drop_conflicting(
        with_fact_overrides(data, include_clones=False), ConflictRule.ANY_FILING
    )
    grouped = data.keep("grouping", grouping(facts))
    labelled = labelled_facts(facts, grouped)
    candidates = data.keep(
        "candidate_matches", candidate_matches(labelled, facts, data)
    )
    chained = data.keep("chaining", chaining(candidates, own_labels(labelled), data))
    rows = inferred(chained, grouped)
    result = apply_overrides(rows, data.table("concept_normalization_overrides"))
    return result.filter(pl.col("company_id") == data.company_id).cast(
        CONCEPT_NORMALIZATION.schema
    )
