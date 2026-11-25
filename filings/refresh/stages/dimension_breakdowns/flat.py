"""Inferred breakdowns: subsets of an axis's members that add up to a parent.

Per line, axis and 10-K period, every subset of two or more members of the
flat list is tried against every parent (another member, or the
undimensioned line). A set must add up in at least two 10-Ks, not counting
years where a member could be swapped for a twin of equal value.

An axis that holds a subtotal beside its parts makes every set containing
the subtotal add up a second time with the parts swapped in. That second
set only expands the first and is dropped. For example, Apple reports
Products beside iPhone, iPad, Mac and Wearables, so Revenue equals both
Products + Services and iPhone + iPad + Mac + Wearables + Services.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Iterator, Sequence

import numpy as np
import polars as pl

from ...rules import sql
from .rows import AXIS_PERIOD, BREAKDOWN, BREAKDOWN_KEY, PERIOD, with_members_key

#: A wider flat axis is skipped: subset search is 2^n.
MAX_FLAT_MEMBERS = 16
#: int64 headroom for exact scaled sums; beyond it, Python ints are used.
_INT64_SAFE = 2**62


# --- Which rows are members of the flat list ---------------------------------


def shared_members(reported: pl.DataFrame, dims: pl.DataFrame) -> pl.DataFrame:
    """Dimensions on every cross-tab cell of one axis and period: the table total.

    A filer can tag every cell of a table with a second member that names
    the whole table, not a slice of it, so a flat list looks like a
    cross-tab. A member every cell carries is that kind of label; ignoring
    it leaves the cells as one flat list. For example, Netflix tags each
    region's revenue with Streaming, its only segment. A member that is
    itself a single-dimension row on the axis is a parent in the tree, not
    a total, and does not count.
    """
    counts = dims.group_by("fact_id").agg(pl.len().alias("dim_count"))
    with_counts = reported.join(
        counts, left_on="id", right_on="fact_id", how="left"
    ).with_columns(pl.col("dim_count").fill_null(0))
    multi = with_counts.filter(pl.col("dim_count") > 1)
    cells = multi.select(*AXIS_PERIOD, "id").join(
        dims.select("fact_id", pl.col("member").alias("dim_member")),
        left_on="id",
        right_on="fact_id",
    )
    totals = multi.group_by(AXIS_PERIOD).agg(pl.len().alias("cells"))
    shared = (
        cells.group_by(*AXIS_PERIOD, "dim_member")
        .agg(pl.col("id").n_unique().alias("with_member"))
        .join(totals, on=AXIS_PERIOD)
        .filter(
            (pl.col("with_member") == pl.col("cells")) & (pl.col("with_member") >= 2)
        )
    )
    single = (
        with_counts.filter(pl.col("dim_count") == 1)
        .select(*AXIS_PERIOD, "id")
        .join(
            dims.select("fact_id", pl.col("member").alias("dim_member")),
            left_on="id",
            right_on="fact_id",
        )
    )
    return shared.join(single, on=[*AXIS_PERIOD, "dim_member"], how="anti").select(
        *AXIS_PERIOD, "dim_member"
    )


def flat_members(reported: pl.DataFrame, dims: pl.DataFrame) -> pl.DataFrame:
    """Rows with at most one real slice among their dimensions.

    The axis default and a member shared by every cell do not count. A zero
    member is left out: it could join any set and make every match ambiguous.
    """
    shared = shared_members(reported, dims).with_columns(pl.lit(True).alias("shared"))
    real = (
        reported.select(*AXIS_PERIOD, "id")
        .join(
            dims.select("fact_id", pl.col("member").alias("dim_member")),
            left_on="id",
            right_on="fact_id",
        )
        .join(shared, on=[*AXIS_PERIOD, "dim_member"], how="left")
        .filter(pl.col("shared").is_null())
        .group_by("id")
        .agg(pl.len().alias("slices"))
    )
    return (
        reported.join(real, on="id", how="left")
        .filter(pl.col("slices").fill_null(0) <= 1)
        .drop("slices")
    )


# --- Groups and the parents they can add up to -------------------------------


def axis_groups(members: pl.DataFrame) -> pl.DataFrame:
    """Members per line, axis and period, in collation order."""
    ordered = sql.sort(members, [sql.collate("member")])
    return ordered.group_by(*AXIS_PERIOD, "fiscal_year", maintain_order=True).agg(
        pl.col("member").alias("names"),
        pl.col("value").alias("amounts"),
        pl.col("rounding").alias("roundings"),
    )


def parent_targets(
    members: pl.DataFrame, groups: pl.DataFrame, totals: pl.DataFrame
) -> pl.DataFrame:
    """Parents a set of ``members`` can add up to: a member, or the line itself.

    A member equal to the undimensioned value copies the statement line; it
    is not a parent of its siblings.
    """
    member_parents = members.join(
        totals.select(*PERIOD, "value"), on=[*PERIOD, "value"], how="anti"
    ).select(
        *AXIS_PERIOD,
        "fiscal_year",
        pl.col("member").alias("parent_member"),
        "value",
        "rounding",
    )
    line_parents = (
        groups.select(*AXIS_PERIOD, "fiscal_year")
        .join(totals.select(*PERIOD, "value", "rounding"), on=PERIOD)
        .with_columns(pl.lit("").alias("parent_member"))
    )
    return pl.concat([member_parents, line_parents.select(member_parents.columns)])


# --- Subset search -------------------------------------------------------------


@dataclass
class _Group:
    names: list[str]
    amounts: list[Decimal]
    roundings: list[int]


def _scale(values: Sequence[Decimal]) -> int:
    """The smallest power of ten that makes every value an integer."""
    k = 0
    for v in values:
        exponent = v.normalize().as_tuple().exponent
        if isinstance(exponent, int) and -exponent > k:
            k = -exponent
    return k


def _subset_tables(
    values: Sequence[int], roundings: Sequence[int], dtype: Any
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Sum, coarsest rounding and size of every subset, indexed by bit mask."""
    sums = np.zeros(1, dtype=dtype)
    widest = np.zeros(1, dtype=np.int64)
    sizes = np.zeros(1, dtype=np.int64)
    for value, rounding in zip(values, roundings):
        sums = np.concatenate([sums, sums + value])
        widest = np.concatenate([widest, np.maximum(widest, rounding)])
        sizes = np.concatenate([sizes, sizes + 1])
    return sums, widest, sizes


def matching_subsets(
    group: _Group, parent_value: Decimal, parent_rounding: int, parent_member: str
) -> Iterator[list[str]]:
    """Member lists of ``group`` that add up to the parent within tolerance."""
    k = _scale([*group.amounts, parent_value])
    factor = 10**k
    values = [int(a * factor) for a in group.amounts]
    target = int(parent_value * factor)
    bound = (len(values) + 1) * (max(map(abs, values), default=0) + abs(target)) * 1000
    dtype: Any = np.int64 if bound < _INT64_SAFE else object
    sums, widest, sizes = _subset_tables(values, group.roundings, dtype)

    gap = np.abs(np.asarray(sums, dtype=dtype) - target)
    coarser = widest > parent_rounding
    # Coarser members may miss by half a unit each, up to 0.1% of the parent.
    loose = (gap * 2 <= sizes * widest * factor) & (gap * 1000 <= abs(target))
    fits = np.where(coarser, loose, gap == 0)

    excluded = 0
    for i, name in enumerate(group.names):
        if name == parent_member:
            excluded |= 1 << i
    masks = np.arange(len(sums), dtype=np.int64)
    ok = fits & (sizes >= 2) & ((masks & excluded) == 0)
    for mask in masks[ok.astype(bool)]:
        yield [group.names[i] for i in range(len(group.names)) if mask >> i & 1]


def _twins(members: pl.DataFrame) -> dict[tuple, dict[str, set[str]]]:
    """Per line, axis and period: each member's other members of equal value."""
    pairs = members.join(members, on=[*AXIS_PERIOD, "value"], suffix="_twin").filter(
        pl.col("member") != pl.col("member_twin")
    )
    twins: dict[tuple, dict[str, set[str]]] = {}
    for row in pairs.select(*AXIS_PERIOD, "member", "member_twin").iter_rows():
        key, member, twin = row[:5], row[5], row[6]
        twins.setdefault(key, {}).setdefault(member, set()).add(twin)
    return twins


def subset_matches(
    members: pl.DataFrame, groups: pl.DataFrame, targets: pl.DataFrame
) -> pl.DataFrame:
    """Every (period, parent, member set) that adds up, flagged when ambiguous.

    Ambiguous: a member of the set has a twin of equal value outside it, so
    this period cannot tell the two sets apart (a region and a country that
    coincide).
    """
    twins = _twins(members)
    by_period: dict[tuple, list[dict]] = {}
    for t in targets.iter_rows(named=True):
        by_period.setdefault(tuple(t[c] for c in AXIS_PERIOD), []).append(t)

    out = []
    sized = groups.filter(
        pl.col("names").list.len().is_between(2, MAX_FLAT_MEMBERS, closed="both")
    )
    for g in sized.iter_rows(named=True):
        key = tuple(g[c] for c in AXIS_PERIOD)
        group = _Group(g["names"], g["amounts"], g["roundings"])
        period_twins = twins.get(key, {})
        for t in by_period.get(key, []):
            for chosen in matching_subsets(
                group, t["value"], t["rounding"], t["parent_member"]
            ):
                inside = set(chosen)
                ambiguous = any(
                    twin not in inside and twin != t["parent_member"]
                    for m in chosen
                    for twin in period_twins.get(m, ())
                )
                out.append(
                    (*key, t["fiscal_year"], t["parent_member"], chosen, ambiguous)
                )
    return pl.DataFrame(
        out,
        schema={
            "company_id": pl.Int32,
            "statement": pl.String,
            "normalized_label": pl.String,
            "axis": pl.String,
            "period_end": pl.Date,
            "fiscal_year": pl.Int32,
            "parent_member": pl.String,
            "members": pl.List(pl.String),
            "is_ambiguous": pl.Boolean,
        },
        orient="row",
    )


# --- From matches to breakdowns ----------------------------------------------


def seen_twice(matches: pl.DataFrame) -> pl.DataFrame:
    """Sets that add up, unambiguously, in at least two fiscal years.

    A single matching 10-K can be a coincidence. A kept set then holds in
    every year where it adds up, ambiguous or not.
    """
    keyed = with_members_key(matches)
    clear_years = (
        pl.col("fiscal_year")
        .filter(~pl.col("is_ambiguous"))
        .n_unique()
        .over(BREAKDOWN_KEY)
    )
    return keyed.filter(clear_years >= 2).drop("members_key")


def without_expansions(kept: pl.DataFrame) -> pl.DataFrame:
    """Drop a set that only swaps some members for their own parent's parts.

    When a member is the sum of other members on the axis, any set holding
    it also adds up with that member replaced by its parts. The second set
    repeats the first one level deeper, and the deeper split is already a
    breakdown of the subtotal, so it is dropped in that period. For example,
    when Products = iPhone + iPad, the line equals both {Products, Services}
    and {iPhone, iPad, Services}.
    """
    rows = kept.with_row_index("row").to_dicts()
    by_period: dict[tuple, list[dict]] = {}
    for r in rows:
        by_period.setdefault(tuple(r[c] for c in AXIS_PERIOD), []).append(r)

    dropped: set[int] = set()
    for period_rows in by_period.values():
        for k in period_rows:
            k_set = set(k["members"])
            for q in period_rows:
                if (
                    q["parent_member"] == k["parent_member"]
                    or q["parent_member"] in k_set
                ):
                    continue
                q_set = set(q["members"])
                if not q_set <= k_set:
                    continue
                for p in period_rows:
                    if (
                        p["parent_member"] != k["parent_member"]
                        or q["parent_member"] not in p["members"]
                    ):
                        continue
                    rest = [m for m in p["members"] if m != q["parent_member"]]
                    if (
                        set(rest) <= k_set
                        and not set(rest) & q_set
                        and len(p["members"])
                        == len(k["members"]) - len(q["members"]) + 1
                    ):
                        dropped.add(k["row"])
                        break
                if k["row"] in dropped:
                    break
    return (
        kept.with_row_index("row")
        .filter(~pl.col("row").is_in(list(dropped)))
        .drop("row")
    )


def inferred_breakdowns(
    reported: pl.DataFrame, dims: pl.DataFrame, totals: pl.DataFrame
) -> pl.DataFrame:
    """Flat-list breakdowns: ``BREAKDOWN`` columns plus ``fiscal_year``."""
    members = flat_members(reported, dims)
    groups = axis_groups(members)
    targets = parent_targets(members, groups, totals)
    matches = subset_matches(members, groups, targets)
    return without_expansions(seen_twice(matches)).select(
        *BREAKDOWN, "fiscal_year", "period_end"
    )
