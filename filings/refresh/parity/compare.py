"""Diff two snapshots row by row, and their diagnostics scorecards.

Rows are joined on each table's primary key and sorted into classes:
``missing`` (only in the baseline), ``extra`` (only in the candidate),
``within_rounding`` (a value off by at most 0.1%), ``value_differs``, and
``column_differs`` for any other column. The run passes when no row differs
and no diagnostics query grew::

    python -m filings.refresh.parity.compare backups/before backups/after
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

import polars as pl

from ..io.schemas import DERIVED_TABLES, PRIMARY_KEYS
from .snapshot import SCORECARD_FILE

VALUE_COLUMNS = ("value", "comparative_value")
_KEY = "__key"


@dataclass
class TableDiff:
    """Differences found in one table."""

    table: str
    baseline_rows: int
    candidate_rows: int
    differences: pl.DataFrame


@dataclass
class Report:
    tables: list[TableDiff] = field(default_factory=list)
    #: Queries that grew; None when either snapshot has no scorecard.
    worse_diagnostics: Optional[pl.DataFrame] = None

    @property
    def passed(self) -> bool:
        tables_ok = all(t.differences.is_empty() for t in self.tables)
        return tables_ok and (
            self.worse_diagnostics is None or self.worse_diagnostics.is_empty()
        )


def _key_expr(columns: Sequence[str], schema: pl.Schema) -> pl.Expr:
    """One string per primary key, so list-typed keys can be joined too."""
    parts = []
    for c in columns:
        dtype = schema[c]
        if isinstance(dtype, pl.List):
            parts.append(pl.col(c).cast(pl.List(pl.String)).list.join("\x1f"))
        else:
            parts.append(pl.col(c).cast(pl.String).fill_null("\x00"))
    return pl.concat_str(parts, separator="|").alias(_KEY)


def _within_rounding(a: pl.Expr, b: pl.Expr) -> pl.Expr:
    tolerance = pl.max_horizontal(a.abs(), b.abs()) / 1000
    return (a - b).abs() <= tolerance


def diff_table(
    table: str, baseline: pl.DataFrame, candidate: pl.DataFrame
) -> pl.DataFrame:
    """Every difference between two versions of ``table``, one row per cell or row."""
    keys = PRIMARY_KEYS[table]
    left = baseline.with_columns(_key_expr(keys, baseline.schema))
    right = candidate.with_columns(_key_expr(keys, candidate.schema))
    company = "company_id"

    missing = left.join(right.select(_KEY), on=_KEY, how="anti").select(
        _KEY,
        company,
        pl.lit("missing").alias("class"),
        pl.lit(None, pl.String).alias("column"),
        pl.lit(None, pl.String).alias("baseline"),
        pl.lit(None, pl.String).alias("candidate"),
    )
    extra = right.join(left.select(_KEY), on=_KEY, how="anti").select(
        _KEY,
        company,
        pl.lit("extra").alias("class"),
        pl.lit(None, pl.String).alias("column"),
        pl.lit(None, pl.String).alias("baseline"),
        pl.lit(None, pl.String).alias("candidate"),
    )
    both = left.join(right, on=_KEY, how="inner", suffix="__new")

    cells = []
    for column in baseline.columns:
        if column in keys:
            continue
        old, new = pl.col(column), pl.col(f"{column}__new")
        changed = both.filter(~old.eq_missing(new))
        if changed.is_empty():
            continue
        if column in VALUE_COLUMNS:
            kind = (
                pl.when(
                    old.is_not_null() & new.is_not_null() & _within_rounding(old, new)
                )
                .then(pl.lit("within_rounding"))
                .otherwise(pl.lit("value_differs"))
            )
        else:
            kind = pl.lit("column_differs")
        cells.append(
            changed.select(
                _KEY,
                company,
                kind.alias("class"),
                pl.lit(column).alias("column"),
                old.cast(pl.String).alias("baseline"),
                new.cast(pl.String).alias("candidate"),
            )
        )
    return pl.concat([missing, extra, *cells], how="vertical_relaxed").rename(
        {_KEY: "key"}
    )


def compare_diagnostics(baseline: Path, candidate: Path) -> Optional[pl.DataFrame]:
    """Diagnostic queries whose row count grew for some company."""
    old_path, new_path = baseline / SCORECARD_FILE, candidate / SCORECARD_FILE
    if not old_path.exists() or not new_path.exists():
        return None
    keys = ["file", "query", "company_id"]
    old = pl.read_parquet(old_path).rename({"rows": "baseline"})
    new = pl.read_parquet(new_path).rename({"rows": "candidate"})
    joined = old.join(new, on=keys, how="full", coalesce=True, nulls_equal=True)
    return (
        joined.with_columns(pl.col("baseline", "candidate").fill_null(0))
        .filter(pl.col("candidate") > pl.col("baseline"))
        .sort(keys)
    )


def compare(
    baseline: Path,
    candidate: Path,
    tables: Optional[Sequence[str]] = None,
    out: Optional[Path] = None,
) -> Report:
    report = Report()
    for spec in DERIVED_TABLES:
        if tables and spec.name not in tables:
            continue
        old_file, new_file = (
            baseline / f"{spec.name}.parquet",
            candidate / f"{spec.name}.parquet",
        )
        if not old_file.exists() or not new_file.exists():
            continue
        old, new = pl.read_parquet(old_file), pl.read_parquet(new_file)
        differences = diff_table(spec.name, old, new)
        if out is not None and not differences.is_empty():
            out.mkdir(parents=True, exist_ok=True)
            differences.write_parquet(out / f"{spec.name}.diff.parquet")
        report.tables.append(TableDiff(spec.name, old.height, new.height, differences))
    report.worse_diagnostics = compare_diagnostics(baseline, candidate)
    return report


def print_report(report: Report, examples: int = 5) -> None:
    for t in report.tables:
        status = "OK  " if t.differences.is_empty() else "DIFF"
        print(
            f"{status} {t.table}: {t.baseline_rows} -> {t.candidate_rows} rows, "
            f"{t.differences.height} differences"
        )
        if not t.differences.is_empty():
            summary = (
                t.differences.group_by("class", "column")
                .agg(
                    pl.len().alias("n"),
                    pl.col("company_id").unique().sort().alias("companies"),
                )
                .sort("n", descending=True)
            )
            for row in summary.iter_rows(named=True):
                print(
                    f"       {row['class']:<16} {row['column'] or '':<22} {row['n']:>7}  companies {row['companies']}"
                )
            for row in t.differences.head(examples).iter_rows(named=True):
                print("       e.g. " + json.dumps(row, default=str))
    worse = report.worse_diagnostics
    if worse is None:
        return
    if worse.is_empty():
        print("OK   diagnostics: no query grew")
        return
    print("DIFF diagnostics with more rows than the baseline:")
    for row in worse.iter_rows(named=True):
        print(
            f"       {row['file']} | {row['query']} | company {row['company_id']}: "
            f"{row['baseline']} -> {row['candidate']}"
        )


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--tables", help="Comma-separated table names (default all)")
    parser.add_argument("--out", type=Path, help="Write the differing rows here")
    parser.add_argument("--examples", type=int, default=5)
    args = parser.parse_args(argv)
    report = compare(
        args.baseline,
        args.candidate,
        tables=args.tables.split(",") if args.tables else None,
        out=args.out,
    )
    print_report(report, examples=args.examples)
    sys.exit(0 if report.passed else 1)


if __name__ == "__main__":
    main()
