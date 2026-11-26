"""Print a filing's XBRL facts with every dimension, straight from the instance.

``financial_facts`` holds what the parser keeps, so "the filing does not tag
it" must be checked here: the instance has every fact, every coordinate and
the label the filing gave each member. Filters are case-insensitive regular
expressions::

    python -m filings.scripts.xbrl_facts 0001193125-24-099840 --dimension nanometer
    python -m filings.scripts.xbrl_facts 0000002488-23-000076 --concept Revenue --dimension Geograph

EDGAR_IDENTITY must be set; edgartools caches the download.
"""

from __future__ import annotations

import argparse
import os
import re
from typing import Optional, Sequence

import pandas as pd
from edgar import find, set_identity


def facts(accession: str) -> pd.DataFrame:
    """Every fact of the filing's instance, one ``dim_*`` column per axis."""
    filing = find(accession)
    xbrl = filing.xbrl() if filing is not None else None
    if xbrl is None:
        raise SystemExit(f"{accession} has no XBRL")
    return xbrl.facts.to_dataframe()


def matching(
    frame: pd.DataFrame,
    concept: Optional[str] = None,
    dimension: Optional[str] = None,
) -> pd.DataFrame:
    """Facts whose concept, and any axis or member, match the patterns."""
    dims = [c for c in frame.columns if c.startswith("dim_")]
    keep = pd.Series(True, index=frame.index)
    if concept:
        keep &= frame["concept"].astype(str).str.contains(concept, flags=re.I)
    if dimension:
        pattern = re.compile(dimension, re.I)
        keep &= frame.apply(
            lambda row: any(
                pd.notna(row[c]) and (pattern.search(c) or pattern.search(str(row[c])))
                for c in dims
            ),
            axis=1,
        )
    return frame[keep]


def describe(frame: pd.DataFrame) -> list[str]:
    """One line per fact: concept, value, unit, period and its coordinates."""
    dims = [c for c in frame.columns if c.startswith("dim_")]
    period = next(
        (c for c in ("period_end", "period_instant", "end_date") if c in frame), None
    )
    lines = []
    for _, row in frame.iterrows():
        coordinates = ", ".join(f"{c[4:]}={row[c]}" for c in dims if pd.notna(row[c]))
        lines.append(
            f"{row['concept']} | {row.get('value')} | {row.get('unit_ref', '')} | "
            f"{row[period] if period else ''} | {coordinates}"
        )
    return lines


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("accession", help="e.g. 0001193125-24-099840")
    parser.add_argument("--concept", help="Regular expression on the concept")
    parser.add_argument("--dimension", help="Regular expression on any axis or member")
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args(argv)
    set_identity(os.environ["EDGAR_IDENTITY"])
    found = matching(facts(args.accession), args.concept, args.dimension)
    for line in describe(found.head(args.limit)):
        print(line)
    print(f"{len(found)} facts")


if __name__ == "__main__":
    main()
