# filings.refresh

Rebuilds every derived financial table from `financial_facts`, `filings` and the
override tables. [AGENTS.md](../../AGENTS.md) explains what each stage decides
and why. This file explains where that logic lives and how to work on it.

## Running

```bash
make refresh                                   # every company, every stage
make refresh COMPANY_IDS=1,2                   # some companies
make refresh COMPANY_IDS=1 STAGES=yearly_financials
python -m filings.refresh --companies 1 -v     # rows and seconds per stage
```

From code, call `await refresh_companies([1, 2])`. Synchronous callers such as
tests can use `refresh_companies_sync`.

A run loads each company's inputs, runs the selected stages in memory, and
replaces the company's rows in every written table in one transaction.
Companies run concurrently (`--jobs`, default 4). When you select only some
stages, each one reads its upstream tables from the database.

## Layout

| Path | Contents |
| --- | --- |
| `pipeline.py` | Stage registry and order, per-company load, compute and write |
| `inputs.py` | `CompanyData`, the frames a stage reads, and `Trace` for debugging |
| `records.py` | Frames to slotted dataclasses and back, for steps decided in plain Python |
| `io/schemas.py` | One polars schema per table, matching the database column order |
| `io/db.py` | asyncpg load, and `DELETE` plus binary `COPY` per company |
| `rules/` | Logic shared by several stages: fact overrides and conflicts, override precedence, rounding (including Decimal helpers), units, periods, rewrites, connected components |
| `rules/ordering.py` | Sorts that pick a winner: null placement, text order (case-insensitive, then lowercase first), first row per key, row numbers |
| `rules/ids.py` | Stable ids hashed from a row's identity (BLAKE2b), and `md5` group ids |
| `rules/prior_periods.py` | Prior filings for periods before a company's first filing, built from its earliest comparatives, and the quarterly gap filter for them |
| `stages/` | One module (or package) per stage. Each is a pure `run(data) -> DataFrame` |
| `parity/` | Snapshot and diff tools for checking a change against a baseline |

Stages never touch the database. Each is a chain of named steps. The bigger
ones are packages split by concern, for example `quarterly/ytd.py`,
`quarterly/restatement.py` and `quarterly/q4.py`.

Most steps are frame operations: filtering, joining, applying overrides,
ranking within a partition. A rule that walks one group in order and
branches (reduce one series' YTD totals, decide whether one line's year
moves, pick the 10-K that one axis-year's Q4 comes from) is a plain function
over slotted dataclasses instead. `records.py` moves rows between the two:

```python
rows = numbered(frame)                                   # adds row_nr
by_series = grouped(records(rows, Amount, key=SERIES), lambda a: a.series)
decided = frame_of((decide(s) for s in by_series.values()), schema)
frame = rows.join(decided, on=ROW)                       # back to a frame
```

The frame keeps every column the output needs, and the record carries only
the fields the decision reads. Use this when the frame version would need a
flag column per branch or a self-join per "previous row"; keep bulk lookups
as joins.

## Debugging a stage

Stages expose their key intermediate frames through `data.keep(name, frame)`.
Pass `--dump-dir` to write those frames, and each stage's output, as Parquet:

```bash
python -m filings.refresh --companies 16 --stages dimension_normalization --dump-dir /tmp/dump
ls /tmp/dump/16/        # dimension_normalization.member_groups.parquet, ...
```

Unit tests in [tests/filings/refresh/](../../tests/filings/refresh/) build small
frames with `builders.company(...)` and call a stage's `run` directly. They need
no database.

## Checking a change against a baseline

```bash
make refresh-snapshot OUT=backups/before       # derived tables + diagnostics counts
# change code or overrides, make refresh
make refresh-snapshot OUT=backups/after
make refresh-compare BASE=backups/before NEW=backups/after
```

`compare` joins each table on its primary key and reports rows missing, extra,
within rounding, or with a changed value or column. It also lists every
diagnostics query whose row count grew for some company. Add `--out DIR` to save
the differing rows.
