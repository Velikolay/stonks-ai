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
| `io/schemas.py` | One polars schema per table, matching the database column order |
| `io/db.py` | asyncpg load, and `DELETE` plus binary `COPY` per company |
| `rules/` | Logic shared by several stages: fact overrides and conflicts, override precedence, rounding, units, periods, rewrites, connected components |
| `rules/sql.py` | Postgres semantics that change results: null ordering, collation, `DISTINCT ON`, `round`, `SUM` and `bool_and` over no rows |
| `rules/ids.py` | Postgres `hashtextextended` and `md5`, so synthetic ids match the database |
| `stages/` | One module (or package) per stage. Each is a pure `run(data) -> DataFrame` |
| `parity/` | Snapshot and diff tools, and the allow-list of reviewed differences |

Stages never touch the database. Each is a chain of named steps. The bigger
ones are packages split by concern, for example `quarterly/ytd.py`,
`quarterly/restatement.py` and `quarterly/q4.py`.

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
the differing rows, and `--no-allowlist` to see all of them.

`parity/allowlist.csv` records differences already reviewed while porting the
SQL procedures, with the reason for each. Most are ties the SQL broke
arbitrarily and Python now breaks deterministically.
