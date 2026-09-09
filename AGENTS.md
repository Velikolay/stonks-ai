# SEC filings data pipeline

How to ingest 10-K and 10-Q filings for many companies and normalize them until
`yearly_financials` and `quarterly_financials` are clean, continuous and
comparable. Written to be followed autonomously; every step is a command with a
checkable result.

For app setup, the RAG API and query endpoints, see [README.md](README.md).

## Pipeline at a glance

```mermaid
flowchart TD
    edgar[EDGAR via edgartools] --> loader[SECXBRLFilingsLoader]
    loader --> raw["financial_facts + filings + companies"]
    raw --> p1[refresh_financial_facts_overridden]
    p1 --> p2[refresh_concept_normalization]
    p2 --> p3[refresh_hierarchy_normalization]
    p3 --> p4[refresh_dimension_normalization]
    p4 --> p5[refresh_financial_facts_normalized]
    p5 --> p6[refresh_quarterly_financials]
    p5 --> p7[refresh_yearly_financials]
    csv["migrations/data/*.csv"] --> overrides[override tables]
    overrides --> p1
    overrides --> p2
    overrides --> p3
    overrides --> p4
    overrides --> p5
```

Raw facts are never modified. Every correction is expressed as an override row
and applied by re-running the procedures, so the whole derived state is
reproducible from EDGAR plus three CSVs.

## Setup

```bash
cp env.example .env          # set DATABASE_URL, and EDGAR_IDENTITY (see below)
make install
make docker-up               # or: make db-start
make db-init                 # alembic upgrade head, seeds the override CSVs
```

`EDGAR_IDENTITY` is required. SEC fair-access rules reject unidentified traffic,
and at hundreds of companies you will be throttled or blocked without it:

```bash
EDGAR_IDENTITY="Jane Doe jane@example.com"
```

The `psql`-based targets read `DATABASE_URL` from the environment and otherwise
default to `postgresql://rag_user:rag_password@localhost:5432/rag_db`.

## The loop

This is the operating procedure. Repeat until the stop condition holds.

```bash
# 1. Ingest the companies' full XBRL history (this is the default)
make ingest TICKERS=AAPL,MSFT FORMS=10-K,10-Q

# 2. Normalize (make ingest already refreshes; run this after editing overrides)
make refresh COMPANY_IDS=1,2

# 3. Diagnose
make diagnose

# 4. Fix: edit the CSVs in migrations/data/, then
make overrides-check         # dry run: shows what would change
make overrides-load
make refresh COMPANY_IDS=1,2

# 5. Back to step 3
```

**Stop when** `make diagnose` reports no rows for coverage gaps, duplicate and
disjoint series, period gaps, orphaned parents, and quarterly-versus-annual
reconciliation; and the remaining rollup, sign-flip and conflicting-fact rows
have each been individually explained as genuine source-data artifacts. Unmapped
concepts with low fact counts are acceptable; unmapped concepts appearing in
every filing are not.

**Work one class of problem at a time.** Fix the most frequent unmapped concept,
refresh, re-diagnose. A single concept override often closes dozens of
downstream gaps, and fixing several at once makes it impossible to tell which
override did what.

### Ingest options

`make ingest` wraps [filings/scripts/ingest.py](filings/scripts/ingest.py):

| Variable | Meaning |
| --- | --- |
| `TICKERS` | Comma-separated, e.g. `AAPL,MSFT` |
| `TICKERS_FILE` | File with one ticker per line; `#` comments allowed |
| `FORMS` | Default `10-K,10-Q` |
| `LIMIT` | Max filings per ticker per form; `0`, the default, loads every available filing |

**Always load a company's complete XBRL history the first time you ingest it.**
That is what the default `LIMIT=0` does, and it is not optional: normalization
infers labels by chaining each period's comparative value back to the previous
period, so a partial history breaks the chains, invents disjoint series, and
produces gaps that look like normalization bugs but are really missing data. Do
not pass a `LIMIT` on a first run.

`LIMIT=N` is only for topping up a company you have already backfilled, and even
then it is rarely worth it. Re-running the default is nearly free: a filing that
is already stored is skipped before its XBRL is downloaded, and skipped filings
do not count against the limit, so a re-run costs one database lookup per filing
and ingests only what is new. Prefer plain `make ingest` for both cases.

EDGAR only carries XBRL from roughly 2009 onward, so "everything available" means
everything tagged, not the company's entire filing history. Confirm what actually
landed with `sql/diagnostics/01_coverage.sql`, which lists filings per fiscal year
and flags missing 10-K years.

The script is sequential with a delay between loads, because edgartools performs
blocking HTTP calls. A failure on one ticker is logged and the run continues; the
summary at the end lists every ticker with its filing and fact counts, and the
process exits non-zero if anything failed. Pass `--override` (via
`python -m filings.scripts.ingest`) to re-download filings already stored, which
is what you need after fixing a parser bug.

## Stage reference

Run in this order; each stage reads the previous one. `refresh_financials(int[])`
in [sql/procedures/refresh_financials.sql](sql/procedures/refresh_financials.sql)
chains all seven.

| Procedure | Writes | What it does |
| --- | --- | --- |
| `refresh_financial_facts_overridden` | `financial_facts_overridden` | Applies `financial_facts_overrides` rules, rewriting a fact's concept, dimensions and weight |
| `refresh_concept_normalization` | `concept_normalization` | Infers a canonical label per concept, then applies concept overrides |
| `refresh_hierarchy_normalization` | `hierarchy_normalization` | Expands `parent_concept` edges from overrides across every concept in the same group |
| `refresh_dimension_normalization` | `dimension_normalization` | Infers canonical axis and member labels, then applies dimension overrides |
| `refresh_financial_facts_normalized` | `financial_facts_normalized` | Joins all layers, resolves hierarchy, creates synthetic rows, rolls up, deduplicates |
| `refresh_quarterly_financials` | `quarterly_financials` | Converts YTD to discrete quarters and derives missing quarters |
| `refresh_yearly_financials` | `yearly_financials` | Annual presentation layer from 10-K facts |

Four behaviours are worth knowing before you debug anything:

**Precedence.** Everywhere, company-specific override beats global override beats
inferred value beats the raw filing label. Global rows are stored with
`company_id = 0` and `is_global = true`; the two must agree.

**Inference.** `concept_normalization` derives labels without any override, from
two signals: *grouping*, where one concept reports different labels across
filings and the most recent label wins; and *chaining*, where one period's
comparative value matches another period's value, which detects a renamed
concept. You only need an override when both signals fail.

**Synthetic rows.** When an override names a `parent_concept` or
`abstract_concept` that is not a real fact in that filing, the procedure invents
a row whose id is a hash of statement, concept, filing and company. Its value is
the weighted rollup of its children. This is how subtotals that a company never
tagged get created.

**Dedup.** Rows are partitioned by company, statement, normalized label, axis,
member and period, then ranked: amendments first, then longest comparative-value
chain, then lowest id. Losers get `is_duplicate = true`, and both
`yearly_financials` and `quarterly_financials` filter them out. If a value
vanishes from the presentation layer, this is usually why — check
`financial_facts_normalized` before assuming ingestion dropped it.

**Quarterly derivation.** A `YTD` fact becomes a discrete quarter by subtracting
the previous quarter when that quarter ended 80 to 100 days earlier. A quarter
absent from all 10-Qs is derived as the annual figure minus the quarters that do
exist, and marked `source_type = 'calculated'`. Balance Sheet and share-count
lines are point-in-time and come from the annual filing instead of being summed.

## Override cookbook

Three CSVs in [migrations/data/](migrations/data/), each the source of truth for
its table. `make overrides-load` replaces the table contents with the CSV, so
deleting a line removes the override. Always follow with `make refresh`.

Blank cells mean NULL. In `financial-facts-overrides.csv` only, the literal
`__EMPTY__` means "match the empty string" as opposed to blank, which means
"match anything".

### Symptom to fix

| Symptom | File | Key columns |
| --- | --- | --- |
| Two labels for the same line across years | `concept-normalization-overrides.csv` | `normalized_label` on both concepts |
| Line item sits under the wrong subtotal, or under none | `concept-normalization-overrides.csv` | `parent_concept`, `weight` |
| Missing section header in the statement tree | `concept-normalization-overrides.csv` | `is_abstract`, `abstract_concept` |
| Expense reported positive in some years, negative in others | `concept-normalization-overrides.csv` | `weight` |
| Segment or product members named differently across years | `dimension-normalization-overrides.csv` | `normalized_member_label` |
| Company-specific concept that should be a dimension of a standard concept | `financial-facts-overrides.csv` | `to_concept`, `to_axis`, `to_member`, `to_member_label` |
| One bad value in one filing only | `financial-facts-overrides.csv` | `from_period`, `to_period`, `form_type` |

Reach for `concept-normalization-overrides.csv` first. It is declarative and
global-capable, so one row can fix every company at once. Use
`financial-facts-overrides.csv` only when the fact itself is wrong, since it
rewrites raw data and is almost always company-specific.

### `concept-normalization-overrides.csv`

`company_id,concept,statement,normalized_label,is_abstract,is_global,abstract_concept,parent_concept,description,unit,weight`

Primary key is `(company_id, concept, statement)`. `parent_concept` and
`abstract_concept` must refer to another row in this file with the same
`statement` and `company_id`. Set `unit` on non-abstract rows and `weight`
whenever `parent_concept` is set.

Give every company the same label for cash:

```csv
0,us-gaap:CashAndCashEquivalentsAtCarryingValue,Balance Sheet,Cash and Cash Equivalents,False,True,,,,usd,
```

Attach payables to current liabilities so the subtotal adds up:

```csv
0,us-gaap:AccountsPayable,Balance Sheet,Accounts Payable,False,True,us-gaap:LiabilitiesCurrentAbstract,us-gaap:LiabilitiesCurrent,,usd,1.0
```

Declare the section header that row points at:

```csv
0,us-gaap:LiabilitiesNoncurrentAbstract,Balance Sheet,Non-current Liabilities,True,True,us-gaap:LiabilitiesAndStockholdersEquityAbstract,,,,
```

Relabel one company's concept without touching anyone else:

```csv
2,us-gaap:AdvertisingRevenue,Income Statement,Revenue,False,False,,,,usd,
```

### `dimension-normalization-overrides.csv`

`company_id,axis,member,member_label,is_global,normalized_axis_label,normalized_member_label,tags`

Match on `member` when the company uses a stable XBRL member, on `member_label`
when it only has a human label, and on neither to rewrite a whole axis. `tags`
is semicolon-separated.

```csv
2,us-gaap:StatementBusinessSegmentsAxis,goog:GoogleServicesMember,,False,Product,Google Services,
0,us-gaap:StatementBusinessSegmentsAxis,,Japan,True,Geographical,Japan,
0,srt:ProductOrServiceAxis,,,True,Product,,
```

The first maps one company's segment member. The second matches any company that
labels a segment "Japan". The third renames an axis globally and leaves members
alone.

### `financial-facts-overrides.csv`

`id,company_id,concept,statement,axis,member,label,form_type,from_period,to_period,to_concept,to_axis,to_member,to_member_label,to_weight,is_global`

Left-hand columns match, `to_*` columns rewrite. `to_axis`, `to_member` and
`to_member_label` must be all set or all blank; a check constraint enforces it.
The `id` column is preserved so admin CSV exports round-trip; new rows can leave
it blank.

Turn a company-specific revenue concept into a dimension of the standard one:

```csv
5,tsla:AutomotiveRevenues,Income Statement,__EMPTY__,__EMPTY__,,,,,us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax,srt:ProductOrServiceAxis,tsla:AutomotiveRevenuesMember,Automotive Revenue,,False
```

Flip a sign for one form type only:

```csv
6,5,us-gaap:IncomeTaxExpenseBenefit,Income Statement,,,,10-K,,,us-gaap:IncomeTaxExpenseBenefit,,,,-1,False
```

Scope a rewrite to a single filing period:

```csv
7,5,us-gaap:NetIncomeLoss,Income Statement,,,,,2018-09-30,,us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic,,,,,False
```

Rows referencing a `company_id` that has not been ingested are skipped with a
warning rather than failing the load, so overrides can be written ahead of time.

## Diagnostics

`make diagnose` runs everything in [sql/diagnostics/](sql/diagnostics/). Run a
single file with `psql "$DATABASE_URL" -f sql/diagnostics/03_duplicates.sql`.

| File | Finds |
| --- | --- |
| `01_coverage.sql` | Filings and facts per company and year, missing 10-K years, filings that yielded no facts |
| `02_unmapped.sql` | Concepts and dimensions with no normalization row, ranked by fact count |
| `03_duplicates.sql` | `is_duplicate` rows, one label split across concepts, one concept split across labels |
| `04_period_gaps.sql` | Missing years and quarters inside otherwise continuous series |
| `05_rollups.sql` | Parents that do not equal the weighted sum of children; synthetic subtotals stuck at zero |
| `06_orphans.sql` | `parent_id` and `abstract_id` pointing at rows that do not exist |
| `07_sign_flips.sql` | Series that change sign between periods; labels carrying conflicting weights |
| `08_quarterly_vs_annual.sql` | Fiscal years where four quarters do not sum to the 10-K figure |
| `09_conflicting_facts.sql` | Facts dropped before normalization because a group reports conflicting values |

Start with `01` and `02`. Coverage gaps and unmapped concepts cause most of what
the later files report, and fixing them removes those findings for free.

`09` deserves attention because nothing else surfaces it: the normalization
procedure silently discards every fact whose group reports more than one distinct
value for the same period, which is what a restatement looks like. If a line item
is missing and appears nowhere else, check here.

## Debugging beyond overrides

An override is the right fix when the source data is fine but our mapping is
wrong. When it is not, work down the layers.

**Isolate the layer.** Query each stage for one company and one concept: raw
`financial_facts`, then `financial_facts_overridden`, then
`concept_normalization`, then `financial_facts_normalized`, then
`yearly_financials`. The first stage where the value is wrong or absent is the
one to fix.

```sql
SELECT id, concept, label, value, axis, member, period_end, form_type
FROM financial_facts
WHERE company_id = 1 AND concept = 'us-gaap:Revenues'
ORDER BY period_end;
```

**Normalization SQL bug.** Suspect this when the raw fact is correct but the
normalized row is wrong in a way no override can express, or when the same
override behaves differently for two companies. Procedures live in
[sql/procedures/](sql/procedures/) and are installed by migration
[0007](migrations/versions/0007_add_financials_procedures.py). Editing a `.sql`
file does nothing until it is reinstalled: apply it with
`psql "$DATABASE_URL" -f sql/procedures/<name>.sql`, then `make refresh`. Add a
new migration to make the change permanent.

**edgartools bug.** Suspect this when the fact is wrong in
`financial_facts` but correct in the filing itself, which the `public_url` column
on `filings` links to. There is precedent in
[filings/parsers/sec_xbrl.py](filings/parsers/sec_xbrl.py): `_is_column_mostly_empty`
works around edgartools issue 408, where a period column comes back nearly empty,
and `_create_dimension_fact` strips a duplicated member-label prefix. Parser
fixes belong there. Re-ingest with `--override` afterwards, since existing rows
were written by the old parser.

**Bad SEC data.** Companies do restate, mistag and occasionally invert signs.
Confirm against the filing HTML before concluding this. The fix is a narrowly
scoped `financial-facts-overrides.csv` row bounded by `from_period` and
`to_period`, with a `description` in the concept CSV if a label needs explaining.
Do not widen the scope to make a diagnostic pass.

## Scaling to hundreds of companies

Prefer global overrides. A row with `company_id = 0` and `is_global = true`
applies everywhere, so the same effort that fixes one company fixes the universe.
Only fall back to a company-specific row when the concept is genuinely proprietary
to that filer.

Ingest in batches of roughly 20 to 50 tickers from a `TICKERS_FILE`, and diagnose
after each batch. Problems cluster by industry and filing agent, so a batch
usually surfaces a handful of new concepts that then apply to everything after it.

Refresh narrowly while iterating. `make refresh COMPANY_IDS=1,2` recomputes two
companies in seconds; a full refresh over hundreds of companies is far slower and
tells you nothing extra when you are testing one override.

Back up before bulk re-ingestion with `make docker-backup`. Re-ingesting with
`--override` deletes and reinserts facts, and the derived tables follow.

## Guardrails

- Never ingest a new company with a `LIMIT`. Load its full XBRL history first; a partial history breaks comparative-value chaining and fabricates gaps and disjoint series.
- Never `UPDATE` or `DELETE` in `financial_facts`. It is the raw record; corrections go in `financial-facts-overrides.csv`.
- Never hand-edit `financial_facts_normalized`, `quarterly_financials`, `yearly_financials`, or any `*_normalization` table. They are rebuilt on every refresh and your edit will vanish.
- Never edit the seeded override rows inside `migrations/versions/`. Edit the CSVs and run `make overrides-load`.
- `make overrides-load` replaces whole tables from the CSVs. If overrides were created through the admin API and not written back to the CSVs, export them first with `GET /admin/*/export`.
- Always `make refresh` after changing overrides, and always re-run `make diagnose` after refreshing. An unrefreshed database shows stale results and looks like the override failed.
- Do not widen an override's scope to silence a diagnostic. If a global override fixes one company and breaks another, the mapping is wrong.
