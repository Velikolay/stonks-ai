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
    raw --> p1[facts_overridden]
    p1 --> p2[concept_normalization]
    p2 --> p3[hierarchy_normalization]
    p3 --> p4[dimension_normalization]
    p4 --> p5[facts_normalized]
    p5 --> p5b[dimension_breakdowns]
    p5 --> p6[quarterly_financials]
    p5 --> p7[yearly_financials]
    csv["migrations/data/*.csv"] --> overrides[override tables]
    overrides --> p1
    overrides --> p2
    overrides --> p3
    overrides --> p4
    overrides --> p5
    overrides --> p5b
    overrides --> p6
    overrides --> p7
```

Raw facts are never modified. Every correction is expressed as an override row
and applied by re-running the refresh, so the whole derived state is
reproducible from EDGAR plus five CSVs. The refresh is the Python package
[filings/refresh/](filings/refresh/); each stage above is one module.

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

`make refresh` and the other Python targets read `DATABASE_URL` from the
environment or `.env`, and so does `make diagnose COMPANY_IDS=...`. The
`psql`-based targets (plain `make diagnose`, `make psql`) read it from the
environment and otherwise default to
`postgresql://rag_user:rag_password@localhost:5432/rag_db`.

## The loop

This is the operating procedure. Repeat until the stop condition holds.

```bash
# 1. Ingest the companies' full XBRL history (this is the default)
make ingest TICKERS=AAPL,MSFT FORMS=10-K,10-Q

# 2. Normalize (make ingest already refreshes; run this after editing overrides)
make refresh COMPANY_IDS=1,2

# 3. Diagnose the companies you just refreshed: every query, filtered to
#    them, without LIMIT, printing only sections that return rows
make diagnose COMPANY_IDS=1,2              # FILES=14,16 narrows to some files
make chart-audit TICKERS=AAPL,MSFT         # needs the API running

# 4. Fix: edit the CSVs in migrations/data/, then
make overrides-check         # dry run: shows what would change
make overrides-load
make refresh COMPANY_IDS=1,2

# 5. Back to step 3
```

**Stop when**, for the company under work, coverage shows no missing 10-K years
or empty filings, period gaps inside a real series are gone, orphaned parents
are gone, no fiscal year fails the four-quarters-sum-to-the-10-K check, Balance
Sheet identities hold, dimension axis splits are gone, `14` lists no member
the chart draws with a remainder, the first three queries of `15` list
nothing for the company but explained rows, and neither `16` nor the breaks
that `make chart-audit` prints list a period you have not explained. Explain
each remaining rollup, sign-flip, conflicting-fact, value-anomaly, and
remainder row as a source-data artifact. Segments get their own pass once the
rest holds; see [Aligning segments](#aligning-segments).

The stop condition is re-checked after every fix, for every company the fix
could touch, not only the period that prompted it. A user asking about one
broken quarter means the check was not run: before answering, run `16` and
the chart-audit breaks for that company and fix or explain everything they
list, so the next broken period is found by the diagnostics and not by the
user. A fix to one quarter often leaves its neighbour broken (Alphabet's
Q4 2018 and Q2 2019 had two different causes).

Duplicate and disjoint rows are not themselves a reason to keep going:

- An `is_duplicate` row whose twin is an amendment (a 10-K beside its 10-K/A,
  same concept and period) is the dedup step keeping one filing. Leave it.
- Two concepts that share a `normalized_label` over periods that do not overlap
  are a rename the label override already merged. `03` keeps listing that pair.
- Act when two concepts share a label in the same period, so dedup drops a
  value, or when one concept still carries two labels.

A concept with no `concept_normalization` row is acceptable when the presentation
label is already canonical. One whose presentation label is still the raw filing
label, and which appears in every filing, is not. Unlinked-series candidates
need not be empty, but each one must have been either merged with an override or
dismissed as a coincidence.

**Work one class of problem at a time.** Inside genuinely unmapped concepts, fix
the most frequent one, refresh, and re-diagnose. A single concept override often
closes dozens of downstream gaps, and fixing several classes at once makes it
impossible to tell which override did what.

### Ingest options

`make ingest` wraps [filings/scripts/ingest.py](filings/scripts/ingest.py):

| Variable | Meaning |
| --- | --- |
| `TICKERS` | Comma-separated, e.g. `AAPL,MSFT` |
| `TICKERS_FILE` | File with one ticker per line; `#` comments allowed |
| `FORMS` | Default `10-K,10-Q` |
| `LIMIT` | Max filings per ticker per form; `0`, the default, loads every available filing |

Company ids that the override CSVs use are pinned in
[migrations/data/company-ids.csv](migrations/data/company-ids.csv). Migration
0002 inserts those companies at those ids, and ingest uses the same file, so a
fresh database lands Apple on 1, Alphabet on 2, Amazon on 3, Microsoft on 4,
Tesla on 5, Uber on 6, Netflix on 7, and NVIDIA on 8. Company 0 stays the
global override owner. Add a row there before writing a company-specific
override for a new ticker. Ingest also attaches an earlier CIK when the
earliest 10-K lists that filer and that filer's own reports stop at or before
this company's first period. Alphabet picks up Google Inc., CIK 1288776, that
way, through the quarter before the October 2015 reorganization.

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

A filing's fiscal year and quarter come from its cover, and after every load
[fiscal_periods.py](filings/fiscal_periods.py) checks them against the
company's year ends. The year ends are the period ends of its annual reports.
A quarter follows from how far the period end sits before the next year end.
The year follows from that year end and the company's convention, which is the
offset most of its covers use: the year a fiscal year ends in, or for some
retailers the year it starts in. A cover that disagrees is corrected and
logged, and the company is refreshed. Netflix's FY2012 10-K declares 2011,
NVIDIA's fiscal 2021 10-Qs declare 2020, and early covers that declare no
period fell back to calendar quarters. A period end that is no whole number of
quarters before a year end keeps its cover. A plain `make ingest` applies the
check to filings already stored.

### What a filing yields

The parser ([filings/parsers/sec_xbrl/](filings/parsers/sec_xbrl/)) stores the
five face statements, the income statement with its dimension rows, and
segment amounts from the notes. A segment amount is a dimension row of the
statement that holds its concept, so it splits that line the way segment
revenue splits the revenue line:

- Revenue members a note tags but the face leaves out join the face revenue
  line, under the face concept ([segments.py](filings/parsers/sec_xbrl/segments.py)).
- A segment amount whose concept is on a face statement joins that statement:
  goodwill and assets by segment on the Balance Sheet, depreciation and
  purchases of property on the Cash Flow Statement. The Income Statement wins
  when a concept is on several ([measures.py](filings/parsers/sec_xbrl/measures.py)).
- A segment measure on no face statement goes by its period. A balance on
  members the filing reports revenue on (long-lived assets by country) goes to
  the Balance Sheet. An amount for a period that its note reports beside
  segment revenue (the ASU 2023-07 segment expenses, segment capital spending,
  a filer's own segment profit measure) goes to the Income Statement, where it
  is a line with members and no undimensioned total.
- Only currency amounts cut by segment axes alone are kept. Percentages,
  counts, a segment further cut by debt instrument or plan, and roll-forwards
  whose note reports no revenue stay out.

Such a line links to revenue through its axis and member. Member labels are
inferred per line, so two lines normally agree because the filer labels a
member the same way everywhere; a `dimension-normalization-overrides.csv` row
applies on every statement and settles a disagreement.

Segment notes often tag a segment's amount twice over: `ConsolidationItemsAxis
= OperatingSegmentsMember` (the whole of the segments) beside the segment on
`StatementBusinessSegmentsAxis`. edgartools stores the first axis, with a
member such as "Operating Segments - Google Cloud". The refresh reads such a
fact on its real slice instead (`on_real_slice` in
[rules/facts.py](filings/refresh/rules/facts.py)): when the stored axis's
coordinate is an axis total and exactly one coordinate is a real slice on a
segment, geography or product axis, the fact takes that coordinate's axis,
qname member and label, the key a single-dimension fact on that segment has.
A fact whose filing also tags that slot alone with another value (a segment
total beside external revenue) keeps its stored axis. A fact that the same
filing also tags on its stored slot with fewer real coordinates is a finer cut
of that amount and is dropped (`without_finer_cuts`): Microsoft's fiscal 2016
first-quarter 10-Q tags Corporate and Other operating income as -1,274 and,
with `IncomeStatementLocationAxis` = impairment and restructuring, as 0, and
the two values used to drop the slot. A fact override still matches the fact
as filed, and its axis wins. A concept that later
appears on the face keeps its label and moves to the face statement's rows.
Notes other than segments (income tax, leases) are not stored.

## Stage reference

Run in this order; each stage reads the previous one.
[pipeline.py](filings/refresh/pipeline.py) chains all eight per company, keeps
each stage's output in memory for the next, and writes the company's rows in
one transaction. Several companies run at once (`--jobs`, default 4).

```bash
make refresh COMPANY_IDS=1,2                     # every stage
make refresh COMPANY_IDS=1 STAGES=quarterly_financials
python -m filings.refresh --companies 1 -v       # per-stage rows and timings
```

A stage run on its own reads its upstream tables from the database, so you can
rerun one stage after a code change without recomputing the rest.

| Stage | Module | Writes | What it does |
| --- | --- | --- | --- |
| `facts_overridden` | [facts_overridden.py](filings/refresh/stages/facts_overridden.py) | `financial_facts_overridden` | Applies `financial_facts_overrides` rules, rewriting a fact's concept, dimensions and weight |
| `concept_normalization` | [concept_normalization.py](filings/refresh/stages/concept_normalization.py) | `concept_normalization` | Infers a canonical label per concept, then applies concept overrides |
| `hierarchy_normalization` | [hierarchy_normalization.py](filings/refresh/stages/hierarchy_normalization.py) | `hierarchy_normalization` | Expands `parent_concept` edges from overrides across every concept in the same group |
| `dimension_normalization` | [dimension_normalization.py](filings/refresh/stages/dimension_normalization.py) | `dimension_normalization` | Infers canonical axis and member labels, then applies dimension overrides |
| `facts_normalized` | [facts_normalized/](filings/refresh/stages/facts_normalized/) | `financial_facts_normalized` | Joins all layers, resolves hierarchy, creates synthetic rows, rolls up, deduplicates |
| `dimension_breakdowns` | [dimension_breakdowns/](filings/refresh/stages/dimension_breakdowns/) | `dimension_breakdowns` | Infers which sets of members add up to another member or to the undimensioned line, then applies breakdown overrides |
| `quarterly_financials` | [quarterly/](filings/refresh/stages/quarterly/) | `quarterly_financials` | Converts YTD to discrete quarters and derives missing quarters |
| `yearly_financials` | [yearly.py](filings/refresh/stages/yearly.py) | `yearly_financials` | Annual presentation layer from 10-K facts |

Rules shared by several stages (override precedence, the two conflict rules,
rounding tolerances, unit canonicalisation, hashed row ids, sort order and
rounding) live in [filings/refresh/rules/](filings/refresh/rules/). Ties a
rule cannot break (two labels last reported in the same period) are broken
deterministically: text sorts ignoring case, then lowercase first, the same on
every machine, so a refresh gives the same result every time.

Four behaviours are worth knowing before you debug anything:

**Precedence.** Everywhere, company-specific override beats global override beats
inferred value beats the raw filing label. Global rows are stored with
`company_id = 0` and `is_global = true`; the two must agree. A company-specific
row replaces the global row for that concept. Blank columns do not fall through
to the global values, so copy every global field you still want and change only
what differs.

**Inference.** `concept_normalization` derives labels without any override, from
two signals: *grouping*, where one concept reports different labels across
filings and the most recent label wins; and *chaining*, where one period's
comparative value matches another period's value, which detects a renamed
concept. The table only receives concepts that one of those signals selected. A
line with a single stable label has no row there, which is not a defect: a
global override is applied later, directly, in the `facts_normalized`
stage. You need an override when both signals
fail and the presentation label is still the raw filing label.

**Synthetic rows.** When an override names a `parent_concept` or
`abstract_concept` that is not a real fact in that filing, the refresh invents
a row whose id is a hash of statement, concept, filing and company. Its value is
the weighted rollup of its children. This is how subtotals that a company never
tagged get created.

**Breakdowns.** Summing every member of an axis over-counts, for two reasons.
An axis can hold a subtotal beside its parts (Apple's Products beside iPhone,
iPad, Mac and Wearables), and it can hold two splits of the same amount
(Amazon's product and service sales beside its seven product lines; regions
beside countries). A breakdown is a set of members on one axis that adds up
exactly to a parent, which is another member or the undimensioned line
(`parent_member = ''`), with the fiscal years where that held in the 10-K.
After dedup, the `dimension_breakdowns` stage keeps a set that adds up in at
least two 10-Ks (a year where one of its members could be swapped for another
member of equal value does not count, so a region and a country that coincide
never mix) and writes it into `dimension_breakdowns`; a set that only expands
another one (iPhone, iPad, Mac, Wearables and Services beside Products and
Services) is dropped. A year where the set falls short is not stored. Members
coarser than their parent, such as millions a later 10-K reprinted beside a
total in thousands, may miss it by half a unit each, up to 0.1%.
Diagnostic 14 finds those years by comparing the members to
`yearly_financials`: at least two members present, none negative, and short of
a positive parent. The shortfall is unallocated. Several breakdowns of one
parent are alternatives when they begin together. A set that stops before the
axis's latest year and began before a set reaching that year is an earlier
presentation of that set, even when it overlaps it for a year or two
(Alphabet's U.S., U.K. and rest of the world, reported beside its 2015 regions
through 2016). The API sends every set with its `fiscal_years`; the chart does
not offer an earlier presentation as a view but draws a period of a view on
the most recent of the view's earlier presentations that adds up when the view
does not, so the later split wins wherever both apply. The test is per view:
Microsoft's fiscal 2025 product lines continue its fiscal 2018-2021 and older
product lines, while product and service revenue, current since fiscal 2018,
takes only the sets that began before 2018. A stopped set that began with
every current set stays a view. Before a view's first period that adds up,
a period that none of its earlier presentations closes draws the metric bar
alone, not the view's members that happen to share a name with an older
split: Microsoft's fiscal 2017 quarters exist on product lines only as the
ASC 606 restatement in the fiscal 2018 10-Qs, 7% above the revenue filed. `make chart-audit` prints each view's exact
periods for the key lines (`views` in the JSONL), since the best view can add
up everywhere while the one a reader picks does not. The API leaves out a set when fewer than two of its members exist
at the requested granularity, so a split reported only in the 10-K is absent
from a quarterly response. With `breakdowns=true`, the API returns
`breakdowns` once on the series: each
entry is an axis and its members. Show one at a time. For a
period, sum the members that have a value; a shortfall is unallocated and a
sum past the line means that split does not apply there. Members reprinted
coarser than the total, such as millions beside an exact dollar total, may
overshoot by half a rounding unit each, up to 0.1%. That split still applies
and the bars stay at the filed values. Drill down through a
member's own `breakdowns`. A member in no breakdown is not additive. A flat
axis wider than 16 members is skipped, and so are per-share amounts. A fact with several stored XBRL dimensions is not part of that flat list,
unless every extra dimension is the axis default or a member carried by every
cell of that axis, both of which mean the whole rather than a second cut.
The fact's breakdown is every sibling that shares the other dimensions and adds
up to the fact with exactly those dimensions; that set is not searched and is
not capped. A declared breakdown still matches members the flat list left out.

**Dedup.** Rows are partitioned by company, statement, normalized label, axis,
member and period, then ranked: amendments first, then the period's own filing,
then the later filing, then a published quarter over the year-to-date total of
the same date, then the longest comparative-value chain, then lowest id. The
undimensioned line is only kept from the period's own filing, so its members
are too. A later filing fills members the period's own filing left out only
when it extends that split: if it prints any shared member at another value,
beyond rounding, it recast the period (NVIDIA's fiscal 2025 10-K carving
Singapore out of fiscal 2023 and 2024), and all of its members for that period
become duplicates, which only the Q4 calculation reads. One rounding unit is a
re-rounding, not a recast: Alphabet's 2017 first-quarter 10-Q reprinted the
2016 quarter with Google at 20,092 million against 20,091, and adds the Google
properties, Network and other revenues the 2016 10-Q never tagged. A later
filing that reprints the period on a complete new presentation, sharing no
changed member, stays beside the original as a second view; the chart draws
the later one (Microsoft's fiscal 2027 first-quarter 10-Q reprinting
September 2025 on new segments). The exception is a
restatement that fixes a split. A fiscal year moves onto a later filing's
members on one axis when four things hold:

- the year's 10-K split misses the line by more than 0.1%;
- a later 10-K's members, or two or more of them when the split carries a
  subtotal beside its parts, add up to the line as filed;
- that later 10-K keeps every member the original had;
- each quarter of the year with a split follows onto a later filing that
  recast it, adding up to the quarter's own line where one exists.

The earliest such filing wins. If any quarter cannot follow, the year stays
as filed, so a year is never half on one basis. Amazon's FY2015 10-K segment
operating income left out stock-based compensation (4,523 against 2,233);
its FY2016 10-K restated it to 1,425 + (699) + 1,507 = 2,233, and the 2016
10-Qs restated the quarters. The undimensioned line always stays as filed.
A row that rounds to the
winner's value, within 0.1%, beats it, so a later reprint in millions does not
replace the original's digits. Losers get `is_duplicate = true`, and both `yearly_financials` and
`quarterly_financials` filter them out. If a value vanishes from the
presentation layer, this is usually why — check `financial_facts_normalized`
before assuming ingestion dropped it.

**Quarterly derivation.** A `YTD` fact becomes a discrete quarter by subtracting
the previous quarter when that quarter ended 80 to 100 days earlier, or that
quarter's year-to-date total when it was published beside it. A statement
header on the same label is not that previous quarter. A dimension
column marked as a quarter is stored as that quarter, and its year-to-date
total is kept only so Q4 can subtract the nine-month figure. Q4 is
derived as annual minus Q1–Q3 and marked `source_type = 'calculated'`, but only
when all three earlier quarters already exist for that series (label + axis +
member). A YTD with no earlier quarter in the series is not a quarter and is
left out of the quarterly table, unless the next year's filing reports that
missing period as a comparative. That comparative is added as a
`comparative_backfill` prior period, so the opening YTD reduces to one
quarter, and the backfill row is filtered out of the result. When the year's own
first three quarters overshoot its 10-K by more than 5% and the next year's
quarterly comparatives still fit under that 10-K, those comparatives replace
the three quarters and Q4 is the 10-K minus them. Other undimensioned income
statement lines of that year follow when the next year's annual comparative
still equals this 10-K, so a subtotal stays on the same basis. Per-share
amounts and share counts stay on the filed quarters. A reversal smaller than
5%, or comparatives that overshoot the same way, stays as filed. Q4 is otherwise the annual
figure of that same member minus the nine-month YTD on the quarter before the
10-K. A later filing can recast the year under new members, so every member's
Q4 on one axis comes from the same 10-K, including rows dedup ranked lower: an
amendment, then the year's own 10-K unless a restatement replaced all its
members, then one on the quarters' split (its
members all have quarters and every member with quarters is in it), then the
first 10-K filed after the quarters (a backfilled comparative does not count),
then the latest before them. A member that 10-K leaves out comes from the next
one in that order. The year's own 10-K yields to a later one when it introduced a line its
quarters lack, no 10-Q prints the nine months on its split, and a later 10-K
pairs with a 10-Q whose nine months carry that line. Q4 then uses only that
later 10-K's members. Microsoft's fiscal 2023 10-K split Dynamics out of Office
and Other, so subtracting the old quarters gave Other a Q4 of -3,924. The nine months come from the 10-Q a year later when the
first 10-K after it prints the year as this 10-K did and all its members on that
axis are the 10-K's, and its comparative nine-month line equals the year's own
(Alphabet's 2013 10-Qs reprinted 2012 without Motorola's home business), unless
next year's comparatives already replaced that line's quarters: a 10-K that moved a line out of another (Tesla's fiscal
2021 10-K taking regulatory credits out of automotive sales) subtracts the
reprint on its own split. Otherwise they come from a 10-Q whose members are the
10-K's, the year's own first, so a 10-K on new members subtracts a 10-Q that
reprinted the nine months on them and not quarters filed on the old ones.
Without such a 10-Q, Q1–Q3 are subtracted. A member of the first-ranked
10-K gets Q4 from those nine months even when its Q1–Q3 are hidden as a recast
(Amazon's 2016 10-K introduced new categories, and only the 2017 10-Qs printed
2016's quarters on them). A later gap
in an existing series stays as its own quarter. Rows in
`financial-rewrites.csv` then delete, replace, or add one member for one
period. `granularity=quarterly` is applied while building
`quarterly_financials`, and `granularity=yearly` while building
`yearly_financials`. A quarterly row leaves the annual figure as filed.
Sparse series (e.g. a geo member tagged only in Q1 and
the 10-K) are left without a calculated Q4 rather than dumping the residual
into the year-end quarter. Balance Sheet and share-count lines are
point-in-time and come from the annual filing instead of being summed. Per-share
amounts are not residualled from an unsplit YTD.

## Override cookbook

Five CSVs in [migrations/data/](migrations/data/), each the source of truth for
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
| Same segment on Product for Revenue but on `StatementBusinessSegmentsAxis` (or Geographical) for Operating Income or segment costs | `dimension-normalization-overrides.csv` | the segment's qname on `us-gaap:StatementBusinessSegmentsAxis`, `normalized_axis_label` = revenue's axis |
| Segment members never add up to Operating Income | `dimension-normalization-overrides.csv` | the corporate, unallocated or elimination member (`CorporateNonSegmentMember`, `MaterialReconcilingItemsMember`, `IntersegmentEliminationMember`) onto the segment axis |
| A set of members that adds up to a subtotal or to the line is missing, or one is a coincidence | `dimension-breakdown-overrides.csv` | `parent_member`, `members` |
| One member is wrong for one period on the quarterly or yearly layer | `financial-rewrites.csv` | `granularity`, `action`, `period_end`, `value` |
| The same fact must also appear on another axis, and the original line stays | `financial-facts-overrides.csv` | `action=clone` |
| Company-specific concept that should be a dimension of a standard concept | `financial-facts-overrides.csv` | `to_concept`, `to_axis`, `to_member`, `to_member_label` |
| Advertising and other revenue filed as their own concepts, with no axis | `financial-facts-overrides.csv` | `to_concept` is the face revenue concept, plus `to_axis`, `to_member`, `to_member_label` |
| One bad value in one filing only | `financial-facts-overrides.csv` | `from_period`, `to_period`, `form_type`, `from_filing_period`, `to_filing_period` |

Reach for `concept-normalization-overrides.csv` first. It is declarative and
global-capable, so one row can fix every company at once. Use
`financial-facts-overrides.csv` only when the fact itself is wrong, since it
rewrites raw data and is almost always company-specific.

`financial-rewrites.csv` is the last resort. Prefer a parser fix when the
amount is in the filing's XBRL and never lands in `financial_facts`. Prefer an
override when the fact is stored and the label, parent, member, sign, or
period is wrong. A rewrite types one number onto the quarterly or yearly layer
after derivation, and only when neither of those can express the correction.

### `financial-rewrites.csv`

Last resort, after a parser fix and an override. One row edits one member on
one already-derived layer.

`company_id,statement,normalized_label,axis,member,period_end,granularity,action,value`

One row edits one member on one layer. `granularity` is `quarterly` or `yearly`. `delete` removes it, `set` replaces its value, and `add` inserts a member copied from another member of that axis and period, or from the undimensioned line when that axis has no row yet. Blank `axis` and `member` are the undimensioned line. `value` is set for `set` and `add` only. A quarterly row leaves the annual figure as filed.

```csv
8,Income Statement,Revenue,Geographical,Other,2023-07-30,quarterly,delete,
```

### `concept-normalization-overrides.csv`

`company_id,concept,statement,normalized_label,is_abstract,is_global,abstract_concept,parent_concept,description,unit,weight`

Primary key is `(company_id, concept, statement)`. `parent_concept` and
`abstract_concept` must refer to another row in this file with the same
`statement` and `company_id`. Set `unit` on non-abstract rows and `weight`
whenever `parent_concept` is set. Quote any field that contains a comma,
including `description`; an unquoted comma shifts the columns and the loader
reads `usd` as the weight.

Do not set concept `weight` when the raw fact weight is already `-1`. The
normalized value is negated when `raw weight × override weight` is negative, and
the stored weight is the override, so a `+1` override on a `-1` fact flips the
sign and the rollup subtracts the line. Correct that fact with
`to_weight` in `financial-facts-overrides.csv` instead. Any weight change is a
chaining input on the next refresh: two unrelated lines that now share an amount
will link. Re-check those chains before merging labels.

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
6,us-gaap:StatementBusinessSegmentsAxis,uber:RidesMember,,False,Product,Mobility,
8,srt:ConsolidationItemsAxis,us-gaap:CorporateNonSegmentMember,,False,us-gaap:StatementBusinessSegmentsAxis,All Other,
```

The first maps one company's segment member. The second matches any company that
labels a segment "Japan". The third renames an axis globally and leaves members
alone. The fourth is the Operating-Income case: segment revenue, operating
income and the ASU 2023-07 segment expenses all arrive on
`StatementBusinessSegmentsAxis` with the segment's qname once the refresh reads
`OperatingSegmentsMember` facts on their real slice, so one row per qname puts
every line on revenue's axis, and a legacy qname (Rides) joins its successor.
Grouping is per metric, so inference will not pull OI onto Product by itself.
The fifth puts the corporate member beside the segments so they add up to the
line. Key these rows on the qname: a row on `ConsolidationItemsAxis` with a
label such as `Operating Segments - X` only matches facts the refresh could
not move (see `15`). `13_dimension_axis_splits.sql` and
`15_segment_alignment.sql` flag these splits.

An override labels a whole group, not one member. Members that share an XBRL
member name, a member label, or a normalized label are one group, and the
override of the group's latest member wins. Tesla's 2014–2017 credits on
`us-gaap:ProductOrServiceAxis` and the 2018 10-K's on `srt:ProductOrServiceAxis`
both use `tsla:RegulatoryCreditsMember`, so two overrides on them land on one
label. To give part of a series its own label, rewrite those facts onto another
member with `financial-facts-overrides.csv` (bounded by `from_period` and
`to_period`), and check `dimension_normalization` after the refresh.

### `dimension-breakdown-overrides.csv`

`company_id,normalized_label,axis,parent_member,members`

One row is one breakdown, written with normalized axis and member labels.
`members` is semicolon-separated. A blank `parent_member` is the undimensioned
line. A blank `normalized_label` applies the row to every metric that reports
the members on that axis. There are no global rows.

The rows for one parent on one axis are its complete list of breakdowns: they
replace everything inferred for that parent on that axis, and other axes keep
their inferred breakdowns. Restate any inferred alternative you want to keep. A
row with blank `members` leaves the parent without breakdowns.

```csv
1,Revenue,Geographical,,U.S.;China;Other countries
1,Revenue,Geographical,,Americas;Asia-Pacific;China;Europe;Japan
5,Revenue,Product,Automotive Revenues,Automotive sales;Automotive regulatory credits;Automotive leasing
5,Revenue,Product,Total revenues from sales and services,
```

The first two pin Apple's two geographic splits. The third declares a split
that inference missed (it needs two matching 10-K years). The fourth removes
every breakdown of a subtotal. A declared breakdown skips the two-year and
expansion rules but, like an inferred one, only lands on rows where its members
add up, so a wrong one shows in `14`. Overriding a parent also hides any split
that inference would find for it later.

### `financial-facts-overrides.csv`

`id,company_id,concept,statement,axis,member,label,form_type,from_period,to_period,to_concept,to_axis,to_member,to_member_label,to_weight,is_global,action,from_filing_period,to_filing_period`

Left-hand columns match, `to_*` columns rewrite. `to_axis`, `to_member` and
`to_member_label` must be all set or all blank; a check constraint enforces it.
`to_concept` is required on every row, including a weight-only rewrite; repeat
the source concept there. `from_period` and `to_period` bound the fact's
`period_end`; `from_filing_period` and `to_filing_period` bound the fiscal
period of the filing it came from, so a rewrite can leave a later filing's
comparatives alone. Bound a member rewrite by filing period whenever a later
filing restated those periods. A rewrite bounded only by `from_period` and
`to_period` also catches the restated reprints. `dimension_normalization`
then sees two values in one slot and drops the slot, so the target member
gets no mapping. Alphabet's overrides 63 and 65 left Q2 and Q3 2019 other
revenues on the raw axis that way, drawn as "Google (remainder)". The `id` column is preserved so admin CSV exports
round-trip. The loader inserts the file in one batch, so once any row has an
`id`, a new row needs the next integer too. A blank `id` fails the load.

`action` is `rewrite` or `clone`. Blank means `rewrite`: one matching row
replaces the fact, and a company-specific row beats a global one. `clone`
keeps the fact and inserts an extra copy for every matching row, so one
undimensioned line can be placed on two axes. A clone requires `to_axis`,
`to_member` and `to_member_label`. Its `to_concept` is what the copy is
labeled as; the source concept is unchanged. Point a dimension override at
the clone's member so the copy lands on Product or Geographical.

Turn a company-specific revenue concept into a dimension of the standard one:

```csv
5,tsla:AutomotiveRevenues,Income Statement,__EMPTY__,__EMPTY__,,,,,us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax,srt:ProductOrServiceAxis,tsla:AutomotiveRevenuesMember,Automotive Revenue,,False
```

A revenue note sometimes lists the product split as its own concepts, with no axis. The parser keeps those lines as filed when they add up to the face total. Rewrite them onto the product axis. `to_concept` is the revenue line on the face, and `to_member` is the member later filings use for that line, so the years join one series:

```csv
9,us-gaap:AdvertisingRevenue,Income Statement,__EMPTY__,__EMPTY__,,,,,us-gaap:Revenues,srt:ProductOrServiceAxis,us-gaap:AdvertisingMember,Advertising,,False
9,us-gaap:OtherSalesRevenueNet,Income Statement,__EMPTY__,__EMPTY__,,,,,us-gaap:Revenues,srt:ProductOrServiceAxis,us-gaap:ServiceOtherMember,Other revenue,,False
```

Flip a sign for one form type only:

```csv
6,5,us-gaap:IncomeTaxExpenseBenefit,Income Statement,,,,10-K,,,us-gaap:IncomeTaxExpenseBenefit,,,,-1,False
```

`to_weight` only flips the value against a concept weight: the value is
negated when the fact's weight and the resolved concept weight disagree, and
with no concept weight the fact's own weight is the resolved one. A concept
whose global row leaves `weight` blank (Operating Income) needs a
company-specific concept row with `weight` 1 beside the fact override. AMD's
10-Ks from FY2022 tag the All Other operating loss as a positive
`MaterialReconcilingItemsMember` amount; fact override 134 sets `to_weight`
-1 and the company 10 `us-gaap:OperatingIncomeLoss` row sets weight 1. Check
that no other fact of that concept has raw weight -1 first, or it flips too.

Scope a rewrite to a single filing period:

```csv
7,5,us-gaap:NetIncomeLoss,Income Statement,,,,,2018-09-30,,us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic,,,,,False
```

Give a line its own member while it meant something else. Tesla counted
regulatory credits inside automotive sales until the fiscal 2021 10-K, and that
10-K's comparatives moved them out, so the rewrite is bounded by the filing:

```csv
95,5,us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax,Income Statement,srt:ProductOrServiceAxis,tsla:AutomotiveRegulatoryCreditsMember,,,,,us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax,srt:ProductOrServiceAxis,tsla:RegulatoryCreditsInAutomotiveSalesMember,Regulatory credits included in automotive sales,,False,,,2021-09-30
```

Keep an undimensioned hedge and also report it on both cuts. The source
concept stays; each clone is revenue on one axis:

```csv
66,2,us-gaap:RevenueNotFromContractWithCustomer,Income Statement,__EMPTY__,__EMPTY__,,,,,us-gaap:Revenues,us-gaap:StatementBusinessSegmentsAxis,goog:OtherRevenueHedgingGainLossMember,Hedging gains (losses),,False,clone
67,2,us-gaap:RevenueNotFromContractWithCustomer,Income Statement,__EMPTY__,__EMPTY__,,,,,us-gaap:Revenues,srt:StatementGeographicalAxis,goog:GeographicRevenueHedgingGainLossMember,Hedging gains (losses),,False,clone
```

Rows referencing a `company_id` that has not been ingested are skipped with a
warning rather than failing the load, so overrides can be written ahead of time.

## Diagnostics

`make diagnose COMPANY_IDS=7` runs every section of
[sql/diagnostics/](sql/diagnostics/) filtered to those companies, without the
files' `LIMIT`, and prints only the sections that return rows (at most 200
rows each). `FILES=14,16` restricts it to files by prefix. Use it, not the
plain target: plain `make diagnose` runs each file through `psql` for every
company, most queries `LIMIT` 50, and on a populated database that first page
belongs to other filers. Microsoft's negative Q4 FY2023 Other was in `11` all
along, one row of 68 for Microsoft alone and off the global page. Run one
file by hand with `psql "$DATABASE_URL" -f sql/diagnostics/03_duplicates.sql`.
Refresh stays narrow too: `make refresh COMPANY_IDS=7`.

The queries that mirror a display rule (`14`'s remainder query mirrors
`_breakdowns_to_show` in [api/financials.py](api/financials.py)) must change
in the same commit as the rule. A query that disagrees with the chart lists
periods the chart draws correctly, teaches you to skip it, and hides the
period it draws wrongly. `make chart-audit` runs the UI's own code and cannot
drift that way, so when the two disagree, trust the audit and fix the query.

| File | Finds |
| --- | --- |
| `01_coverage.sql` | Filings and facts per company and year, missing 10-K years, filings that yielded no facts |
| `02_unmapped.sql` | Concepts with no `concept_normalization` row, and dimensions with no normalization row. A missing concept row is not a defect when the presentation label is already canonical |
| `03_duplicates.sql` | `is_duplicate` rows, one label carried by several concepts, one concept split across labels. Amendment twins and non-overlapping renames are expected; same-period clashes are not |
| `04_period_gaps.sql` | Missing years and quarters inside otherwise continuous series |
| `05_rollups.sql` | Parents that do not equal the weighted sum of children; synthetic subtotals stuck at zero |
| `06_orphans.sql` | `parent_id` and `abstract_id` pointing at rows that do not exist |
| `07_sign_flips.sql` | Series that change sign between periods; labels carrying conflicting weights |
| `08_quarterly_vs_annual.sql` | Fiscal years where four quarters do not sum to the 10-K figure |
| `09_conflicting_facts.sql` | Slots where filings disagree. Dedup keeps the latest filing; a conflict inside that filing is dropped |
| `10_unlinked_series.sql` | Series that should be one series: renames the inference step failed to link |
| `11_value_anomalies.sql` | Negative / outlier Q4s, repeating Q4 anomalies, yearly spikes, rare sign outliers |
| `12_balance_sheet_structure.sql` | BS identities and missing Current / Non-current section abstracts |
| `13_dimension_axis_splits.sql` | Same segment under Product for one metric and ConsolidationItems/Segments for another; product-like members not remapped to Product |
| `14_dimension_breakdowns.sql` | Exact breakdown years that do not add up in yearly_financials, axes with years no exact breakdown covers, added overrides that never add up, years a known breakdown falls short, members the chart draws with a remainder, every breakdown with its years |
| `15_segment_alignment.sql` | Segment members a line reports on another axis than revenue, profit and cost lines on revenue's segment axis with no breakdown, segment amounts still under an axis total, breakdowns that are earlier presentations |
| `16_chart_breaks.sql` | One period that looks wrong beside its neighbours: members covering a different share of the line than the periods around it, a member negative between positive periods, two members carrying the same values under two names |

Start with `01` and `02`, scoped to the company. Coverage gaps and concepts
whose presentation label is still raw cause most of what the later files report,
and fixing them removes those findings for free.

`02` lists concepts grouping and chaining never selected. Its `sample_label` is
`MIN(label)`, which is alphabetical, so read the latest filing label before
writing an override. Call the concept unmapped only when no override matches it
and `yearly_financials` or `quarterly_financials` still shows the raw label. A
statement-of-equity or comprehensive-income concept that uses several labels in
one filing (beginning balance, ending balance, adoption adjustments) has
grouping disabled on purpose. Do not force it onto one label.

`13` catches a UI-shaped gap that `02` misses: the members are normalized, but
Revenue is on `Product` while Operating Income (or similar) is still on
`StatementBusinessSegmentsAxis` or `ConsolidationItemsAxis`. Filter-by-Product
then looks empty for that metric. Remap the segment qnames to `Product` in
`dimension-normalization-overrides.csv` (see the cookbook example). `13` only
compares against `Product`; `15` compares every line against revenue's own
axis, Geographical segments included, and is the one to clear. Its "product-like"
query flags a member by name, so a company whose segments deliberately stay on
the segment axis (S&P Global's Mobility) keeps a row there.

`14` checks the breakdowns. Its first query must stay empty: a row there means
an exact `fiscal_years` entry no longer adds up in `yearly_financials`, which
is a refresh bug. The second lists axes with 10-K years that no exact
breakdown of the line covers. The shortfall query compares each breakdown's
members to the 10-K years outside `fiscal_years` and shows the unallocated
share; it is often a member left off the axis or two members colliding on one
label, so fix the dimension mapping before accepting the gap. Treat the second
query's axes as candidates: add a row to `dimension-breakdown-overrides.csv`
only for a split you confirmed in the filing. Partial disclosures (one country
beside regions), segment profit before unallocated corporate costs, and
equity-component or share-class axes are expected and stay listed.

The remainder query is what the chart shows. It takes the breakdowns the API
sends at each granularity (every set, earlier presentations included) and
lists members, quarterly and yearly, where no sent split of the member adds
up and one falls short, which the chart draws as
"<member> (remainder)". A shortfall against the undimensioned line is the gray
metric bar, which partial disclosures leave, so it is not listed. The usual
causes, with `present_in_first` naming the members that were found:

- A member is missing in those periods. Check whether its own filing tagged it
 under another concept (Tesla's energy leasing under
 `OperatingLeasesIncomeStatementLeaseRevenue` in 2019–2021), and rewrite those
 facts onto the line's concept. A later filing that recast the period does
 not fill it, because dedup marks all of its members as duplicates.
- A stray member shares a label with a split member (Tesla's 2013 resale value
 guarantee revenue filed as "Automotive sales"). Rewrite it onto its own member.
- A member was relabeled into years where the rest of its split was never
 reported (credits inside automotive sales before the 2018 split). Give those
 years their own label, which the grouping note under the dimension cookbook
 explains how to do.
- `max_remainder_pct` rounds to 0.0. A member was reprinted in millions or
 rounded to 100k beside an exact parent. The chart tolerates only one unit, so
 it still draws that small gap; explain it rather than override it.

`09` deserves attention because nothing else surfaces it: when filings disagree
on one slot, dedup keeps the latest filing and the earlier value stays on a
duplicate row. A slot whose latest filing still reports two values is dropped
entirely. If a
line item is missing and appears nowhere else, check here.

`11` catches years that pass `08` but still look wrong: a Q4 whose sign flips
against Q1–Q3, a Q4 that is more than 3× any earlier quarter, the same Q4
pattern repeating across years, a yearly value more than 5× the series median,
or a rare negative (or positive) in an otherwise one-sided series. Prefer the
repeating-Q4 query when triageing — one-off seasonality is common; a repeat is
usually a systematic YTD or calculated-Q4 bug. Confirm in the filing before
overriding.

`12` is the Balance Sheet shape check. `05` only verifies each parent against its
*direct* children, so a synthetic Non-current Assets bucket that never attaches
to Total Assets can look fine in `05` while the BS is still wrong. `12` asserts
the accounting identities below and that the Current / Non-current section
abstracts exist.

## Balance Sheet structure

The presentation tree — not the raw XBRL parent chain — must satisfy:

```
Total Assets      = Total Current Assets      + Total Non Current Assets
Total Liabilities = Total Current Liabilities + Total Non Current Liabilities
Total Assets      = Total Liabilities + Equity
```

Equity on the credit side means the full ownership residual: prefer
`Total Equity including NCI` when present, otherwise `Total Equity`, plus
redeemable / mezzanine NCI when the filer reports it. Equivalently, the BS total
`Total liabilities, redeemable non-controlling interests and equity` should equal
`Total Assets`.

Required abstracts (section headers, `is_abstract`) parallel the liability side:

| Abstract label | Typical concept |
| --- | --- |
| Current Assets | `us-gaap:AssetsCurrentAbstract` |
| Non-current Assets | `us-gaap:AssetsNoncurrentAbstract` |
| Current liabilities | `us-gaap:LiabilitiesCurrentAbstract` |
| Non-current Liabilities | `us-gaap:LiabilitiesNoncurrentAbstract` |

Filers often parent every asset line directly to Total Assets. That is valid XBRL
and will not fail chaining; it still fails `12` until overrides hang current lines
under Current Assets / `AssetsCurrent` and non-current lines under Non-current
Assets / `AssetsNoncurrent`, with those subtotals under Total Assets. Same pattern
for liabilities. Fix with `concept-normalization-overrides.csv` (`parent_concept`,
`abstract_concept`, and company-scoped scaffold rows when FKs require them).

A proprietary concept can only point at a parent stored under the same
`company_id`. The company scaffold replaces the global row for that concept,
blanks included, so copy `parent_concept`, `abstract_concept`, `weight`, and
`unit` from the global row. A scaffold `us-gaap:Liabilities` with an empty
`parent_concept` unparents Total Liabilities from the balance-sheet total.

## Working the unlinked-series candidates

`10_unlinked_series.sql` is different from the defect diagnostics: it reports
*candidates*, not defects. Its rows are pairs of series that look like the two
halves of one renamed line item, together with the evidence for that reading.
Deciding is your job, and the rules below keep that decision honest.

Nothing else finds these. `03` needs the two halves to already share a
`normalized_label`, and `04` only finds holes *inside* a series, so a series that
runs 2010 to 2015 beside a successor running 2016 to 2025 looks like two healthy
series to every other file.

### Why the link was missed

All rename detection rests on one join, `candidate_matches` in
[concept_normalization.py](filings/refresh/stages/concept_normalization.py),
which matches a filing's comparative value against the prior period's value.
Every condition on it is a way to miss a real rename: exact value equality fails
on a restatement, both sides are multiplied by `weight` so a weight override on
only one concept breaks the match, `form_type` must agree so a rename that
appears first in a 10-Q is invisible, and a missing `comparative_value` means
there is no bridge at all. A zero never bridges, since every line that did not
occur that year shares it, so a rename that first prints as zero is not linked. The same multiplication works in reverse: a weight
you just corrected can create a new match against an unrelated line that now
shares the amount. `false_matches` then drops a pair entirely for the whole
company and statement if any period triple chains or any mirrored pair
appears. `matches` in
[dimension_normalization.py](filings/refresh/stages/dimension_normalization.py)
repeats the same join and additionally requires the same `normalized_label`.

The grouping signal has its own bail-out: if a single filing uses two labels for
one concept, grouping is disabled for that concept everywhere. That is the most
common finding in the first query.

### The four queries

| Query | Signal |
| --- | --- |
| Series that stop early | The loose gate: two series in the same slot that never coexist, one ending where the other begins. `shares_concept` and `similar_concept_name` rank them; `successor_candidates = 1` means the seam is unambiguous |
| Value bridges not used | Re-runs the refresh's own join with one condition relaxed at a time. `why_unlinked` names the condition that had to give |
| Statement slots | A parent that lost exactly one child and gained exactly one at the next annual boundary |
| Axis members | An axis where exactly one member retired and exactly one took its place, siblings otherwise unchanged |

`why_unlinked` tells you which CSV to edit. "Magnitudes match, signs or weights
differ" is a `weight` override, not a label override. "Values within tolerance,
likely restated" means the numbers genuinely disagree, so confirm the restatement
before merging. "Exact bridge rejected by false_matches" means the evidence was
conclusive and a guard threw it away, which is the safest class to act on.

### Deciding

The queries are deterministic and never merge anything. Judgement happens
between the diagnostic and the CSV, and then stops being judgement: the override
row is reviewable in git and reapplied identically on every refresh. Do not push
scoring or thresholds into the refresh code, and never let a tolerance match merge
series automatically.

- **Act directly** on a clean one-out-one-in swap and on an exact bridge that only `false_matches` rejected. These are mechanical.
- **Confirm in the filing** for everything else. `filings.public_url` links to it. Adjacent periods and similar labels are a reason to open the 10-K, not a reason to write the override.
- **Prefer leaving it unlinked** when the evidence is only suggestive. The two errors are not symmetric: a missed link is loud and keeps reappearing here, while a wrong link produces a continuous-looking series with a silent discontinuity that passes every other diagnostic precisely because it looks well formed.
- **Record why** in the `description` column of `concept-normalization-overrides.csv`, including the filing and period you confirmed against, so the next reader can tell an established merge from an inferred one.

Expect false positives in the last two queries; two unrelated line items can
swap places at the same boundary by coincidence. That is the intended cost of a
candidate list, and it is why the filing is the arbiter.

## Aligning segments

A segment chart is right when every line the segment note reports (revenue,
cost of revenue, the ASU 2023-07 segment expenses, segment profit) sits on the
same normalized axis with the same member labels, and the members add up to
the line in every year the filing says they do. Revenue usually gets there
by itself; the other lines do not, because filers tag them on other axes and
leave the corporate and elimination pieces on `ConsolidationItemsAxis`.

### What the chart draws

`make chart-audit TICKERS=AMZN` runs the chart's own stacking code
(`resolveDimensionStack` from `stonks-ui`, `UI_DIR` defaults to
`../stonks-ui`) against the running API for every Income Statement line and
axis, and classifies each period of the line: `exact`, `member_remainder`,
`short` (the gray metric bar), `over` (members exceed the line, so nothing is
drawn), `unsplit`, `gap_inside`, `no_breakdown`. It prints, for revenue, cost,
gross profit, operating expenses and operating income, the periods where some
axis draws a split that adds up. Save a run with `OUT=` before a change and
pass it as `BASE=` after, the way `refresh-compare` works for tables. The
JSONL keeps each period's status, the gray share and the members that were
missing, which is where to look next.

The coverage table counts periods; it does not show the one quarter a reader
spots at a glance. The audit therefore also prints **breaks**, periods that
look wrong beside their neighbours on the same line, axis and granularity:
a status that stops adding up between two that do, drawn members that differ
from the periods before and after while those agree (a member missing for a
quarter, a member not opened into its split for a quarter), and a revenue or
cost member drawn negative after a positive period. It reads the same axes as
`16`. The audit only sees splits the API sends, so a line with no breakdown
(Microsoft's product lines from FY2022) has no breaks there; `16` reads the
members directly and covers it.

### The procedure, one company at a time

1. Find revenue's segment axis: `14`'s last query lists revenue's
   breakdowns. Segments go on `Product` beside product lines when the filer
   has no separate segment cut (Google, Amazon, AMD, Uber), on
   `Geographical` when the segments are regions (Apple, Costco), or stay on
   `us-gaap:StatementBusinessSegmentsAxis` when revenue already splits there
   (Microsoft, NVIDIA, S&P Global).
2. Run `15`. Its first query lists segment members a line reports on another
   axis than revenue. Map each qname on the axis the fact is read on
   (usually `us-gaap:StatementBusinessSegmentsAxis`) to revenue's axis and
   label, one row per qname, legacy names included.
3. Its second query lists profit and cost lines with segment members but no
   breakdown. Add the reconciling member to the same axis: corporate
   (`CorporateNonSegmentMember`, a filer's `AllOtherMember`), unallocated
   (`MaterialReconcilingItemsMember`), eliminations
   (`IntersegmentEliminationMember`). Check the identity in the 10-K first,
   segments plus that member equal the line, and write it in the row's
   description. Apple's FY2025 10-K: 175,677 + (42,627) = 133,050.
4. Its third query lists segment amounts still under an axis total. They are
   usually a different measure (Moody's segment totals before intersegment
   royalties, beside external revenue) and stay.
5. Refresh, rerun `make chart-audit` with `BASE=`, and fix what got worse.
   Two members with equal values in every year (a segment renamed between
   10-Ks, S&P's Commodity Insights and Energy for 2024) stop inference from
   finding any breakdown; give them one label. `16`'s last query lists them.
6. Read the chart period by period: `make diagnose COMPANY_IDS=<id>
   FILES=16` and the audit's breaks, at both granularities. Work each row
   through the table below until it is fixed or explained. This step is what
   finds the quarter a user would otherwise report.

### One period that looks wrong

Each row of `16` or of the audit's breaks is one of these. Find the cause in
the data before writing anything: the first stage where the member is wrong
(see [Isolate the layer](#debugging-beyond-overrides)) tells you which.

| What `16` or the audit shows | Usual cause | Fix |
| --- | --- | --- |
| A member missing in the latest period, and `16`'s last query pairs it with a new member that equals it in the comparatives | The 10-K renamed the member (Microsoft's FY2026 10-K files Gaming as `msft:XBOXMember`, equal for fiscal 2024-2025) | A fact override onto the old qname and label (override 137). A dimension override on the new qname would also take the label group of any older member with that name: the 2013-2017 Xbox line would become Gaming |
| A member negative or missing in Q4 only, its Q1-Q3 positive (`source_type = 'calculated'`) | The 10-K split differs from its quarters' split, and Q4 subtracts nine months on the old basis (Microsoft's FY2023 10-K carving Dynamics out of Other: Q4 Other = 211 - 4,135) | Read `q4_sources` from `--dump-dir`. A later 10-K paired with a 10-Q that prints the nine months on the new split takes over (see Quarterly derivation). If none exists, the year stays gray |
| A member missing in one or two quarters while its own filing tagged it, often with `"<parent> (remainder)"` | The slot dropped out of `dimension_normalization`: a fact override bounded only by period also caught a restated reprint, so the slot saw two values (Alphabet's overrides 63 and 65) | Bound the override by `from_filing_period` and `to_filing_period` |
| A parent member not opened into its split for one quarter (the audit shows its children missing, the parent added) | The children have no value in that quarter. For a Q4, an earlier quarter of the year lacks them, and Q4 is only calculated when Q1-Q3 exist (Alphabet's 2016: the Q1 10-Q tagged Google but not its children; next year's 10-Q reprinted them, beside Google one million off, which dedup took for a recast until a one-unit change counted as re-rounding) | Look for a later filing that printed the children for that quarter (`financial_facts_normalized`, `is_duplicate`) and why it lost. A child tagged under another concept or axis is an override; one never tagged is explained |
| Members over the line in one period, coverage doubling, with `new_members` | Two splits mixed. 16 already skips a later filing whose members alone cover the line as the neighbours do (a second presentation). A row here means the new members only make sense with the old ones, usually a renamed member reprinted beside its old name | Find the renamed pair in `16`'s last query and merge it (Microsoft's FY2026 10-K filing Search and News Advertising under the older `SearchAdvertisingMember`, fact override 141) |
| A segment or reconciling member missing in one quarter while the line and the other members are there | The filing tagged the member twice in that slot, once cut again by a second dimension. Before `without_finer_cuts` the two values dropped the slot (Microsoft's Corporate and Other, September 2014 and 2015) | Check `financial_fact_dimensions` for the fact ids in `09`'s conflicts. A second real coordinate is now dropped; a conflict left between two single-coordinate facts is the filer's |
| An earlier presentation unsplit in one year while the years around it split | A member renamed or moved to another qname in that year's filings, so the set no longer matches across 10-Ks (Microsoft's FY2013 10-K: Windows & Windows Live became Windows Division, and Unallocated and other moved onto `CorporateAndReconcilingItemsMember`) | A dimension override for a pure rename; a fact override bounded by filing period when the qname means something else in other years (overrides 138-140) |
| A member present in every quarter but one, with no rename and no override involved | The quarter's own filing never tagged it, or tagged it under another concept | Check `financial_facts` for that filing; rewrite the other concept onto the line, or explain it |
| A Q4 whose members exceed the line, with one member gone that quarter | The 10-K moved a business to discontinued operations after the third quarter, so the line's Q4 is the 10-K minus nine months that still include it (Alphabet's FY2012 10-K and Motorola Home: Q4 revenue 13,215 against 12,904 + 1,515 = 14,419 of members) | Explained: the members are on the 10-K's basis, the filed quarters are not |

Explaining a row means naming the filing and the period, as for any other
diagnostic. Earlier presentations, segment profit before unallocated costs and
a member reprinted coarser than its parent stay listed and stay explained; see
below.

### What stays unsplit, and why

- **Segment profit before unallocated costs.** Apple before FY2023, Amazon
  before 2016 and S&P Global report segment operating income that excludes
  R&D, stock compensation or corporate expense, and tag those only as
  reconciling items of another concept. The members overshoot the line
  (`over`) and the chart draws the gray bar. Do not invent the missing piece.
- **Earlier presentations.** After a re-segmentation (AMD's Computing and
  Graphics, NVIDIA's GPU and Tegra, Alphabet's old regions) the chart draws
  the older years on the older split, one chart per axis, as described under
  Breakdowns. A year still draws gray when neither the current split nor any
  earlier one adds up there. `15`'s last query lists the earlier sets.
- **As filed, unless a restatement fixes the split.** Dedup keeps the
  period's own filing, and a later filing that recast the period is hidden,
  except when the year as filed does not add up and the restatement does
  for the 10-K and every quarter (see Dedup). Amazon's FY2015 segments
  therefore show the FY2016 10-K's restated basis. Its FY2014 split stays as
  filed: the FY2016 10-K restated the year, but the only reprints of its
  quarters, in the 2015 10-Qs, still left stock-based compensation out.
- **Filer tagging errors.** Confirm in the instance (`filings.public_url`,
  or `Filing.xbrl()` in edgartools, whose facts carry the context
  dimensions) before writing an override. NVIDIA's FY2026 10-K tags the
  segment total on `CorporateNonSegmentMember`; fact override 135 moves it
  onto `OperatingSegmentsMember`, its twin, bounded by filing period, and the
  year's All Other is left missing rather than computed.

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

**Normalization code bug.** Suspect this when the raw fact is correct but the
normalized row is wrong in a way no override can express, or when the same
override behaves differently for two companies. Each stage is a module in
[filings/refresh/stages/](filings/refresh/stages/), a chain of named steps
that mirror the rules in this file. The quarterly stage decides one series,
line or axis-year at a time in plain Python functions, which a unit test can
call with a handful of records. To see what a step produced, dump the
intermediate frames a stage exposes and read them with polars:

```bash
python -m filings.refresh --companies 7 --stages quarterly_financials --dump-dir /tmp/dump
python -c "import polars as pl; print(pl.read_parquet('/tmp/dump/7/quarterly_financials.q4_sources.parquet'))"
```

A code change takes effect on the next `make refresh`; there is nothing to
install. Write the failing case as a unit test in
[tests/filings/refresh/](tests/filings/refresh/) first: those run without
Postgres in about two seconds (`pytest tests/filings/refresh`). A logic
change affects every company, so refresh them all and re-run `08` and `14`
unfiltered, not only for the company that prompted it. A dedup rule written for
one filer's recast dropped another filer's only copy of a member, and only `14`
showed it. To see every row the change moved, snapshot before and after and
diff them:

```bash
make refresh-snapshot OUT=backups/before
# ... change the code, then make refresh ...
make refresh-snapshot OUT=backups/after
make refresh-compare BASE=backups/before NEW=backups/after
```

The compare joins each derived table on its primary key, classifies every
difference (missing, extra, within rounding, value or column changed) and
counts the diagnostics queries that grew per company.

**edgartools bug.** Suspect this when the fact is wrong or missing in
`financial_facts` but present in the filing's XBRL, which the `public_url` column
on `filings` links to. Fix the parser and re-ingest so the fact is stored
with the rest of the filing. The parser is the package
[filings/parsers/sec_xbrl/](filings/parsers/sec_xbrl/), and there is precedent
in it: `columns.is_mostly_empty` works around edgartools issue 408, where a
period column comes back nearly empty, and `facts._display_member` strips a
duplicated member-label prefix. Segment members tagged only in a note are
added by `segments.py`. Parser fixes belong there. To check a parser change,
parse a fixed set of cached filings before and after and diff the facts; a
change to the comparative-column rules in `columns.py` shows up on 10-Q
balance sheets and statements of equity first. Re-ingest with `--override`
afterwards, since existing rows were written by the old parser.

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
tells you nothing extra when you are testing one override. Diagnose narrowly
too: `make diagnose COMPANY_IDS=1,2`, since the plain target's `LIMIT 50`
pages belong to other companies. A logic change is the exception: refresh
everything and compare snapshots, whose scorecard counts every query per
company.

Back up before bulk re-ingestion with `make docker-backup`. Re-ingesting with
`--override` deletes and reinserts facts, and the derived tables follow.

## Guardrails

- Never ingest a new company with a `LIMIT`. Load its full XBRL history first; a partial history breaks comparative-value chaining and fabricates gaps and disjoint series.
- Never `UPDATE` or `DELETE` in `financial_facts`. It is the raw record; corrections go in `financial-facts-overrides.csv`.
- Never hand-edit `financial_facts_normalized`, `quarterly_financials`, `yearly_financials`, or any `*_normalization` table. They are rebuilt on every refresh and your edit will vanish.
- Never edit the seeded override rows inside `migrations/versions/`. Edit the CSVs and run `make overrides-load`.
- `make overrides-load` replaces whole tables from the CSVs. If overrides were created through the admin API and not written back to the CSVs, export them first with `GET /admin/*/export`.
- Always `make refresh` after changing overrides, and always re-run `make diagnose COMPANY_IDS=...` after refreshing. An unrefreshed database shows stale results and looks like the override failed.
- Do not report a company's charts as fixed until `16` and the chart-audit breaks for it are clear or explained. Fixing only the period someone asked about leaves the next one for them to find.
- Do not widen an override's scope to silence a diagnostic. If a global override fixes one company and breaks another, the mapping is wrong.
- A company-specific row's `company_id` must be the filer whose members it names. Look it up in `company-ids.csv` before writing it; a row naming `amzn:` members under Microsoft's id matches nothing and fails silently.
