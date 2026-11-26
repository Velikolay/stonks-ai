// Chart audit: what the stacked chart in stonks-ui draws for every dimension.
//
// For each company, granularity, Income Statement line and axis, fetch the
// two series the UI fetches (/financials with breakdowns=true, with and
// without the axis), run the UI's own resolveDimensionStack on them, and
// classify each period of the undimensioned line:
//
//   exact             a split the API sends adds up
//   member_remainder  adds up, but a member is drawn with "(remainder)"
//   short             the split falls short; the gap is the gray metric bar
//   over              the members present exceed the line, nothing is drawn
//   unsplit           members exist that period but no sent split applies
//   gap_inside        no member that period, inside the members' span
//   no_breakdown      members exist but the API sends no split for the axis
//   no_members        nothing on the axis that period (outside its span)
//
// It then lists breaks: periods that look wrong beside their neighbours on
// the same line, axis and granularity, which is what a reader of the chart
// notices first. A period breaks when
//
//   status            it does not add up (gray over 0.5%, over, unsplit,
//                     gap_inside, a remainder) and the period before does,
//                     as does the period after unless this is the latest
//   members           the members it draws differ from the periods before
//                     and after, which agree with each other (a member
//                     missing for one quarter, a member not opened into its
//                     split for one quarter)
//   negative          it draws a member negative that is positive before
//                     (not on profit, income or tax lines, and not a
//                     corporate, reconciling, elimination or hedging member)
//
// Only named axes (Product, Geographical, Technology) and segment, product or
// region axes are read, as in 16_chart_breaks.sql.
// Breaks are printed per ticker and kept on each JSONL row under "breaks".
// Each row also keeps "views": every view the chart offers on the axis, with
// the periods it draws and the periods it draws exactly. With --tickers, the
// key lines' views are printed.
//
// Usage (the API must be running, Node 22.6+ for TypeScript type stripping):
//   node filings/scripts/chart_audit.mjs [--ui ../stonks-ui] [--tickers AAPL,MSFT]
//        [--api http://localhost:8000] [--out audit.jsonl] [--base before.jsonl]
//
// --base compares split coverage per line with an earlier --out file.
// --report reads an earlier --out file instead of the API and prints its
// summaries, so a saved run can be read again.
//
// Last it lists the axes that do not add up: every key line and charted axis
// whose periods with members do not all draw exactly, the work list of a
// company review.

import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const args = Object.fromEntries(
  process.argv.slice(2).reduce((pairs, arg, i, all) => {
    if (arg.startsWith("--")) pairs.push([arg.slice(2), all[i + 1] ?? ""]);
    return pairs;
  }, [])
);
const repo = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const uiDir = resolve(args.ui || join(repo, "../stonks-ui"));
const api = args.api || "http://localhost:8000";
const out = args.out || "chart_audit.jsonl";
const only = args.tickers ? args.tickers.split(",") : null;
const KEY = [
  "Revenue",
  "Net sales",
  "Cost of Revenue",
  "Gross Profit",
  "Operating Expenses",
  "Operating Income",
];

// The UI module imports its types from "@/lib/services/financial-data"; make
// that import type-only so Node can strip it, and load the copy.
const work = mkdtempSync(join(tmpdir(), "chart-audit-"));
const stackSource = readFileSync(
  join(uiDir, "src/components/charts/dimension-stack.ts"),
  "utf8"
).replace(
  /^import \{([^}]*)\} from "@\/lib\/services\/financial-data";/m,
  'import type {$1} from "./types.ts";'
);
writeFileSync(join(work, "types.ts"), "export {};\n");
writeFileSync(join(work, "dimension-stack.ts"), stackSource);
const { resolveDimensionStack } = await import(join(work, "dimension-stack.ts"));

async function get(path, params) {
  const url = new URL(api + path);
  for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
  const response = await fetch(url);
  if (response.status === 404) return [];
  if (!response.ok) throw new Error(`${response.status} ${url}`);
  return response.json();
}

function parse(raw) {
  return raw.map(item => ({
    name: item.member || item.normalized_label || "Total",
    data: (item.values || [])
      .map(v => ({
        date: v.period_end,
        value: typeof v.value === "number" ? v.value : parseFloat(v.value) || 0,
        fiscal_quarter: v.fiscal_quarter,
      }))
      .sort((a, b) => a.date.localeCompare(b.date)),
    breakdowns: item.breakdowns?.length
      ? item.breakdowns.map(b => ({
          axis: b.axis,
          members: [...new Set(b.members)],
          fiscal_years: b.fiscal_years?.length ? [...b.fiscal_years] : undefined,
        }))
      : undefined,
  }));
}

const tolerance = v => Math.max(1, Math.abs(v) * 1e-6);

async function auditAxis(ticker, granularity, label, statement, axis) {
  const base = { ticker, granularity, normalized_labels: label, statement, breakdowns: "true" };
  const [memberRaw, totalRaw] = await Promise.all([
    get("/financials/", { ...base, axis }),
    get("/financials/", base),
  ]);
  const series = parse(memberRaw);
  const total = parse(Array.isArray(totalRaw) ? totalRaw.slice(0, 1) : [totalRaw])[0];
  const stack = resolveDimensionStack(
    { ticker, metric: label, granularity, series, total },
    axis
  );
  const splits = (total?.breakdowns ?? []).filter(b => b.axis === axis);
  const byMember = new Map(series.map(s => [s.name, new Map(s.data.map(p => [p.date, p.value]))]));
  const memberDates = [...new Set(series.flatMap(s => s.data.map(p => p.date)))].sort();
  const cuts = stack.cuts.length ? stack.cuts : [{ label: "", periods: stack.periods }];
  // The metric's own bar ("Revenue", "Revenue (remainder of ...)") is gray.
  const isGray = leaf => leaf.name === label || leaf.name.startsWith(`${label} (`);
  const drawnOn = (cut, date) => {
    const leaves = cut.periods[date] ?? [];
    const members = leaves.filter(l => !isGray(l));
    return {
      gray: leaves.find(isGray)?.value ?? 0,
      remainders: members.filter(l => l.name.endsWith("(remainder)")).map(l => l.name),
      leaves: members.map(l => [l.name, l.value]),
    };
  };
  const isExact = (view, value) =>
    view.leaves.length > 0 && view.gray <= tolerance(value) && view.remainders.length === 0;
  const views = stack.cuts.map(cut => ({ label: cut.label, exact: 0, drawn: 0, periods: 0 }));
  const periods = [];
  for (const { date, value } of total?.data ?? []) {
    const drawn = cuts.map(cut => drawnOn(cut, date));
    drawn.forEach((view, i) => {
      if (!views[i]) return;
      views[i].periods += 1;
      if (view.leaves.length) views[i].drawn += 1;
      if (isExact(view, value)) views[i].exact += 1;
    });
    // The view a reader sees first: something drawn, the least gray.
    const best = drawn.reduce(
      (a, b) => (b.leaves.length > 0 && (a.leaves.length === 0 || b.gray < a.gray) ? b : a),
      drawn[0]
    );
    const present = series.filter(s => byMember.get(s.name).has(date)).length;
    let status;
    if (splits.length === 0) status = present ? "no_breakdown" : "no_members";
    else if (best.leaves.length === 0) {
      if (!present) {
        const inside = date > memberDates[0] && date < memberDates.at(-1);
        status = inside ? "gap_inside" : "no_members";
      } else {
        const over = splits.some(split => {
          const sum = split.members.reduce((acc, m) => acc + (byMember.get(m)?.get(date) ?? 0), 0);
          return split.members.some(m => byMember.get(m)?.has(date)) && sum - value > tolerance(value);
        });
        status = over ? "over" : "unsplit";
      }
    } else if (best.gray > tolerance(value)) status = "short";
    else if (best.remainders.length) status = "member_remainder";
    else status = "exact";
    periods.push({
      date,
      status,
      grayPct: value ? +((100 * best.gray) / Math.abs(value)).toFixed(2) : null,
      remainders: best.remainders,
      leaves: best.leaves,
    });
  }
  return { ticker, granularity, label, statement, axis, members: series.map(s => s.name), splits: splits.map(s => s.members), periods, views, breaks: CHARTED_AXIS(axis) ? breaks(periods, label) : [] };
}

const ADDS_UP = new Set(["exact", "no_breakdown", "no_members"]);
const addsUp = p => ADDS_UP.has(p.status) || (p.status === "short" && p.grayPct < 0.5);
const names = p => p.leaves.map(([n]) => n).sort().join("|");
// Breaks are read on the axes 16_chart_breaks.sql reads: axes an override
// named (Product, Geographical, Technology) and segment, product or region
// axes. A member that changes
// sign by nature, or a profit line whose segments can lose money, is not a
// negative break.
// An axis an override named (Product, Geographical, Technology) has no namespace.
const CHARTED_AXIS = axis => !axis.includes(":") || /segment|product|geograph|region/i.test(axis);
const SIGNED = /corporate|unallocated|reconcil|eliminat|hedg|intersegment/i;
const SIGNED_LINE = /income|profit|loss|margin|earnings|tax/i;

function breaks(periods, label) {
  const found = [];
  periods.forEach((p, i) => {
    const before = periods[i - 1], after = periods[i + 1];
    if (!before) return;
    const drawnBefore = new Map(before.leaves);
    if (!addsUp(p) && addsUp(before) && (!after || addsUp(after)))
      found.push({ date: p.date, kind: "status", detail: `${p.status}${p.grayPct ? ` gray ${p.grayPct}%` : ""}${p.remainders.length ? ` ${p.remainders.join(", ")}` : ""}` });
    if (after && before.leaves.length && names(before) === names(after) && names(p) !== names(before)) {
      const here = new Set(p.leaves.map(([n]) => n));
      const missing = [...drawnBefore.keys()].filter(n => !here.has(n));
      const added = [...here].filter(n => !drawnBefore.has(n));
      found.push({ date: p.date, kind: "members", detail: `missing ${missing.join(", ") || "-"}; added ${added.join(", ") || "-"}` });
    }
    const negative = p.leaves
      .filter(([n, v]) => v < 0 && (drawnBefore.get(n) ?? 0) > 0 && !SIGNED.test(n) && !SIGNED_LINE.test(label))
      .map(([n]) => n);
    if (negative.length) found.push({ date: p.date, kind: "negative", detail: negative.join(", ") });
  });
  return found;
}

const tickers = [];
const seen = new Set();
for (const line of readFileSync(join(repo, "migrations/data/company-ids.csv"), "utf8").trim().split("\n").slice(1)) {
  const [id, ticker] = line.split(",");
  if (seen.has(id) || (only && !only.includes(ticker))) continue;
  seen.add(id);
  tickers.push([Number(id), ticker]);
}

const rows = args.report
  ? readFileSync(args.report, "utf8").trim().split("\n").map(l => JSON.parse(l)).filter(r => !only || only.includes(r.ticker))
  : [];
const jobs = args.report ? [] : tickers.flatMap(([id, ticker]) =>
  ["yearly", "quarterly"].map(granularity => async () => {
    const labels = await get("/financials/normalized-labels", { ticker, granularity, statement: "Income Statement" });
    const combos = new Map();
    for (const l of labels) if (l.axis) combos.set(`${l.normalized_label}|${l.axis}`, [l.normalized_label, l.axis]);
    for (const [label, axis] of combos.values()) {
      try {
        rows.push({ company_id: id, ...(await auditAxis(ticker, granularity, label, "Income Statement", axis)) });
      } catch (error) {
        rows.push({ company_id: id, ticker, granularity, label, axis, error: String(error) });
      }
    }
  })
);
let next = 0;
await Promise.all(Array.from({ length: 6 }, async () => { while (next < jobs.length) await jobs[next++](); }));
if (!args.report) writeFileSync(out, rows.map(r => JSON.stringify(r)).join("\n") + "\n");

// Coverage: periods of each key line where some axis draws a split that adds up.
function coverage(list) {
  const ok = new Map(), all = new Map();
  for (const r of list) {
    if (!KEY.includes(r.label) || !r.periods) continue;
    const key = `${r.company_id}|${r.ticker}|${r.label}|${r.granularity}`;
    for (const p of r.periods) {
      if (!all.has(key)) { all.set(key, new Set()); ok.set(key, new Set()); }
      all.get(key).add(p.date);
      if (p.status === "exact" || p.status === "member_remainder") ok.get(key).add(p.date);
    }
  }
  return { ok, all };
}
const now = coverage(rows);
const before = args.base
  ? coverage(readFileSync(args.base, "utf8").trim().split("\n").map(l => JSON.parse(l)))
  : null;
console.log(args.report ? `read ${rows.length} rows from ${args.report}` : `wrote ${rows.length} rows to ${out}; ${rows.filter(r => r.error).length} errors`);
console.log("periods with a split that adds up / periods of the line");
console.log("co ticker metric               yearly        quarterly");
const lines = [...new Set([...now.all.keys()].map(k => k.split("|").slice(0, 3).join("|")))].sort(
  (a, b) => Number(a.split("|")[0]) - Number(b.split("|")[0]) || a.localeCompare(b)
);
for (const line of lines) {
  const cells = ["yearly", "quarterly"].map(g => {
    const key = `${line}|${g}`;
    const n = now.all.get(key)?.size ?? 0, y = now.ok.get(key)?.size ?? 0;
    const was = before ? `${before.ok.get(key)?.size ?? 0}->` : "";
    return `${was}${y}/${n}`.padEnd(13);
  });
  const [id, ticker, label] = line.split("|");
  console.log(`${id.padStart(2)} ${ticker.padEnd(6)} ${label.padEnd(20)} ${cells.join(" ")}`);
}

const broken = rows.filter(r => r.breaks?.length);
console.log(`\nbreaks: ${broken.reduce((n, r) => n + r.breaks.length, 0)} periods on ${broken.length} line/axis/granularity series`);
for (const r of broken.sort((a, b) => a.company_id - b.company_id || a.label.localeCompare(b.label) || a.axis.localeCompare(b.axis))) {
  for (const b of r.breaks)
    console.log(`${String(r.company_id).padStart(2)} ${r.ticker.padEnd(6)} ${r.granularity.padEnd(9)} ${r.label} | ${r.axis} | ${b.date} ${b.kind}: ${b.detail}`);
}

if (only) {
  console.log("\nviews: periods drawn exactly / drawn / periods of the line");
  for (const r of rows.filter(r => KEY.includes(r.label) && r.views?.length > 1)) {
    for (const v of r.views)
      console.log(`${String(r.company_id).padStart(2)} ${r.ticker.padEnd(6)} ${r.granularity.padEnd(9)} ${r.label} | ${r.axis} | ${v.exact}/${v.drawn}/${v.periods} ${v.label.slice(0, 90)}`);
  }
}

// Axes that do not add up: every key line and charted axis with members in a
// period that does not draw exactly. A reader picking that axis sees these
// periods gray or unsplit; each is a finding until fixed or explained. A
// gray under 0.5% (a member reprinted coarser) is left out.
const fine = p => p.status === "exact" || (p.status === "short" && p.grayPct < 0.5);
function runs(periods) {
  const out = [];
  for (const p of periods) {
    const last = out.at(-1);
    if (last && last.status === p.status) last.to = p.date;
    else out.push({ status: p.status, from: p.date, to: p.date });
  }
  return out.map(r => `${r.from.slice(0, 7)}${r.to !== r.from ? ".." + r.to.slice(0, 7) : ""} ${r.status}`);
}
const gaps = rows
  .filter(r => KEY.includes(r.label) && r.periods && CHARTED_AXIS(r.axis))
  .map(r => {
    const withMembers = r.periods.filter(p => p.status !== "no_members");
    return { r, withMembers, bad: withMembers.filter(p => !fine(p)) };
  })
  .filter(g => g.withMembers.length && g.bad.length)
  .sort((a, b) => a.r.company_id - b.r.company_id || a.r.label.localeCompare(b.r.label) || a.r.axis.localeCompare(b.r.axis) || a.r.granularity.localeCompare(b.r.granularity));
console.log(`\naxes that do not add up: ${gaps.length} line/axis/granularity series (exact / periods with members)`);
for (const { r, withMembers, bad } of gaps) {
  const shown = runs(bad);
  console.log(
    `${String(r.company_id).padStart(2)} ${r.ticker.padEnd(6)} ${r.granularity.padEnd(9)} ${r.label} | ${r.axis} | ` +
      `${withMembers.length - bad.length}/${withMembers.length} | ${shown.slice(0, 4).join("; ")}${shown.length > 4 ? ` (+${shown.length - 4})` : ""}`
  );
}
