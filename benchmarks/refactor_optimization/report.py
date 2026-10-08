"""What each recorded step changed, as one self-contained HTML page.

    python -m benchmarks.refactor_optimization.report

Reads every `results/*.json` written by `bench.py`, orders them by date and writes
`docs/03-impacto.html`. Nothing here rejects a step: the page
only shows what moved. Every metric is lower-is-better.
"""
from __future__ import annotations

import argparse
import datetime
import json
import statistics
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_DEST = _HERE / "docs/03-impacto.html"
REPO = "https://github.com/MolSuite/ms_flow"

# One line per step, added in the PR that records it: label -> (PR number, what changed, still open).
STEPS = {
    "baseline": (None, "main before the refactor", False),
    "engine core": (None, "New engine next to the old one (ms_flow.engine): state in memory, CPU/GPU/memory tokens, "
                          "event loop, pools that recover. No journal yet, so nothing is written to disk. First job: "
                          "the workers import the whole ms_flow package (pool start alone is 75 ms).", True),
    "lazy import": (None, "The ms_flow package loads its top-level names on first use, so a worker that only runs chunks "
                          "imports the engine (16 ms) and not the whole API (358 ms).", True),
    "feeders": (None, "The source of chunks is read on a thread per job, a window ahead, so a slow generator holds only "
                      "its own job. Every chunk gets a content key (16-byte blake2b) and may carry its own CPU, GPU and "
                      "memory cost. Costs about 0.005 ms per chunk: the key and one more thread sharing the interpreter.",
                True),
    "journal": (None, "Jobs are recorded in executor.db again: raw sqlite3, one writer thread, one transaction every "
                      "50 ms, a 5-column row per finished chunk and no payload. A finished job keeps one row plus its "
                      "failures (the large job peaks at 37 bytes per chunk while it runs). One process owns each "
                      "journal through an operating system lock, so the second engine is refused.", True),
}

# What the design aims for once the journal is in (02-diseno-motor.md); without it, 0.05 ms per chunk.
TARGETS = {"thread_ms_per_chunk": 0.2, "process_ms_per_chunk": 0.2, "scale_last_ms": 0.2, "scale_first_ms": 0.2,
           "scale_growth": 1.1, "state_kb_per_chunk": 0.3}

# key, short name, chart title, how it is measured. The first CHARTED get a chart panel.
METRICS = [
    ("thread_ms_per_chunk", "thread", "Thread executor, cost per chunk (ms)",
     "Warm engine, one job of empty chunks on the thread executor with default settings; wall time ÷ chunks. Median of 3."),
    ("process_ms_per_chunk", "process", "Process executor, cost per chunk (ms)",
     "The same job on the loky process pool, already started. Median of 3."),
    ("scale_last_ms", "large job", "Late in a large job, cost per chunk (ms)",
     "One large thread job with 64 chunks in flight; cost per chunk over its last tenth."),
    ("state_kb_per_chunk", "state", "State on disk per chunk (KB)",
     "Size of executor.db after the large job ÷ its chunks."),
    ("first_job_ms", "first job", "First job on a fresh engine (ms)",
     "New engine, one-chunk job on the process executor, pool start-up included. Median of 3."),
    ("import_ms", "import", "Import time (ms)",
     "`import ms_flow.api` in a new interpreter. Median of 5."),
    ("scale_first_ms", "large job, start", "Early in a large job, cost per chunk (ms)",
     "The same large job; cost per chunk over its first tenth."),
    ("scale_growth", "growth", "Growth inside a large job (×)",
     "Last tenth ÷ first tenth of the large job. 1 means the cost does not depend on the job size."),
    ("peak_rss_mb", "peak RSS", "Peak memory of the benchmark process (MB)",
     "ru_maxrss of the main process at the end of the run; workers not included."),
]
CHARTED = 6

# key, name, what it does, task that is meant to make it pass.
CHECKS = [
    ("worker_death_isolated", "Worker death stays isolated",
     "One of 40 chunks kills its worker process; at most that chunk may fail.", "F0-1"),
    ("worker_death_pool_recovers", "Pool recovers after a worker death",
     "The next job on the same executor finishes 20 of 20.", "F0-1"),
    ("kill9_no_orphans", "No orphans after kill -9",
     "The main process is killed mid-job; no worker is alive 5 s later.", "F1-4"),
    ("second_engine_safe", "Second engine on the same file is safe",
     "A second engine on the same executor.db is refused, or both jobs finish complete.", "F1-3"),
]

NOISE_FLOOR_PTS = 3.0


def classify(delta_pts: float, noise_pts: float) -> str:
    if abs(delta_pts) <= noise_pts:
        return "noise"
    return "improves" if delta_pts < 0 else "worsens"


assert [classify(-10, 3), classify(2, 3), classify(5, 3)] == ["improves", "noise", "worsens"]


def summarize(records: list[dict]) -> list[dict]:
    """Per step and metric: median, range, % against the baseline and the step from the previous column.

    A step counts as a change only beyond the run-to-run range of both columns (or 3 points).
    A metric first measured in a later step takes that step as its baseline.
    """
    base: dict[str, float] = {}
    prev: dict[str, tuple[float, float]] = {}
    steps = []
    for rec in records:
        pr, title, is_open = STEPS.get(rec["label"], (None, "", False))
        cells = {}
        for key, m in rec["metrics"].items():
            med = statistics.median(m["values"])
            b = base.setdefault(key, med)
            cum = 100 * (med / b - 1)
            spread = 100 * (max(m["values"]) - min(m["values"])) / b
            cell = {"med": med, "lo": min(m["values"]), "hi": max(m["values"]), "unit": m["unit"], "cum": cum}
            if key in prev:
                cell["step"] = cum - prev[key][0]
                cell["kind"] = classify(cell["step"], max(NOISE_FLOOR_PTS, spread, prev[key][1]))
            prev[key] = (cum, spread)
            cells[key] = cell
        steps.append({"label": rec["label"], "pr": pr, "title": title, "open": is_open, "commit": rec["commit"],
                      "date": rec["recorded_at"][:10], "m": cells, "checks": rec.get("checks", {})})
    return steps


def _self_check() -> None:
    rec = lambda label, v: {"label": label, "commit": "x", "recorded_at": "2026-01-01", "metrics": {"a": {"values": v, "unit": "ms"}}}
    a = [s["m"]["a"] for s in summarize([rec("0", [10.0]), rec("1", [5.0]), rec("2", [5.1]), rec("3", [8.0])])]
    assert [c.get("kind") for c in a] == [None, "improves", "noise", "worsens"]
    assert round(a[1]["cum"]) == -50 and round(a[3]["step"]) == 29


_self_check()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--results", type=Path, default=_HERE / "results")
    ap.add_argument("--out", type=Path, default=_DEST)
    args = ap.parse_args()

    records = sorted((json.loads(p.read_text()) for p in args.results.glob("*.json")), key=lambda r: r["recorded_at"])
    quick = [r["label"] for r in records if r.get("quick")]
    if quick:
        print(f"skipped --quick runs, not comparable: {', '.join(quick)}")
    records = [r for r in records if not r.get("quick")]
    if not records:
        raise SystemExit(f"no results in {args.results}")

    data = {
        "date": datetime.datetime.now(datetime.timezone.utc).date().isoformat(), "repo": REPO,
        "machine": records[0]["machine"], "sizes": records[0]["sizes"],
        "steps": summarize(records),
        "metrics": [dict(zip(("key", "short", "title", "what"), m)) for m in METRICS],
        "charted": CHARTED,
        "targets": TARGETS,
        "checks": [dict(zip(("key", "title", "what", "task"), c)) for c in CHECKS],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(PAGE.replace("/*DATA*/", json.dumps(data, separators=(",", ":")).replace("</", "<\\/")))
    print(args.out, f"{args.out.stat().st_size / 1024:.0f} kB,", len(records), "steps")


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>ms_flow · what each step changed in the engine</title>
<style>
.viz-root {
  color-scheme: light;
  --surface-0: #f4f3f0; --surface-1: #fcfcfb; --border: #e2e1dc; --grid: #ecebe7;
  --text-primary: #0b0b0b; --text-secondary: #52514e; --text-muted: #7a7974;
  --series-1: #2a78d6; --ref: #7a7974; --good: #0a8a0a; --bad: #d03b3b;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) .viz-root {
    color-scheme: dark;
    --surface-0: #111110; --surface-1: #1a1a19; --border: #33332f; --grid: #2a2a27;
    --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #8f8e86;
    --series-1: #3987e5; --ref: #8f8e86; --good: #3fbf3f; --bad: #e66767;
  }
}
:root[data-theme="dark"] .viz-root {
  color-scheme: dark;
  --surface-0: #111110; --surface-1: #1a1a19; --border: #33332f; --grid: #2a2a27;
  --text-primary: #ffffff; --text-secondary: #c3c2b7; --text-muted: #8f8e86;
  --series-1: #3987e5; --ref: #8f8e86; --good: #3fbf3f; --bad: #e66767;
}
body { margin: 0; }
.viz-root { background: var(--surface-0); color: var(--text-primary);
  font: 14px/1.45 system-ui, sans-serif; padding: 24px; min-height: 100vh; box-sizing: border-box; }
h1 { font-size: 20px; margin: 0 0 4px; }
h2 { font-size: 15px; margin: 0 0 2px; }
a { color: var(--series-1); }
.sub { color: var(--text-secondary); margin: 0 0 16px; max-width: 1100px; }
.card { background: var(--surface-1); border: 1px solid var(--border); border-radius: 8px;
  padding: 16px; margin-bottom: 16px; max-width: 1100px; }
.tiles { display: flex; gap: 12px; flex-wrap: wrap; max-width: 1134px; margin-bottom: 16px; }
.tile { background: var(--surface-1); border: 1px solid var(--border); border-radius: 8px; padding: 12px 16px; flex: 1 1 180px; }
.tile .v { font-size: 28px; font-weight: 600; }
.tile .l { color: var(--text-secondary); font-size: 13px; }
.legend { display: flex; gap: 16px; flex-wrap: wrap; color: var(--text-secondary); font-size: 13px; margin: 6px 0 8px; }
.legend i { display: inline-block; width: 12px; height: 12px; border-radius: 3px; margin-right: 6px; vertical-align: -1px; }
svg text { fill: var(--text-secondary); font-size: 11px; paint-order: stroke; stroke: var(--surface-1); stroke-width: 4px; stroke-linejoin: round; }
svg .t { fill: var(--text-primary); font-size: 12px; font-weight: 600; }
svg .h { fill: transparent; }
svg .h:hover { fill: var(--text-primary); fill-opacity: .05; }
.note { color: var(--text-secondary); font-size: 13px; margin: 8px 0 0; max-width: 1000px; }
.scroll { overflow-x: auto; }
table { border-collapse: collapse; font-size: 13px; font-variant-numeric: tabular-nums; }
th, td { padding: 4px 8px; border-bottom: 1px solid var(--border); text-align: right; white-space: nowrap; }
th:first-child, td:first-child, td.l, th.l { text-align: left; }
th { color: var(--text-secondary); font-weight: 500; }
td.b, th.b { border-left: 1px solid var(--border); }
.good { color: var(--good); font-weight: 600; } .bad { color: var(--bad); font-weight: 600; } .muted { color: var(--text-muted); } td.w { white-space: normal; }
#tip { position: fixed; pointer-events: none; background: var(--surface-1); color: var(--text-primary);
  border: 1px solid var(--border); border-radius: 6px; padding: 6px 8px; font-size: 12px; display: none;
  box-shadow: 0 2px 8px rgba(0,0,0,.15); max-width: 460px; }
.toggle { float: right; background: none; border: 1px solid var(--border); color: var(--text-secondary);
  border-radius: 6px; padding: 4px 10px; cursor: pointer; }
</style>
</head>
<body>
<div class="viz-root">
<button class="toggle" onclick="const r=document.documentElement;r.dataset.theme=(r.dataset.theme==='dark'||(!r.dataset.theme&&matchMedia('(prefers-color-scheme: dark)').matches))?'light':'dark'">light / dark</button>
<h1>What did each step change in the engine?</h1>
<p class="sub" id="sub"></p>

<div class="tiles" id="tiles"></div>

<div class="card">
  <h2>Which step moved what</h2>
  <p class="sub">Step each change added, in points of the baseline value: −13 pts means the metric dropped by 13 % of what it was on the
    baseline. Everything is a cost, so down is better. An empty cell: the step did not measure that metric.</p>
  <div class="scroll" id="steptable"></div>
  <p class="note">A step inside the run-to-run range of the two columns it compares, or under 3 points, is noise on this machine and is shown as ≈.
    Nothing here blocks a change; the page only shows its effect.</p>
</div>

<div class="card">
  <h2>Engine cost after each step</h2>
  <p class="sub">Left to right: the baseline, then each step in the order it was recorded; every column includes the ones before it.
    Above each bar: the value, how many times lower than the baseline, and the step from the previous column.</p>
  <div class="legend"><span><i style="background:var(--ref)"></i>baseline</span>
    <span><i style="background:var(--series-1)"></i>after the step</span>
    <span><i style="background:var(--series-1);opacity:.55"></i>PR still open</span>
    <span><i style="background:none;border-top:2px dashed var(--ref);border-radius:0;height:0;vertical-align:3px"></i>baseline level</span>
    <span><b class="good">▼</b> step lowers the cost</span><span><b class="bad">▲</b> step raises it</span><span>≈ within noise</span></div>
  <div class="scroll" id="chart"></div>
</div>

<div class="card">
  <h2>Robustness checks</h2>
  <p class="sub">Each check runs a failure against the real engine on a fresh state file and looks at what is left.</p>
  <div class="scroll" id="checks"></div>
</div>

<div class="card">
  <h2>What needs a look</h2>
  <p class="sub">Every check that fails on the last step, and every step that raised a cost beyond noise.</p>
  <div class="scroll" id="findings"></div>
</div>

<div class="card">
  <h2>Table</h2>
  <p class="sub">Median of the repeats; the range is in the tooltip of each cell. Green: the value meets the design target of the last row.</p>
  <div class="scroll" id="table"></div>
</div>

<div class="card">
  <h2>How this was measured</h2>
  <p class="note"><b>Workload.</b> Empty tasks, so what is timed is the orchestration alone: with real tasks the same cost is added on top
    of the task time. Every run starts from a new state file and a temporary HOME.</p>
  <div id="how"></div>
  <p class="note"><b>Reading it.</b> The numbers are from one machine and move a few percent between runs; compare columns of this page
    with each other, not with another machine.</p>
  <p class="note">Record a step: <code>python -m benchmarks.refactor_optimization.bench --label &lt;step&gt;</code>, add its line to
    <code>STEPS</code> in <code>report.py</code>, then <code>python -m benchmarks.refactor_optimization.report</code>.</p>
</div>
<div id="tip"></div>
</div>

<script>
// ponytail: data pasted in by benchmarks/refactor_optimization/report.py; rerun it instead of editing here
const D = /*DATA*/;
const NS = 'http://www.w3.org/2000/svg';
const el = (p, n, a = {}, t) => { const e = document.createElementNS(NS, n); for (const k in a) e.setAttribute(k, a[k]); if (t != null) e.textContent = t; p.appendChild(e); return e; };
const esc = s => String(s).replace(/[&<>]/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;'}[c]));
const tip = document.getElementById('tip');
function hover(e, html) {
  e.addEventListener('mousemove', ev => { tip.innerHTML = html; tip.style.display = 'block';
    tip.style.left = Math.min(ev.clientX + 14, innerWidth - tip.offsetWidth - 8) + 'px';
    tip.style.top = Math.min(ev.clientY + 14, innerHeight - tip.offsetHeight - 8) + 'px'; });
  e.addEventListener('mouseleave', () => tip.style.display = 'none');
}
const num = v => v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v >= 1 ? v.toFixed(2) : v.toPrecision(2);
const val = c => `${num(c.med)} ${c.unit}`;
const times = c => c.cum < -3 && c.med > 0 ? `${(100 / (100 + c.cum)).toFixed(1)}× lower` : c.cum > 3 ? `${(1 + c.cum / 100).toFixed(1)}× higher` : 'same';
const stepText = c => c.kind === 'noise' ? '≈' : `${c.step < 0 ? '▼ −' : '▲ +'}${Math.abs(c.step).toFixed(0)} pts`;
const stepClass = c => ({improves: 'good', worsens: 'bad', noise: 'muted'})[c.kind];
const stepHtml = c => c?.kind ? `<span class="${stepClass(c)}">${stepText(c)}</span>` : '';
const prHtml = s => s.pr ? `<a href="${D.repo}/pull/${s.pr}">#${s.pr}</a>${s.open ? ' <span class="muted">open</span>' : ''}` : '';
const passed = s => Object.values(s.checks).filter(Boolean).length;
const charted = D.metrics.slice(0, D.charted), last = D.steps.at(-1);

function chart(host) {
  const n = D.steps.length, L = 14, W = Math.max(1000, L + 92 * n + 6), cw = Math.min(150, (W - L - 6) / n), P = 132;
  const cx = i => L + cw * (i + .5), H = 20 + charted.length * P + 40;
  const svg = el(host, 'svg', {width: '100%', viewBox: `0 0 ${W} ${H}`, style: `min-width:${W > 1000 ? W * .85 : 0}px`, role: 'img',
    'aria-label': 'Engine cost after each step, one panel per metric'});
  charted.forEach((m, p) => {
    const y0 = 20 + p * P, floor = y0 + P - 14, h = P - 62, cells = D.steps.map(s => s.m[m.key]);
    const top = Math.max(...cells.filter(Boolean).map(c => c.med)), by = v => floor - v / top * h;
    el(svg, 'text', {x: L, y: y0 + 10, class: 't'}, m.title);
    el(svg, 'line', {x1: L, x2: W - 6, y1: floor, y2: floor, stroke: 'var(--ref)'});
    const first = cells.findIndex(Boolean);
    if (first < 0) return;
    el(svg, 'line', {x1: cx(first), x2: L + cw * n, y1: by(cells[first].med), y2: by(cells[first].med), stroke: 'var(--ref)', 'stroke-dasharray': '4 3'});
    cells.forEach((c, i) => {
      if (!c) return;
      const bw = Math.min(46, cw * .5), y = by(c.med);
      el(svg, 'rect', {x: cx(i) - bw / 2, y, width: bw, height: Math.max(1, floor - y), rx: 2,
        fill: i === first ? 'var(--ref)' : 'var(--series-1)', opacity: D.steps[i].open ? .55 : 1});
      el(svg, 'text', {x: cx(i), y: y - (c.kind ? 18 : 5), 'text-anchor': 'middle', class: 't'},
        num(c.med) + (c.cum < -3 && c.med > 0 ? ` · ${(100 / (100 + c.cum)).toFixed(1)}×` : ''));
      if (c.kind) el(svg, 'text', {x: cx(i), y: y - 5, 'text-anchor': 'middle', style: `fill:var(--${c.kind === 'noise' ? 'text-muted' : stepClass(c)})`}, stepText(c));
    });
  });
  D.steps.forEach((s, i) => {
    el(svg, 'text', {x: cx(i), y: H - 22, 'text-anchor': 'middle', class: 't'}, s.label);
    el(svg, 'text', {x: cx(i), y: H - 8, 'text-anchor': 'middle'}, s.pr ? `#${s.pr} ${s.open ? 'open' : 'merged'}` : s.commit);
    hover(el(svg, 'rect', {x: L + cw * i, y: 4, width: cw, height: H - 4, class: 'h'}),
      `<b>${esc(s.label)}</b>${s.pr ? ` · PR #${s.pr}${s.open ? ' open' : ''}` : ''} · ${esc(s.commit)} · ${s.date}<br>${esc(s.title)}<br>`
      + charted.filter(m => s.m[m.key]).map(m => { const c = s.m[m.key];
        return `${m.short}: <b>${val(c)}</b>${i ? ` · ${times(c)}${c.kind ? ` · step ${stepText(c)}` : ''}` : ''}`; }).join('<br>')
      + `<br>checks: ${passed(s)} of ${Object.keys(s.checks).length} pass`);
  });
}

function stepTable(host) {
  const rows = D.steps.slice(1);
  if (!rows.length) { host.innerHTML = '<p class="note">No step recorded yet: the only column is the baseline.</p>'; return; }
  host.innerHTML = '<table><tr><th>Step</th><th>PR</th><th class="l">Change</th>' + charted.map((m, i) => `<th${i ? '' : ' class="b"'}>${m.short}</th>`).join('')
    + '<th class="b">checks</th></tr>' + rows.map((s, i) => { const gained = passed(s) - passed(D.steps[i]);
      return `<tr><td>${esc(s.label)}</td><td>${prHtml(s)}</td><td class="l w">${esc(s.title)}</td>`
        + charted.map((m, j) => `<td${j ? '' : ' class="b"'}>${stepHtml(s.m[m.key])}</td>`).join('')
        + `<td class="b">${gained > 0 ? `<span class="good">+${gained}</span>` : gained < 0 ? `<span class="bad">−${-gained}</span>` : '<span class="muted">≈</span>'}`
        + ` <span class="muted">${passed(s)}/${Object.keys(s.checks).length}</span></td></tr>`; }).join('') + '</table>';
}

function checks(host) {
  host.innerHTML = '<table><tr><th class="l">Check</th><th class="l">What it does</th>' + D.steps.map(s => `<th>${esc(s.label)}</th>`).join('') + '</tr>'
    + D.checks.map(c => `<tr><td>${c.title}</td><td class="l w muted" style="min-width:300px">${c.what}</td>`
      + D.steps.map(s => { const v = s.checks[c.key];
        return `<td>${v == null ? '' : v ? '<span class="good">✔ pass</span>' : '<span class="bad">✖ fail</span>'}</td>`; }).join('') + '</tr>').join('')
    + '</table>';
}

function findings(host) {
  const rows = D.checks.filter(c => last.checks[c.key] === false)
    .map(c => [c.title, `<span class="bad">✖ fails</span> on ${esc(last.label)}`, `${c.what} Planned fix: task ${c.task}.`]);
  D.steps.forEach((s, i) => D.metrics.forEach(m => { const c = s.m[m.key];
    if (c?.kind === 'worsens') rows.push([m.title, `${stepHtml(c)} at ${esc(s.label)}`, `${val(D.steps[i - 1].m[m.key])} → ${val(c)}. ${esc(s.title)}`]); }));
  host.innerHTML = rows.length ? '<table><tr><th class="l">What</th><th class="l">Where</th><th class="l">What is known</th></tr>'
    + rows.map(r => `<tr><td>${r[0]}</td><td class="l">${r[1]}</td><td class="l w" style="min-width:380px">${r[2]}</td></tr>`).join('') + '</table>'
    : '<p class="note">Nothing: every check passes and no step raised a cost beyond noise.</p>';
}

function table(host) {
  host.innerHTML = '<table style="font-size:12px"><tr><th>Step</th><th>PR</th><th class="l">Commit</th><th class="l">Date</th>'
    + D.metrics.map((m, i) => `<th${i ? '' : ' class="b"'} title="${m.title}">${m.short}</th>`).join('') + '<th class="b">checks</th></tr>'
    + D.steps.map(s => `<tr><td>${esc(s.label)}</td><td>${prHtml(s)}</td><td class="l">${esc(s.commit)}</td><td class="l">${s.date}</td>`
      + D.metrics.map((m, i) => { const c = s.m[m.key];
        return `<td class="${i ? '' : 'b'}${c && c.med <= D.targets[m.key] ? ' good' : ''}"${c ? ` title="${num(c.lo)} to ${num(c.hi)} ${c.unit}"` : ''}>${c ? val(c) : ''}</td>`; }).join('')
      + `<td class="b">${passed(s)}/${Object.keys(s.checks).length}</td></tr>`).join('')
    + '<tr><td class="muted">target</td><td></td><td></td><td></td>'
    + D.metrics.map((m, i) => `<td class="muted${i ? '' : ' b'}">${D.targets[m.key] ? '≤ ' + D.targets[m.key] : ''}</td>`).join('')
    + `<td class="muted b">${D.checks.length}/${D.checks.length}</td></tr></table>`;
}

document.getElementById('sub').innerHTML = `Report of ${D.date} · ${D.steps.length} column${D.steps.length > 1 ? 's' : ''}, baseline ${esc(D.steps[0].commit)} `
  + `of ${D.steps[0].date} · ${D.machine.cpus} logical CPUs, engine on ${D.machine.engine_cpus} · Python ${D.machine.python} · `
  + `${D.sizes.chunks_per_job.toLocaleString('en')} empty chunks per job, large job ${D.sizes.chunks_scale_job.toLocaleString('en')} · `
  + `data: <code>benchmarks/refactor_optimization/results</code>`;
document.getElementById('tiles').innerHTML = D.metrics.slice(0, 4).filter(m => last.m[m.key]).map(m => { const c = last.m[m.key];
  return `<div class="tile"><div class="v">${val(c)}</div><div class="l">${m.title.replace(/ \(.*/, '')} · `
    + (D.steps.length > 1 ? `<b>${times(c)}</b> than the baseline` : 'baseline') + '</div></div>'; }).join('')
  + `<div class="tile"><div class="v">${passed(last)} / ${Object.keys(last.checks).length}</div><div class="l">robustness checks pass on ${esc(last.label)}</div></div>`;
stepTable(document.getElementById('steptable'));
chart(document.getElementById('chart'));
checks(document.getElementById('checks'));
findings(document.getElementById('findings'));
table(document.getElementById('table'));
document.getElementById('how').innerHTML = D.metrics.map(m => `<p class="note"><b>${m.title.replace(/ \(.*/, '')}.</b> ${m.what.replace(/`([^`]+)`/g, '<code>$1</code>')}</p>`).join('');
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
