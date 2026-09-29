"""The group page. DESIGN_PHASE1 §21.7.

A glanceable dashboard, not a merged transcript — "transcripts are for audits, ambient
cues are for monitoring". Per-agent detail stays one click away at /v0/agents/<corr>.

The percent-done bar is honest HERE and would not be on an agent page. Current guidance
on agent progress names the denominator problem: an agent cannot know it is 40% done
with its reasoning, so a per-agent percentage is theatre. A group's denominator is the
number of agents, which is declared and countable — so the bar is legitimate at group
level and the member rows show STATE, never a fabricated per-agent percentage.
"""
from __future__ import annotations

import html
import json
from string import Template
from typing import Any

_CSS = """
:root{--bg:#0f1115;--fg:#e6e6e6;--dim:#9aa0a6;--line:#262a31;--card:#161a20;
--ok:#3fb950;--run:#58a6ff;--fail:#f85149;--wait:#6e7681;--accent:#d29922}
*{box-sizing:border-box}
body{margin:0;padding:24px 16px;background:var(--bg);color:var(--fg);
font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}
.wrap{max-width:1040px;margin:0 auto}
h1{font-size:19px;margin:0 0 4px;font-weight:600}
a{color:var(--run);text-decoration:none}a:hover{text-decoration:underline}
.sub{color:var(--dim);font-size:13px;margin-bottom:20px}
.chip{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;
font-weight:600;vertical-align:middle}
.chip.running{background:#0d2d4d;color:var(--run)}
.chip.all,.chip.complete{background:#0f2e18;color:var(--ok)}
.chip.deadline,.chip.cancelled{background:#3d2b0d;color:var(--accent)}
.chip.quorum{background:#0f2e18;color:var(--ok)}
.chip.finished{background:#0f2e18;color:var(--ok)}
.chip.failed{background:#3d1416;color:var(--fail)}
.chip.submitted{background:#20242b;color:var(--wait)}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;
padding:16px 18px;margin-bottom:16px}
.headline{font-size:26px;font-weight:650;letter-spacing:-.4px}
.headline .of{color:var(--dim);font-weight:400;font-size:18px}
.pct{color:var(--dim);font-size:14px;margin-left:8px}
.bar{height:10px;background:#20242b;border-radius:6px;overflow:hidden;margin:14px 0 6px}
.bar>div{height:100%;transition:width .4s ease}
.bar .done{background:var(--ok);float:left}
.bar .fail{background:var(--fail);float:left}
.meta{display:flex;flex-wrap:wrap;gap:18px;color:var(--dim);font-size:13px}
.meta b{color:var(--fg);font-weight:600}
.note{margin-top:10px;padding:8px 10px;border-radius:6px;background:#2a2412;
color:var(--accent);font-size:13px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;color:var(--dim);font-weight:600;padding:6px 8px;
border-bottom:1px solid var(--line);font-size:12px;text-transform:uppercase;
letter-spacing:.4px}
td{padding:7px 8px;border-bottom:1px solid #1d2128;vertical-align:top}
tr:last-child td{border-bottom:none}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.txt{color:var(--dim);max-width:460px;overflow:hidden;text-overflow:ellipsis;
white-space:nowrap}
.sr{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0)}
.updated{margin-top:12px;color:var(--dim);font-size:12px}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--ok);
margin-left:6px;vertical-align:middle;animation:pulse 2s ease-in-out infinite}
.dot.off{background:var(--wait);animation:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
.back{font-size:13px;margin-bottom:14px}
"""


# The live-update script lives OUTSIDE the f-string template on purpose. Embedded in one,
# every literal JavaScript brace needs doubling, which is unreadable and was already the
# source of one bug. string.Template only substitutes $name, so JS braces pass through.
_JS = Template("""<script>
// Patch the DOM in place once a second. Deliberately NOT location.reload(): a reload
// throws away scroll position and makes a 100-row table flicker every second, and on a
// long batch the operator is watching this page precisely so they need not babysit it.
const S = $state;
const $$ = (id) => document.getElementById(id);
const esc = (v) => String(v == null ? "" : v).replace(/[&<>"']/g, function (c) {
  return {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c];
});

let lastPayload = "";
let lastFetch = Date.now();

function fmtAgo(ms) {
  const s = Math.round(ms / 1000);
  if (s <= 1) return "just now";
  if (s < 60) return s + "s ago";
  return Math.floor(s / 60) + "m" + String(s % 60).padStart(2, "0") + "s ago";
}

// The clock ticks between fetches too, so a stalled poll shows up as a growing number
// rather than as a page that merely looks fine.
setInterval(function () {
  const el = $$("updated");
  if (el) el.textContent = fmtAgo(Date.now() - lastFetch);
}, 1000);

function renderMeta(p) {
  const bits = ["<span><b>" + p.finished + "</b> finished</span>"];
  if (p.failed) bits.push("<span><b>" + p.failed + "</b> failed</span>");
  bits.push("<span><b>" + p.running + "</b> running</span>");
  bits.push("<span><b>" + p.queued + "</b> queued</span>");
  bits.push("<span>elapsed <b>" + esc(p.elapsed) + "</b></span>");
  if (p.eta) bits.push("<span>" + esc(p.eta) + " left</span>");
  if (p.throughput_per_min) {
    bits.push("<span><b>" + p.throughput_per_min + "</b>/min</span>");
  }
  return bits.join("");
}

function renderNotes(p) {
  const out = [];
  if (p.stall_note) out.push(esc(p.stall_note) + " — the batch may be stuck");
  if (p.denominator_revised) {
    out.push("more agents joined than declared: expected " + p.expected +
             " → " + p.denominator);
  }
  return out.map(function (n) { return '<div class="note">' + n + "</div>"; }).join("");
}

function renderRows(p) {
  return (p.members || []).map(function (m) {
    const dur = m.duration || "—";
    const txt = m.text || "";
    return '<tr><td class="mono"><a href="' + esc(S.base) + "/v0/agents/" +
      esc(m.correlationid) + '">' + esc(m.correlationid) + "</a></td>" +
      '<td><span class="chip ' + esc(m.status) + '">' + esc(m.status) + "</span></td>" +
      '<td class="mono">' + esc(dur) + "</td>" +
      '<td class="txt" title="' + esc(txt) + '">' + esc(txt.slice(0, 160)) + "</td></tr>";
  }).join("");
}

function paint(p) {
  const denom = p.denominator;
  let pctTxt = "";
  if (p.percent != null) {
    const v = Number.isInteger(p.percent) ? p.percent.toFixed(0) : p.percent;
    pctTxt = '<span class="pct">' + v + "%</span>";
  }
  $$("headline").innerHTML = denom
    ? p.terminal + ' <span class="of">of ' + denom + " agents finished</span>" + pctTxt
    : p.terminal + ' <span class="of">agents finished</span>';
  if (denom) {
    const d = (100 * p.finished / denom).toFixed(2);
    const f = (100 * p.failed / denom).toFixed(2);
    $$("barwrap").innerHTML =
      '<div class="bar" role="progressbar" aria-valuenow="' + p.terminal +
      '" aria-valuemin="0" aria-valuemax="' + denom +
      '" aria-label="agents finished">' +
      '<div class="done" style="width:' + d + '%"></div>' +
      '<div class="fail" style="width:' + f + '%"></div></div>';
  }
  $$("meta").innerHTML = renderMeta(p);
  $$("notes").innerHTML = renderNotes(p);
  const st = $$("state");
  if (st && p.state) { st.textContent = p.state; st.className = "chip " + p.state; }
  const rows = renderRows(p);
  if (rows) $$("members").innerHTML = rows;
}

function finish() {
  const dot = $$("livedot");
  if (dot) { dot.className = "dot off"; dot.title = "finished"; }
}

async function tick() {
  try {
    const r = await fetch("/v0/groups/" + S.groupid + "/status?full=1",
                          {cache: "no-store"});
    if (r.ok) {
      const p = await r.json();
      lastFetch = Date.now();
      // Only touch the DOM when something changed: rebuilding a 100-row table every
      // second for no reason is how a "live" page becomes unusable.
      const sig = JSON.stringify(p);
      if (sig !== lastPayload) { lastPayload = sig; paint(p); }
      if (p.completed_utc) { finish(); return; }
    }
  } catch (e) { /* keep the clock ticking; the next tick retries */ }
  setTimeout(tick, 1000);
}

if (S.done) { finish(); } else { setTimeout(tick, 1000); }
</script>""")


def _esc(v: Any) -> str:
    return html.escape("" if v is None else str(v))


def _bar(p: dict) -> str:
    """Done and failed as separate segments, so a failing batch looks different at a
    glance from a succeeding one."""
    denom = p.get("denominator") or 0
    if not denom:
        return ""
    done_pct = 100.0 * p["finished"] / denom
    fail_pct = 100.0 * p["failed"] / denom
    now = p["terminal"]
    return (
        f'<div class="bar" role="progressbar" aria-valuenow="{now}" aria-valuemin="0" '
        f'aria-valuemax="{denom}" aria-label="agents finished">'
        f'<div class="done" style="width:{done_pct:.2f}%"></div>'
        f'<div class="fail" style="width:{fail_pct:.2f}%"></div></div>'
    )


def _member_row(m: dict, base: str) -> str:
    corr = m["correlationid"]
    status = m.get("status") or "submitted"
    dur = m.get("duration") or "—"
    text = m.get("text") or ""
    return (
        "<tr>"
        f'<td class="mono"><a href="{_esc(base)}/v0/agents/{_esc(corr)}">{_esc(corr)}</a></td>'
        f'<td><span class="chip {_esc(status)}">{_esc(status)}</span></td>'
        f'<td class="mono">{_esc(dur)}</td>'
        f'<td class="txt" title="{_esc(text)}">{_esc(text[:160])}</td>'
        "</tr>"
    )


def render(p: dict, *, base_url: str = "") -> str:
    """Server-render the page. SSE then appends; a reload replays nothing."""
    gid = p.get("groupid") or "?"
    label = p.get("label") or gid
    state = p.get("state") or "running"
    denom = p.get("denominator")

    # Counts first, percentage second: "12 of 40 agents finished" can be turned into a
    # decision; "30%" cannot.
    headline = (f'{p["terminal"]} <span class="of">of {denom} agents finished</span>'
                if denom else f'{p["terminal"]} <span class="of">agents finished</span>')
    _p = p.get("percent")
    pct = (f'<span class="pct">{_p:.0f}%</span>' if _p is not None and float(_p).is_integer()
           else f'<span class="pct">{_p}%</span>' if _p is not None else "")

    meta = [f'<span><b>{p["finished"]}</b> finished</span>']
    if p["failed"]:
        meta.append(f'<span><b>{p["failed"]}</b> failed</span>')
    meta += [f'<span><b>{p["running"]}</b> running</span>',
             f'<span><b>{p["queued"]}</b> queued</span>',
             f'<span>elapsed <b>{_esc(p["elapsed"])}</b></span>']
    # Elapsed always; remaining only when it can be computed honestly.
    if p.get("eta"):
        meta.append(f'<span>{_esc(p["eta"])} left</span>')
    if p.get("throughput_per_min"):
        meta.append(f'<span><b>{p["throughput_per_min"]}</b>/min</span>')

    notes = []
    if p.get("stall_note"):
        notes.append(_esc(p["stall_note"]) + " — the batch may be stuck")
    if p.get("denominator_revised"):
        # Never let the bar move backward: the denominator was revised up, and we say so.
        notes.append(f'more agents joined than declared: expected '
                     f'{p.get("expected")} → {denom}')
    if p.get("running") and state != "running":
        notes.append(f'{p["running"]} agent(s) were still running when the group '
                     f'reached "{_esc(state)}"')

    rows = "".join(_member_row(m, base_url) for m in p.get("members") or [])
    if not rows:
        rows = '<tr><td colspan="4" class="txt">no members yet</td></tr>'

    live = json.dumps({"groupid": gid, "done": bool(p.get("completed_utc")),
                       "base": base_url})
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>group {_esc(label)}</title><style>{_CSS}</style></head>
<body><div class="wrap">
<h1>{_esc(label)} <span class="chip {_esc(state)}" id="state">{_esc(state)}</span></h1>
<div class="sub mono">{_esc(gid)}</div>

<div class="card">
  <div class="headline" id="headline">{headline}{pct}</div>
  <div id="barwrap">{_bar(p)}</div>
  <div class="meta" id="meta">{''.join(meta)}</div>
  <div id="notes">{''.join(f'<div class="note">{n}</div>' for n in notes)}</div>
  <div class="updated">last updated <span id="updated">just now</span>
    <span id="livedot" class="dot" title="live"></span></div>
</div>

<div class="card">
  <table><thead><tr>
    <th>agent</th><th>status</th><th>duration</th><th>latest</th>
  </tr></thead><tbody id="members">{rows}</tbody></table>
</div>
</div>
{_JS.substitute(state=live)}
</body></html>"""
