"""The group list page. DESIGN_PHASE1 §21.7.

One row per batch with its progress and — the point of this page — **when it was last
updated**. A list of batches whose rows might be minutes stale is worse than no list:
the operator cannot tell "nothing is happening" from "the page is not refreshing".

So every row carries an explicit age, the whole table refreshes in place every second
with no reload, and the header says when the data was fetched.
"""
from __future__ import annotations

import html
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
.sub{color:var(--dim);font-size:13px;margin-bottom:18px}
a{color:var(--run);text-decoration:none}a:hover{text-decoration:underline}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;
padding:6px 14px 10px;margin-bottom:16px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;color:var(--dim);font-weight:600;padding:8px;
border-bottom:1px solid var(--line);font-size:12px;text-transform:uppercase;
letter-spacing:.4px}
td{padding:8px;border-bottom:1px solid #1d2128;vertical-align:middle}
tr:last-child td{border-bottom:none}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.chip{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;
font-weight:600}
.chip.running{background:#0d2d4d;color:var(--run)}
.chip.all,.chip.quorum{background:#0f2e18;color:var(--ok)}
.chip.deadline,.chip.cancelled{background:#3d2b0d;color:var(--accent)}
.mini{height:6px;width:120px;background:#20242b;border-radius:4px;overflow:hidden}
.mini>div{height:100%;float:left}
.mini .done{background:var(--ok)}.mini .fail{background:var(--fail)}
.dim{color:var(--dim)}
.stale{color:var(--accent)}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--ok);
margin-left:6px;animation:pulse 2s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
"""

_JS = Template("""<script>
// Refresh in place every second. No reload: this page is meant to be left open on a
// second monitor, and a reload would fight scrolling and flicker the table.
const $$ = (id) => document.getElementById(id);
const esc = (v) => String(v == null ? "" : v).replace(/[&<>"']/g, function (c) {
  return {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c];
});
let lastFetch = Date.now();
let lastSig = "";

function ago(iso) {
  if (!iso) return "\\u2014";
  const then = Date.parse(iso);
  if (isNaN(then)) return "\\u2014";
  const s = Math.max(0, Math.round((Date.now() - then) / 1000));
  if (s < 60) return s + "s ago";
  if (s < 3600) return Math.floor(s / 60) + "m ago";
  return Math.floor(s / 3600) + "h ago";
}

function stateOf(g) {
  if (g.completion_reason) return g.completion_reason;
  if (g.cancelled_utc) return "cancelled";
  return g.completed_utc ? "complete" : "running";
}

function rows(groups) {
  return groups.map(function (g) {
    const denom = Math.max(g.expected || 0, g.members || 0);
    const term = (g.finished || 0) + (g.failed || 0);
    const d = denom ? (100 * (g.finished || 0) / denom).toFixed(1) : 0;
    const f = denom ? (100 * (g.failed || 0) / denom).toFixed(1) : 0;
    const st = stateOf(g);
    // The age is per row, because "the list refreshed" and "this batch moved" are
    // different facts and only the second one tells you the batch is alive.
    const age = ago(g.last_activity_utc);
    const staleCls = (st === "running" && /[hm] ago/.test(age)) ? "stale" : "dim";
    return '<tr><td class="mono"><a href="/v0/groups/' + esc(g.groupid) + '">' +
      esc(g.label || g.groupid) + "</a><br><span class=\\"dim mono\\">" +
      esc(g.groupid) + "</span></td>" +
      '<td><span class="chip ' + esc(st) + '">' + esc(st) + "</span></td>" +
      "<td>" + term + " / " + (denom || "?") +
      (g.failed ? ' <span class="stale">(' + g.failed + " failed)</span>" : "") +
      '</td><td><div class="mini"><div class="done" style="width:' + d +
      '%"></div><div class="fail" style="width:' + f + '%"></div></div></td>' +
      '<td class="' + staleCls + '">' + esc(age) + "</td></tr>";
  }).join("");
}

async function tick() {
  try {
    const r = await fetch("/v0/groups", {headers: {"Accept": "application/json"},
                                        cache: "no-store"});
    if (r.ok) {
      const body = await r.json();
      lastFetch = Date.now();
      const sig = JSON.stringify(body.groups);
      if (sig !== lastSig) {
        lastSig = sig;
        const html = rows(body.groups || []);
        $$("rows").innerHTML = html ||
          '<tr><td colspan="5" class="dim">no groups yet</td></tr>';
      }
    }
  } catch (e) { /* next tick retries */ }
  setTimeout(tick, 1000);
}

// Re-render once a second regardless of fetches, so the per-row ages keep counting up.
setInterval(function () {
  const el = $$("fetched");
  if (el) {
    const s = Math.round((Date.now() - lastFetch) / 1000);
    el.textContent = s <= 1 ? "just now" : s + "s ago";
  }
  if (lastSig) { try { $$("rows").innerHTML = rows(JSON.parse(lastSig)); } catch (e) {} }
}, 1000);

setTimeout(tick, 1000);
</script>""")


def render(groups: list[dict[str, Any]]) -> str:
    def esc(v: Any) -> str:
        return html.escape("" if v is None else str(v))

    body_rows = []
    for g in groups:
        denom = max(g.get("expected") or 0, g.get("members") or 0)
        term = (g.get("finished") or 0) + (g.get("failed") or 0)
        state = (g.get("completion_reason") or
                 ("cancelled" if g.get("cancelled_utc") else
                  ("complete" if g.get("completed_utc") else "running")))
        body_rows.append(
            f'<tr><td class="mono"><a href="/v0/groups/{esc(g["groupid"])}">'
            f'{esc(g.get("label") or g["groupid"])}</a><br>'
            f'<span class="dim mono">{esc(g["groupid"])}</span></td>'
            f'<td><span class="chip {esc(state)}">{esc(state)}</span></td>'
            f'<td>{term} / {denom or "?"}</td>'
            f'<td><div class="mini"></div></td>'
            f'<td class="dim" data-ts="{esc(g.get("last_activity_utc"))}">—</td></tr>')
    rows_html = "".join(body_rows) or '<tr><td colspan="5" class="dim">no groups yet</td></tr>'

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>agent groups</title><style>{_CSS}</style></head>
<body><div class="wrap">
<h1>agent groups</h1>
<div class="sub">fetched <span id="fetched">just now</span><span class="dot"></span>
 · updates every second in place</div>
<div class="card"><table><thead><tr>
  <th>group</th><th>state</th><th>done</th><th>progress</th><th>last activity</th>
</tr></thead><tbody id="rows">{rows_html}</tbody></table></div>
</div>
{_JS.substitute()}
</body></html>"""
