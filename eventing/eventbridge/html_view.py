"""Server-rendered HTML history view — turn-grouped conversation.

A single correlationid can carry many turns (one initial /agents plus N
/continue calls). The view groups response events by turn boundary (each
`system/init` frame marks a fresh `claude -p` invocation), pairs each
group with the user prompt from `prompts.sqlite`, and renders a chat-like
`USER: … / ASSISTANT: …` layout — everything else (init/echoed-final/
tool_use/raw stream-json) collapses behind a per-turn "Details" toggle.
"""
from __future__ import annotations

import html
import json
from typing import Any


def _card(e: dict[str, Any], prev_assistant_text: str | None = None) -> str:
    phase = e.get("phase", "")
    seq   = e.get("sequence", 0)
    t     = e.get("time", "")
    data  = e.get("data") if isinstance(e.get("data"), dict) else {"raw": e.get("data")}

    role   = (data.get("role") or "") if isinstance(data, dict) else ""
    text   = data.get("text") if isinstance(data, dict) else None
    stats  = data.get("stats") if isinstance(data, dict) else None
    raw    = data.get("raw") if isinstance(data, dict) else None
    echo_flag = bool(data.get("text_echoes_prior_assistant")) if isinstance(data, dict) else False

    css_role = html.escape(role or phase)
    header = (f'<header><span><span class="role">{html.escape(role or phase)}</span>'
              f'#{seq}</span><span>{html.escape(t)}</span></header>')

    parts = [header]

    # Duplicate-text notice — claude's own `type=result` frame echoes the
    # last assistant message; label it so users don't wonder about the repetition.
    # Two ways this shows up:
    #  1. EventRunner already stripped `text` and set text_echoes_prior_assistant=true
    #     (default: ER_DEDUPE_FINAL_TEXT=true).
    #  2. Dedupe was disabled and both cards carry the same text — flag it here.
    echoed = echo_flag or (
        role == "final" and text and prev_assistant_text
        and text.strip() == prev_assistant_text.strip()
    )
    css_extra = " echoed" if echoed else ""
    if echoed:
        parts.append('<div class="note">↑ echoes the previous assistant message; this card is claude\'s own final-result frame with usage stats.</div>')

    if text is not None:
        parts.append(f'<pre class="text">{html.escape(str(text))}</pre>')

    if stats:
        parts.append(f'<div class="stats">{html.escape(_stats_line(stats))}</div>')

    if raw is not None:
        parts.append(
            "<details><summary>raw stream-json</summary>"
            f'<pre>{html.escape(json.dumps(raw, indent=2, ensure_ascii=False))}</pre>'
            "</details>"
        )
    elif text is None and stats is None:
        # Fall back to dumping whatever's in `data`
        parts.append(f'<pre>{html.escape(json.dumps(data, indent=2, ensure_ascii=False))}</pre>')

    return f'<div class="card {css_role}{css_extra}">{"".join(parts)}</div>'


def _stats_line(stats: dict[str, Any]) -> str:
    """Compact one-line summary of the numbers in a result-frame's stats."""
    bits: list[str] = []
    for k in ("num_turns", "duration_ms", "duration_api_ms"):
        v = stats.get(k)
        if v is not None: bits.append(f"{k}={v}")
    if (c := stats.get("total_cost_usd")) is not None:
        bits.append(f"cost=${c:.4f}")
    u = stats.get("usage") or {}
    if u:
        u_bits = [f"{k}={v}" for k, v in u.items() if v is not None]
        if u_bits: bits.append("usage(" + ",".join(u_bits) + ")")
    if (sr := stats.get("stop_reason")) is not None: bits.append(f"stop={sr}")
    return " · ".join(bits)


def group_by_turns(events: list[dict[str, Any]], prompts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group response events into turns and pair each with its user prompt.

    A turn *ends* on any `final=true` event; the next event opens a new turn.
    This heuristic is robust across real claude (which emits `type=result`
    per invocation) and the mock runner (which emits the same terminal
    shape). `system/init` frames are informational — they don't start turns.
    Prompts are paired positionally: turn 1 with prompts[0], etc.
    """
    turns: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def _new(started_time: str | None):
        return {
            "events": [], "started": started_time, "finished": None,
            "assistant_text": None, "stats": None,
        }

    for e in events:
        d = e.get("data") if isinstance(e.get("data"), dict) else {}
        role = d.get("role") if isinstance(d, dict) else None

        if current is None:
            current = _new(e.get("time"))
        current["events"].append(e)

        if role == "assistant" and d.get("text"):
            current["assistant_text"] = d["text"]

        if e.get("final"):
            current["finished"] = e.get("time")
            if d.get("stats"):
                current["stats"] = d["stats"]
            # If the final event's text differs from the assistant text (e.g.
            # dedupe was disabled and result frame carries its own summary),
            # prefer the assistant text for the chat preview; otherwise the
            # final text is fine too.
            if current["assistant_text"] is None and d.get("text"):
                current["assistant_text"] = d["text"]
            turns.append(current)
            current = None

    if current is not None:
        turns.append(current)

    # If we have more recorded prompts than event-groups (e.g. events were
    # never captured or the responses topic is empty on this EB), pad turns
    # with prompt-only placeholders so every recorded prompt still shows up
    # as a chat turn.
    while len(turns) < len(prompts):
        turns.append({"events": [], "started": None, "finished": None,
                      "assistant_text": None, "stats": None})

    # Pair prompts by position (turn_index is 1-based, list is 0-indexed)
    for i, t in enumerate(turns):
        p = prompts[i] if i < len(prompts) else None
        t["turn_index"] = i + 1
        t["prompt"] = p["prompt"] if p else None
        t["mode"] = (p["mode"] if p else None) or ("start" if i == 0 else "continue")
        t["submitted_utc"] = p["submitted_utc"] if p else None

    return turns


PAGE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>agent {corr}</title>
<style>
 body {{ font: 14px -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; margin: 1.5em; background:#fafafa; color:#111; }}
 h1 {{ font-size: 1.25rem; margin: 0 0 .3em; }}
 .meta {{ color:#555; font-size:.85rem; margin-bottom: 1em; }}
 .meta code {{ background:#eee; padding: 1px 4px; border-radius: 3px; }}
 .status {{ display:inline-block; padding: .1em .5em; border-radius: 3px; font-size:.8em; }}
 .status.running {{ background:#ffe; color:#960; }}
 .status.done {{ background:#efe; color:#060; }}
 .turn {{ border:1px solid #ddd; border-radius:6px; background:#fff; margin:.7em 0; box-shadow: 0 1px 2px rgba(0,0,0,.03); }}
 .turn > summary {{ list-style:none; cursor:pointer; padding:.6em .8em; user-select:none; display:flex; justify-content:space-between; gap:1em; align-items:baseline; }}
 .turn > summary::-webkit-details-marker {{ display:none; }}
 .turn > summary::before {{ content:"▸"; display:inline-block; margin-right:.5em; color:#888; transition:transform .1s; }}
 .turn[open] > summary::before {{ content:"▾"; }}
 .turn > summary .idx {{ font-weight:600; color:#333; }}
 .turn > summary .mode {{ color:#888; font-size:.85em; margin-left:.3em; }}
 .turn > summary .preview {{ color:#666; font-style:italic; flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; margin:0 1em; }}
 .turn > summary .stats {{ color:#888; font-size:.8em; white-space:nowrap; }}
 .turn > .body {{ padding:0 .8em .8em; border-top:1px solid #eee; }}
 .msg {{ margin:.6em 0; }}
 .msg .who {{ display:inline-block; min-width:5.5em; margin-right:.5em; font-weight:600; color:#666; font-size:.8em; text-transform:uppercase; letter-spacing:.05em; vertical-align:top; }}
 .msg .who.user {{ color:#369; }}
 .msg .who.assistant {{ color:#282; }}
 .msg .body-txt {{ display:inline-block; max-width:calc(100% - 6.5em); white-space:pre-wrap; word-break:break-word; font-family:inherit; }}
 /* On narrow (phone) viewports, stack the USER/ASSISTANT label above the text so nothing collides or shrinks. */
 @media (max-width: 640px) {{
   body {{ margin: 1em .8em; font-size: 15px; }}
   .msg .who {{ display:block; min-width:0; margin:0 0 .1em; font-size:.75em; }}
   .msg .body-txt {{ display:block; max-width:100%; margin-left:0; }}
   .turn > summary {{ flex-wrap:wrap; gap:.3em; }}
   .turn > summary .preview {{ flex-basis:100%; margin:0; order:3; }}
   .card pre, .card details pre {{ font-size:.85em; }}
 }}
 .details-toggle {{ margin-top:.5em; font-size:.85em; }}
 .details-toggle > summary {{ cursor:pointer; color:#888; user-select:none; }}
 .details-toggle > summary:hover {{ color:#333; }}
 .card {{ border-left:3px solid #ccc; background:#fafafa; padding:.4em .6em; margin:.4em 0; border-radius:3px; font-size:.9em; }}
 .card.system {{ border-color:#bbb; color:#555; }}
 .card.assistant {{ border-color:#282; }}
 .card.final {{ border-color:#282; }}
 .card.stderr {{ border-color:#c90; background:#fffaee; }}
 .card.error {{ border-color:#c33; background:#fff2f2; }}
 .card.echoed {{ opacity:.6; }}
 .card header {{ display:flex; justify-content:space-between; font-size:.75em; color:#888; }}
 .card pre {{ margin:.3em 0 0; white-space:pre-wrap; word-break:break-word; font-family:ui-monospace,SFMono-Regular,Menlo,monospace; font-size:.9em; }}
 .card .note {{ font-size:.8em; color:#888; font-style:italic; }}
 .card .stats {{ font-size:.85em; color:#555; margin-top:.2em; }}
 .card details {{ margin-top:.3em; }}
 .card details summary {{ cursor:pointer; color:#888; font-size:.8em; user-select:none; }}
 .card details pre {{ font-size:.85em; background:#fff; padding:.3em; border:1px solid #eee; border-radius:2px; }}
 form {{ margin-top:1.5em; }}
 textarea {{ width:100%; min-height:6em; font-family:inherit; padding:.5em; box-sizing:border-box; }}
 button {{ padding:.5em 1em; font-size:1em; }}
 #live:empty {{ display:none; }}
 #live {{ border-top:1px dashed #ccc; margin-top:1em; padding-top:.5em; }}
 #live > .live-label {{ font-size:.8em; color:#888; }}
 #refresh-banner {{ display:none; position:sticky; top:0; margin:.4em 0; padding:.4em .7em;
                    background:#fffbe6; border:1px solid #e6d67a; border-radius:4px; font-size:.9em; z-index:5; }}
 #refresh-banner a {{ margin-left:.6em; color:#960; text-decoration:underline; cursor:pointer; }}
 /* Sticky compose bar — always reachable no matter how long the history grows. */
 form.compose {{ position:sticky; bottom:0; margin-top:1.5em; padding:.6em; background:#fafafa;
                 border-top:1px solid #ddd; box-shadow:0 -2px 8px rgba(0,0,0,.05); z-index:4; }}
 form.compose textarea {{ min-height:4em; }}
 form.compose .row {{ display:flex; gap:.6em; align-items:flex-end; margin-top:.3em; }}
 form.compose .row .hint {{ flex:1; font-size:.75em; color:#888; }}
 .groupback {{ font-size:.85em; margin-bottom:.6em; }}
 .groupback a {{ font-weight:600; }}
</style></head><body>
{group_back}
<h1>agent <code>{corr}</code></h1>
<div class="meta">
  sessionuuid: <code>{sessionuuid}</code>
  · <span class="status {status_cls}">{status}</span>
  · {n_turns} turns · {n_events} events{cost_summary}
  · <a href="/v0/agents/{corr}/events.jsonl">raw CloudEvents</a>
  · <a href="/docs">API docs</a>
</div>
<div id="refresh-banner">Auto-regroup failed —
  <a onclick="location.reload()">refresh</a> to see the completed turn.
</div>
<div id="turns" data-last-seq="{last_seq}">
{turns}
</div>
<div id="live"><div class="live-label">↓ live updates (will regroup into turn on refresh)</div></div>
{compose_html}
<script>
(function() {{
  var CORR = {corr_json};
  var lastSeq = parseInt(document.getElementById("turns").dataset.lastSeq || "0", 10) || 0;
  var live = document.getElementById("live");
  var banner = document.getElementById("refresh-banner");
  var lastAssistantText = null;

  // --- draft persistence for the compose box: survives manual refreshes ---
  // Only wired when the compose form is present (?continue=1 opt-in).
  var draftKey = "eb.draft." + CORR;
  var ta = document.getElementById("compose-prompt");
  if (ta) {{
    try {{
      var saved = localStorage.getItem(draftKey);
      if (saved) ta.value = saved;
    }} catch (_) {{}}
    ta.addEventListener("input", function() {{
      try {{ localStorage.setItem(draftKey, ta.value); }} catch (_) {{}}
    }});
    ta.addEventListener("keydown", function(e) {{
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {{
        e.preventDefault();
        ta.form.requestSubmit();
      }}
    }});
    ta.form.addEventListener("submit", function() {{
      // form will POST → 303 → reload; clear the draft so the fresh page is empty
      try {{ localStorage.removeItem(draftKey); }} catch (_) {{}}
    }});
  }}

  function esc(s) {{ var d = document.createElement("div"); d.textContent = s == null ? "" : String(s); return d.innerHTML; }}
  function statsLine(stats) {{
    var bits = [];
    ["num_turns","duration_ms","duration_api_ms"].forEach(function(k) {{ if (stats[k] != null) bits.push(k+"="+stats[k]); }});
    if (stats.total_cost_usd != null) bits.push("cost=$"+Number(stats.total_cost_usd).toFixed(4));
    if (stats.usage) {{
      var ub = Object.keys(stats.usage).map(function(k) {{ return k+"="+stats.usage[k]; }});
      if (ub.length) bits.push("usage(" + ub.join(",") + ")");
    }}
    if (stats.stop_reason != null) bits.push("stop="+stats.stop_reason);
    return bits.join(" · ");
  }}

  // --- live updates via SSE ---
  // Pass `since=<last-seq-rendered-server-side>` so we only receive NEW events
  // and never replay the ones already on the page. This is what killed the
  // reload loop.
  var es = null;

  function openSse() {{
    try {{
      es = new EventSource("/v0/agents/" + encodeURIComponent(CORR) + "/events.sse?since=" + lastSeq);
    }} catch (_) {{ return; }}
    es.onmessage = onSseMessage;
    es.onerror = function() {{ /* ignore transient */ }};
  }}

  function onSseMessage(m) {{
    try {{
      var e = JSON.parse(m.data);
      if (typeof e.sequence === "number" && e.sequence <= lastSeq) return;   // defensive dedupe
      lastSeq = Math.max(lastSeq, e.sequence || 0);
      var d = e.data || {{}};
      var role = d.role || e.phase || "";
      var text = d.text;
      var stats = d.stats;
      var raw = d.raw;
      var echoed = !!d.text_echoes_prior_assistant || (role === "final" && text && lastAssistantText && text.trim() === lastAssistantText.trim());
      var el = document.createElement("div");
      el.className = "card " + esc(role) + (echoed ? " echoed" : "");
      var html = "<header><span>" + esc(role) + " #" + e.sequence + "</span><span>" + esc(e.time||"") + "</span></header>";
      if (echoed) html += "<div class='note'>↑ echoes the previous assistant message.</div>";
      if (text != null) html += "<pre>" + esc(text) + "</pre>";
      if (stats) html += "<div class='stats'>" + esc(statsLine(stats)) + "</div>";
      if (raw != null) html += "<details><summary>raw stream-json</summary><pre>" + esc(JSON.stringify(raw, null, 2)) + "</pre></details>";
      else if (text == null && !stats) html += "<pre>" + esc(JSON.stringify(d, null, 2)) + "</pre>";
      el.innerHTML = html;
      live.appendChild(el);
      if (role === "assistant" && text) lastAssistantText = text;
      if (e.final) {{
        // Auto-regroup in place — no page reload, so scroll position and any
        // in-progress compose text are preserved.
        try {{ es.close(); }} catch (_) {{}}
        regroupInPlace().then(function() {{
          openSse();          // ready for the next turn to arrive (e.g. from another tab)
        }}).catch(function(err) {{
          console.error("in-place regroup failed", err);
          banner.style.display = "block";
        }});
      }}
    }} catch (err) {{ console.error(err); }}
  }}

  async function regroupInPlace() {{
    var res = await fetch("/v0/agents/" + encodeURIComponent(CORR) + "/turns");
    if (!res.ok) throw new Error("HTTP " + res.status);
    var data = await res.json();
    if (!data.turns || !data.turns.length) return;
    var latest = data.turns[data.turns.length - 1];

    // Pull the accumulated live cards out of #live and into the new turn's Details.
    var liveCards = Array.from(live.querySelectorAll(".card"));
    var cardHtml = liveCards.map(function(c) {{ return c.outerHTML; }}).join("");
    liveCards.forEach(function(c) {{ c.remove(); }});

    var idx = latest.turn_index;
    var mode = latest.mode || "?";
    var prompt = latest.prompt != null ? latest.prompt : "(prompt not recorded)";
    var assistant = latest.assistant_text != null ? latest.assistant_text : "(no text)";
    var stats = latest.stats || {{}};
    var preview = (assistant || "").replace(/\\n/g, " ");
    if (preview.length > 120) preview = preview.slice(0, 120) + "…";
    var statsRow = statsLine(stats);
    var detailsCount = liveCards.length;

    var turnHtml =
      '<details class="turn" open>' +
        '<summary>' +
          '<span><span class="idx">Turn ' + idx + '</span><span class="mode">· ' + esc(mode) + '</span></span>' +
          '<span class="preview">' + esc(preview) + '</span>' +
          '<span class="stats">' + esc(statsRow) + '</span>' +
        '</summary>' +
        '<div class="body">' +
          '<div class="msg"><span class="who user">USER</span><span class="body-txt">' + esc(prompt) + '</span></div>' +
          '<div class="msg"><span class="who assistant">ASSISTANT</span><span class="body-txt">' + esc(assistant) + '</span></div>' +
          (detailsCount ? ('<details class="details-toggle"><summary>Details (' + detailsCount + ' events)</summary>' + cardHtml + '</details>') : '') +
        '</div>' +
      '</details>';

    // Collapse previously-open turns so the new one is the focused thread.
    document.querySelectorAll("#turns > details.turn[open]").forEach(function(d) {{ d.open = false; }});

    var turnsEl = document.getElementById("turns");
    turnsEl.insertAdjacentHTML("beforeend", turnHtml);
    turnsEl.dataset.lastSeq = String(lastSeq);
  }}

  openSse();
}})();
</script>
</body></html>
"""


def _turn_summary(t: dict[str, Any]) -> str:
    """The one-line preview shown when a turn's <details> is closed."""
    text = t.get("assistant_text") or ""
    text = text.replace("\n", " ").strip()
    if len(text) > 120:
        text = text[:120] + "…"
    bits: list[str] = []
    stats = t.get("stats") or {}
    if (dm := stats.get("duration_ms")) is not None:
        bits.append(f"{dm/1000:.1f}s")
    if (c := stats.get("total_cost_usd")) is not None:
        bits.append(f"${c:.4f}")
    return " · ".join(bits), text


def _turn_html(t: dict[str, Any], is_latest: bool) -> str:
    idx  = t["turn_index"]
    mode = t.get("mode") or "?"
    prompt = t.get("prompt")
    assistant = t.get("assistant_text")
    stats_line, preview = _turn_summary(t)

    body_parts: list[str] = []

    # 1) Chat surface — user prompt + assistant reply
    if prompt is not None:
        body_parts.append(
            f'<div class="msg"><span class="who user">USER</span>'
            f'<span class="body-txt">{html.escape(prompt)}</span></div>'
        )
    else:
        body_parts.append('<div class="msg"><span class="who user">USER</span>'
                          '<span class="body-txt" style="color:#999">(prompt not recorded)</span></div>')

    if assistant is not None:
        body_parts.append(
            f'<div class="msg"><span class="who assistant">ASSISTANT</span>'
            f'<span class="body-txt">{html.escape(assistant)}</span></div>'
        )
    elif t.get("finished"):
        body_parts.append('<div class="msg"><span class="who assistant">ASSISTANT</span>'
                          '<span class="body-txt" style="color:#999">(no text)</span></div>')
    else:
        body_parts.append('<div class="msg"><span class="who assistant">ASSISTANT</span>'
                          '<span class="body-txt" style="color:#999">…running</span></div>')

    # 2) Details toggle — the flat event list for this turn
    events = t.get("events") or []
    if events:
        cards: list[str] = []
        last_a_text: str | None = None
        for e in events:
            cards.append(_card(e, prev_assistant_text=last_a_text))
            d = e.get("data") if isinstance(e.get("data"), dict) else {}
            if d.get("role") == "assistant" and d.get("text"):
                last_a_text = d["text"]
        body_parts.append(
            f'<details class="details-toggle"><summary>Details ({len(events)} events)</summary>'
            + "".join(cards)
            + "</details>"
        )

    # Latest turn opens by default; earlier turns collapse to reduce scroll.
    open_attr = " open" if is_latest else ""
    summary = (
        f'<summary><span><span class="idx">Turn {idx}</span>'
        f'<span class="mode">· {html.escape(mode)}</span></span>'
        f'<span class="preview">{html.escape(preview)}</span>'
        f'<span class="stats">{html.escape(stats_line)}</span></summary>'
    )
    return f'<details class="turn"{open_attr}>{summary}<div class="body">{"".join(body_parts)}</div></details>'


_COMPOSE_FORM = '''<form class="compose" method="POST" action="/v0/agents/{corr}/continue-html">
  <textarea name="prompt" id="compose-prompt" placeholder="continue this conversation…" required></textarea>
  <div class="row">
    <div class="hint">Draft is auto-saved locally. Cmd/Ctrl+Enter to submit.</div>
    <button type="submit">Continue</button>
  </div>
</form>'''

_COMPOSE_HIDDEN = ""     # read-only mode: no compose form, no hint text


def render(corr: str, session: dict[str, Any] | None, events: list[dict[str, Any]],
           prompts: list[dict[str, Any]] | None = None,
           show_continue: bool = False,
           groupid: str | None = None, group_label: str | None = None) -> str:
    """Render one agent's history.

    `groupid`, when the agent belongs to a batch (§21), adds a link back to the group
    page. Without it a member reached from the group page is a dead end: the operator
    has to edit the URL to get back to the thing they were actually watching.
    """
    session = session or {}
    group_back = ""
    if groupid:
        _lbl = html.escape(group_label or groupid)
        group_back = (f'<div class="groupback">&#8592; back to group '
                      f'<a href="/v0/groups/{html.escape(groupid)}">{_lbl}</a>'
                      f' <code>{html.escape(groupid)}</code></div>')
    running = not any(e.get("final") for e in events)
    turns = group_by_turns(events, prompts or [])

    # Compute a small header cost/token summary from the last stats we saw
    total_cost = 0.0; total_in = 0; total_out = 0
    for t in turns:
        s = t.get("stats") or {}
        if isinstance(s.get("total_cost_usd"), (int, float)):
            total_cost += s["total_cost_usd"]
        u = s.get("usage") or {}
        if isinstance(u.get("input_tokens"), int):  total_in  += u["input_tokens"]
        if isinstance(u.get("output_tokens"), int): total_out += u["output_tokens"]
    bits: list[str] = []
    if total_cost > 0: bits.append(f" · ${total_cost:.4f}")
    if total_in or total_out: bits.append(f" · {total_in}→{total_out} tokens")
    cost_summary = "".join(bits)

    turns_html = "\n".join(_turn_html(t, is_latest=(i == len(turns) - 1))
                           for i, t in enumerate(turns))
    if not turns:
        turns_html = '<p style="color:#888">No events yet — the runner may still be spinning up.</p>'

    # Last sequence rendered server-side — SSE will only stream events past this.
    last_seq = max((e.get("sequence") or 0) for e in events) if events else 0

    compose_html = (_COMPOSE_FORM.format(corr=html.escape(corr))
                     if show_continue else _COMPOSE_HIDDEN)

    return PAGE.format(
        group_back=group_back,
        corr=html.escape(corr),
        corr_json=json.dumps(corr),
        sessionuuid=html.escape(session.get("sessionuuid", "?")),
        n_turns=len(turns),
        n_events=len(events),
        cost_summary=cost_summary,
        status="running" if running else "done",
        status_cls="running" if running else "done",
        turns=turns_html,
        last_seq=last_seq,
        compose_html=compose_html,
    )
