"""group_by_turns + prompts persistence — the turn-grouped chat view."""
import pathlib

import pytest

from eventbridge.html_view import group_by_turns, render
from eventbridge.store import Store


def _ev(seq, role, text=None, subtype=None, final=False, stats=None, time="t"):
    data = {"role": role}
    if text is not None: data["text"] = text
    if subtype is not None: data["subtype"] = subtype
    if stats: data["stats"] = stats
    return {"sequence": seq, "phase": "result" if final else "stdout",
            "time": time, "final": final, "data": data}


def test_two_turns_grouped_by_init_boundary():
    events = [
        _ev(1, "system",    subtype="init", time="t0"),
        _ev(2, "assistant", text="OK1"),
        _ev(3, "final",     text="OK1", final=True, stats={"duration_ms": 100, "total_cost_usd": 0.01}),
        _ev(4, "system",    subtype="init", time="t1"),
        _ev(5, "assistant", text="OK2"),
        _ev(6, "final",     text="OK2", final=True, stats={"duration_ms": 200, "total_cost_usd": 0.02}),
    ]
    prompts = [
        {"turn_index": 1, "mode": "start",    "prompt": "reply OK1", "submitted_utc": "t0"},
        {"turn_index": 2, "mode": "continue", "prompt": "reply OK2", "submitted_utc": "t1"},
    ]
    turns = group_by_turns(events, prompts)
    assert len(turns) == 2
    assert [t["turn_index"] for t in turns] == [1, 2]
    assert turns[0]["prompt"] == "reply OK1"
    assert turns[0]["assistant_text"] == "OK1"
    assert turns[0]["mode"] == "start"
    assert turns[0]["stats"]["duration_ms"] == 100
    assert turns[1]["prompt"] == "reply OK2"
    assert turns[1]["assistant_text"] == "OK2"
    assert turns[1]["mode"] == "continue"


def test_turn_ends_on_final_event_next_turn_starts_fresh():
    """Boundary = final event, not init. So init frames become just noise
    inside a turn."""
    events = [
        _ev(1, "system",    subtype="init"),
        _ev(2, "assistant", text="early"),
        _ev(3, "final",     text="early", final=True, stats={"duration_ms": 50}),
        _ev(4, "system",    subtype="init"),          # ← informational, not a boundary
        _ev(5, "assistant", text="later"),
        _ev(6, "final",     text="later", final=True, stats={"duration_ms": 80}),
    ]
    turns = group_by_turns(events, prompts=[])
    assert len(turns) == 2
    assert turns[0]["assistant_text"] == "early"
    assert turns[1]["assistant_text"] == "later"
    # Each turn owns exactly the events that were part of it
    assert len(turns[0]["events"]) == 3
    assert len(turns[1]["events"]) == 3


def test_events_before_first_final_all_belong_to_turn_one():
    events = [
        _ev(1, "system", subtype="init"),
        _ev(2, "assistant", text="hi"),
        # no final yet — turn is still in flight
    ]
    turns = group_by_turns(events, prompts=[])
    assert len(turns) == 1
    assert turns[0]["assistant_text"] == "hi"
    assert turns[0]["finished"] is None


def test_missing_prompts_are_tolerated():
    events = [
        _ev(1, "system",    subtype="init"),
        _ev(2, "assistant", text="hi"),
    ]
    turns = group_by_turns(events, prompts=[])
    assert turns[0]["prompt"] is None
    assert turns[0]["mode"] == "start"      # default when no prompt record


def test_render_produces_turn_details_and_hides_events_behind_toggle():
    events = [
        _ev(1, "system",    subtype="init", time="t0"),
        _ev(2, "assistant", text="Hi Alek!"),
        _ev(3, "final",     text="Hi Alek!", final=True, stats={"duration_ms": 800, "total_cost_usd": 0.03}),
    ]
    prompts = [{"turn_index": 1, "mode": "start", "prompt": "say hi", "submitted_utc": "t0"}]
    out = render("wake-otter-0001",
                 {"sessionuuid": "abc-uuid", "turns": 1, "created_utc": "t0", "updated_utc": "t0"},
                 events, prompts=prompts)
    assert "Turn 1" in out
    assert "USER" in out and "say hi" in out
    assert "ASSISTANT" in out and "Hi Alek!" in out
    # Details toggle is present but not expanded
    assert "Details (3 events)" in out
    # Header rollup shows the turn count + cost
    assert "1 turns" in out
    assert "$0.0300" in out


def test_prompts_table_insert_and_get(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "otter-0001"
    idx1 = s.insert_prompt(corr, "start", "hello")
    idx2 = s.insert_prompt(corr, "continue", "again")
    idx3 = s.insert_prompt(corr, "continue", "and again")
    assert (idx1, idx2, idx3) == (1, 2, 3)
    prompts = s.get_prompts(corr)
    assert len(prompts) == 3
    assert [p["prompt"] for p in prompts] == ["hello", "again", "and again"]
    assert [p["turn_index"] for p in prompts] == [1, 2, 3]
    assert [p["mode"] for p in prompts] == ["start", "continue", "continue"]


def test_backfill_prompt_if_missing_is_idempotent(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "warm-crab-6884"
    first = s.backfill_prompt_if_missing(corr, "start", "say hi to Alek", submitted_utc="t0")
    dup   = s.backfill_prompt_if_missing(corr, "start", "say hi to Alek", submitted_utc="t0")
    assert first == 1
    assert dup is None, "identical (corr, mode, prompt) must not be re-inserted"
    prompts = s.get_prompts(corr)
    assert len(prompts) == 1 and prompts[0]["prompt"] == "say hi to Alek"


def test_backfill_and_direct_insert_coexist(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "warm-crab-6884"
    # Kafka mirror sees the start request first
    idx1 = s.backfill_prompt_if_missing(corr, "start", "first")
    # Later, HTTP path posts a continue
    idx2 = s.insert_prompt(corr, "continue", "second")
    # Then the Kafka mirror sees that continue too (later in the log)
    idx3 = s.backfill_prompt_if_missing(corr, "continue", "second")
    assert idx1 == 1 and idx2 == 2 and idx3 is None
    assert [p["prompt"] for p in s.get_prompts(corr)] == ["first", "second"]


def test_prompts_scoped_per_correlationid(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    s.insert_prompt("A", "start", "hi-A")
    s.insert_prompt("B", "start", "hi-B")
    s.insert_prompt("A", "continue", "again-A")
    a = [p["prompt"] for p in s.get_prompts("A")]
    b = [p["prompt"] for p in s.get_prompts("B")]
    assert a == ["hi-A", "again-A"]
    assert b == ["hi-B"]


def test_render_shows_no_events_message_for_empty_correlation():
    out = render("wake-otter-9999", {"sessionuuid": "u"}, events=[], prompts=[])
    assert "No events yet" in out


def test_prompts_without_events_still_render_as_turns():
    """Back-filled prompts show up as turn cards even when responses.sqlite is
    empty — you see USER: <prompt> per turn, ASSISTANT: (no text)."""
    prompts = [
        {"turn_index": 1, "mode": "start",    "prompt": "Say hi Alek!", "submitted_utc": "t0"},
        {"turn_index": 2, "mode": "continue", "prompt": "Check weather", "submitted_utc": "t1"},
        {"turn_index": 3, "mode": "continue", "prompt": "Second turn", "submitted_utc": "t2"},
    ]
    turns = group_by_turns(events=[], prompts=prompts)
    assert len(turns) == 3
    assert [t["prompt"] for t in turns] == ["Say hi Alek!", "Check weather", "Second turn"]
    assert [t["mode"] for t in turns] == ["start", "continue", "continue"]
    # Render also produces HTML — no crash on the empty-events path
    out = render("warm-crab-6884", None, events=[], prompts=prompts)
    assert "Turn 1" in out and "Turn 3" in out
    assert "Say hi Alek!" in out and "Check weather" in out


def test_render_emits_last_seq_and_auto_regroups_in_place():
    """Regression: the SSE handler must (a) build the URL with `?since=<last
    server-rendered seq>` so on-load replay is zero events, (b) auto-regroup
    in place on final=true (no page reload), (c) fall back to a manual
    refresh banner only if the regroup fetch fails."""
    events = [
        _ev(1, "system",    subtype="init"),
        _ev(2, "assistant", text="hi"),
        _ev(3, "final",     text="hi", final=True, stats={"duration_ms": 50}),
    ]
    # show_continue=True to keep the compose-form assertions valid
    out = render("test-corr-0001", {"sessionuuid": "u"}, events, prompts=[],
                 show_continue=True)

    # The SSE URL is built with the server-rendered last_seq
    assert 'data-last-seq="3"' in out
    assert "/events.sse?since=" in out
    # Auto-regroup path exists
    assert "regroupInPlace" in out
    assert 'fetch("/v0/agents/' in out and '/turns"' in out
    # The old always-reload pattern is gone — location.reload() may appear
    # only inside the fallback banner's onclick handler, never inside the
    # SSE final handler.
    sse_section = out.split("onSseMessage")[1].split("regroupInPlace")[0]
    assert "location.reload" not in sse_section, \
        "final=true handler must not call location.reload directly"
    # The refresh banner exists as a fallback only
    assert "refresh-banner" in out
    # The compose form is sticky and has draft-persistence hooks (opt-in)
    assert 'class="compose"' in out
    assert 'localStorage' in out
    assert 'compose-prompt' in out


def test_render_last_seq_zero_when_no_events():
    out = render("test-corr-9998", {"sessionuuid": "u"}, events=[], prompts=[])
    assert 'data-last-seq="0"' in out


def test_compose_form_hidden_by_default():
    """The Continue textarea + button are hidden unless the URL says
    ?continue=1. Read-only viewers see NOTHING in that slot — no form,
    no hint text — so a shared read-only URL looks perfectly clean."""
    events = [_ev(1, "assistant", text="hi")]
    out = render("test-corr-0003", {"sessionuuid": "u"}, events,
                 prompts=[{"turn_index": 1, "mode": "start", "prompt": "hi",
                           "submitted_utc": "t"}],
                 show_continue=False)
    assert 'id="compose-prompt"' not in out
    assert 'class="compose"' not in out
    # No leftover hint text either
    assert "Enable continue" not in out
    assert 'href="?continue=1"' not in out
    assert "Read-only view" not in out


def test_compose_form_shown_when_show_continue_true():
    events = [_ev(1, "assistant", text="hi")]
    out = render("test-corr-0004", {"sessionuuid": "u"}, events,
                 prompts=[{"turn_index": 1, "mode": "start", "prompt": "hi",
                           "submitted_utc": "t"}],
                 show_continue=True)
    assert 'id="compose-prompt"' in out
    assert 'class="compose"' in out


def test_render_has_mobile_viewport_and_narrow_stacking():
    """Regression: without a viewport meta, mobile browsers render the page
    at ~980px and scale it down, making 14px text look tiny. Also the
    USER/ASSISTANT label must have a right margin so it doesn't collide
    with the body text on narrow screens."""
    out = render("test-corr-0002", {"sessionuuid": "u"}, events=[
        _ev(1, "assistant", text="hi"),
    ], prompts=[{"turn_index": 1, "mode": "start", "prompt": "say hi",
                 "submitted_utc": "t"}])
    # Viewport meta is present with device-width
    assert '<meta name="viewport"' in out
    assert 'width=device-width' in out
    # Label gets a margin-right so it doesn't touch the body text
    assert 'margin-right:.5em' in out or 'margin-right: .5em' in out
    # Narrow-screen media query stacks USER / body-txt vertically
    assert '@media (max-width: 640px)' in out
    # The .msg .body-txt line uses max-width:calc(100% - 6.5em) so label + gap
    # still fits on a 320px screen at 15px font
    assert '.msg .body-txt' in out
