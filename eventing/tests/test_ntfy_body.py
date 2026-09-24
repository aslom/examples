"""ntfy message composition — the smartphone-friendly content.

We want the phone user to see everything useful in the message body itself
because the click / attach URLs point at localhost and won't resolve off
the developer's Mac.
"""
import pathlib

from eventbridge.ntfy import (
    compose_body, compose_title, _stats_line, NtfyPublisher,
)
from eventbridge.config import NtfyCfg
from eventbridge.store import Store


def _final_event(corr: str, text: str | None = None, stats: dict | None = None):
    return {
        "correlationid": corr, "phase": "result", "final": True,
        "sequence": 3, "time": "2026-09-21T22:00:00Z",
        "data": {
            "type": "result", "role": "final", "text": text,
            "stats": stats or {"num_turns": 1, "duration_ms": 6600,
                                "total_cost_usd": 0.047,
                                "usage": {"input_tokens": 812, "output_tokens": 24}},
        },
    }


def _error_event(corr: str, msg: str):
    return {
        "correlationid": corr, "phase": "error", "final": True,
        "sequence": 1, "time": "t",
        "data": {"role": "error", "text": msg},
    }


def test_compose_body_includes_prompt_assistant_stats_corr(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "warm-lemur-9426"
    s.insert_prompt(corr, "start", "Say hi to Alek")
    s.insert_response({
        "correlationid": corr, "sequence": 2, "phase": "stdout",
        "id": "x", "time": "t",
        "data": {"role": "assistant", "text": "Hi Alek! What can I help with?"},
        "final": "false",
    })
    ev = _final_event(corr, text=None)   # dedupe stripped text — must fall back to store
    body = compose_body(ev, store=s)

    assert "Say hi to Alek" in body, "user prompt must appear in body"
    assert "Hi Alek! What can I help with?" in body, \
        "assistant reply must appear even when final.text is None (store fallback)"
    assert "6.6s"     in body    # duration
    assert "$0.0470"  in body    # cost
    assert "812→24"   in body    # token usage
    assert corr       in body


def test_compose_body_uses_data_text_when_present(tmp_path: pathlib.Path):
    """If the final event carries text (dedupe off), use it directly."""
    ev = _final_event("brave-otter-4718",
                      text="Look ma, no store lookup needed.")
    body = compose_body(ev, store=None)
    assert "Look ma, no store lookup needed." in body
    assert "brave-otter-4718" in body


def test_compose_body_prompt_and_text_are_truncated():
    long_prompt = "prompt " * 200          # 1400 chars
    long_reply  = "reply " * 1000          # 6000 chars
    ev = _final_event("truncate-test-0001", text=long_reply)

    class _FakeStore:
        def get_prompts(self, _): return [{"prompt": long_prompt, "mode": "start"}]
        def events_for(self, _): return []

    body = compose_body(ev, store=_FakeStore())
    assert len(body) < 4000, "body must fit under ntfy's 4KB cap"
    assert body.startswith("❓ prompt")
    assert "💬 reply" in body
    # Both truncated with ellipsis
    assert "…" in body


def test_compose_body_error_phase():
    ev = _error_event("bad-corr-0001", "claude exited with code 137")
    body = compose_body(ev)
    assert body.startswith("❌ claude exited with code 137")
    assert "bad-corr-0001" in body


def test_compose_title_success_includes_duration_and_cost():
    ev = _final_event("shiny-tiger-2095")
    t = compose_title(ev)
    assert "shiny-tiger-2095" in t
    assert "6.6s" in t
    assert "$0.0470" in t


def test_compose_title_error_marks_it():
    ev = _error_event("bad-corr-0001", "x")
    t = compose_title(ev)
    assert t.startswith("⚠") and "bad-corr-0001" in t and "error" in t


def test_stats_line_omits_zero_cost():
    line = _stats_line({"duration_ms": 100, "total_cost_usd": 0.0,
                        "usage": {"input_tokens": 1, "output_tokens": 2}})
    assert "0.0s" in line or "0.1s" in line
    assert "$" not in line, "zero cost should be omitted"


def test_stats_line_empty_when_no_data():
    assert _stats_line(None) == ""
    assert _stats_line({}) == ""


def _build_payload(public_base_url: str, event: dict) -> dict:
    """Build the ntfy JSON payload as _publish() would, without POSTing."""
    import json as _json
    cfg = NtfyCfg(enabled=True, topic="t", base_url="https://ntfy.sh")
    pub = NtfyPublisher(cfg, public_base_url, store=None)
    # Reuse the real publish flow but capture the JSON body before it goes out.
    captured: dict = {}
    class _FakeResp:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def read(self): return b""
    import urllib.request
    orig = urllib.request.urlopen
    def _fake(req, *a, **kw):
        captured["body"] = _json.loads(req.data)
        return _FakeResp()
    urllib.request.urlopen = _fake
    try:
        pub._publish(event)
    finally:
        urllib.request.urlopen = orig
    return captured["body"]


def test_payload_always_includes_click_and_actions_even_for_loopback():
    """The operator's EVENT_BRIDGE_PUBLIC_BASE_URL is used verbatim — no loopback filter."""
    ev = _final_event("brave-otter-4718", text="ok")
    for base in ("http://localhost:8080",
                 "http://127.0.0.1:8080",
                 "http://192.168.1.15:8080",
                 "https://demo.example.com"):
        p = _build_payload(base, ev)
        assert p["click"]  == f"{base}/v0/agents/brave-otter-4718",  f"base={base}"
        assert p["attach"] == f"{base}/v0/agents/brave-otter-4718/events.jsonl", f"base={base}"
        assert len(p["actions"]) == 3, f"base={base}"
        assert p["actions"][2]["action"] == "http", f"http-Continue action must exist for {base}"


def test_payload_message_carries_the_summary_verbatim():
    ev = _final_event("brave-otter-4718", text="Hi Alek! What can I help you with?")
    p = _build_payload("http://127.0.0.1:8080", ev)
    assert "Hi Alek! What can I help you with?" in p["message"]
    assert "brave-otter-4718" in p["message"]
    assert "6.6s" in p["message"] and "$0.0470" in p["message"]
