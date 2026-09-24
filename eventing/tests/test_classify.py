"""EventRunner.classify — extracts human text + role + stats from claude
stream-json frames, keeps raw for debugging, promotes type=result to final."""
import io
import json
import pathlib
import threading

from eventrunner.config import Cfg
from eventrunner.runner import _pump, classify, is_hook_frame


def test_assistant_frame_extracts_text():
    frame = {
        "type": "assistant",
        "message": {"role": "assistant", "content": [
            {"type": "text", "text": "Hi Alek! 👋 What can I help you with today?"}
        ]},
        "session_id": "abc",
    }
    d = classify(frame)
    assert d["role"] == "assistant"
    assert d["text"] == "Hi Alek! 👋 What can I help you with today?"
    assert d["type"] == "assistant"
    assert d["raw"] == frame


def test_assistant_frame_concatenates_multiple_text_blocks():
    frame = {"type": "assistant",
             "message": {"content": [
                 {"type": "text", "text": "part one"},
                 {"type": "text", "text": "part two"},
             ]}}
    assert classify(frame)["text"] == "part one\npart two"


def test_assistant_frame_notes_tool_use_blocks():
    frame = {"type": "assistant",
             "message": {"content": [
                 {"type": "text", "text": "let me search"},
                 {"type": "tool_use", "name": "WebSearch", "id": "abc"},
             ]}}
    d = classify(frame)
    assert "let me search" in d["text"]
    assert "[tool_use:WebSearch]" in d["text"]


def test_result_frame_becomes_role_final_with_stats():
    frame = {
        "type": "result", "subtype": "success",
        "result": "Hi Alek! 👋 What can I help you with today?",
        "num_turns": 1, "duration_ms": 2881, "duration_api_ms": 2789,
        "total_cost_usd": 0.189105, "stop_reason": "end_turn", "is_error": False,
        "usage": {"input_tokens": 3091, "output_tokens": 21,
                  "cache_creation_input_tokens": 27700, "cache_read_input_tokens": 0,
                  "extraneous_field": "dropped"},
    }
    d = classify(frame)
    assert d["role"] == "final"
    assert d["text"] == frame["result"]
    assert d["stats"]["num_turns"] == 1
    assert d["stats"]["duration_ms"] == 2881
    assert d["stats"]["total_cost_usd"] == 0.189105
    assert d["stats"]["usage"]["input_tokens"] == 3091
    assert "extraneous_field" not in d["stats"]["usage"], "unrelated usage keys must be dropped"


def test_system_init_frame_is_summarized():
    frame = {"type": "system", "subtype": "init",
             "model": "claude-opus-5[1m]", "session_id": "5363385c-3568-…",
             "tools": ["Bash", "Read", "Write", "Edit"]}
    d = classify(frame)
    assert d["role"] == "system"
    assert d["subtype"] == "init"
    assert "claude-opus-5" in d["text"]
    assert "tools=4" in d["text"]
    # The huge init frame's fields are NOT flattened onto data — they live in raw
    assert "tools" not in d
    assert d["raw"] == frame


def test_system_hook_frame_is_summarized():
    frame = {"type": "system", "subtype": "hook_started",
             "hook_name": "SessionStart:startup", "hook_id": "x"}
    d = classify(frame)
    assert d["role"] == "system"
    assert d["subtype"] == "hook_started"
    assert "SessionStart:startup" in d["text"]


def test_is_hook_frame_flags_hooks_only():
    assert is_hook_frame({"type": "system", "subtype": "hook_started"})
    assert is_hook_frame({"type": "system", "subtype": "hook_response"})
    assert not is_hook_frame({"type": "system", "subtype": "init"})
    assert not is_hook_frame({"type": "assistant"})
    assert not is_hook_frame(None)


def test_include_raw_false_omits_raw_field():
    frame = {"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}}
    d = classify(frame, include_raw=False)
    assert d["text"] == "hi"
    assert "raw" not in d


def test_non_dict_frame_becomes_raw_role():
    d = classify("not-json-but-still-a-line")
    assert d["role"] == "raw"
    assert d["text"] == "not-json-but-still-a-line"


def test_unknown_frame_type_kept_as_raw():
    frame = {"type": "something_new_2027", "payload": {"foo": 1}}
    d = classify(frame)
    assert d["role"] == "raw"
    assert d["type"] == "something_new_2027"
    assert d["text"] is None
    assert d["raw"] == frame


def test_dark_moose_regression_full_sequence():
    """Replays the exact frames from the dark-moose-3985 session, ensures
    every one classifies cleanly and the assistant text lands in `text`."""
    frames = [
        {"type": "system", "subtype": "hook_started", "hook_name": "SessionStart:startup"},
        {"type": "system", "subtype": "hook_response", "hook_name": "SessionStart:startup",
         "output": "", "exit_code": 0},
        {"type": "system", "subtype": "init", "model": "claude-opus-5[1m]",
         "session_id": "5363385c", "tools": ["a", "b", "c"]},
        {"type": "assistant", "message": {
            "content": [{"type": "text", "text": "Hi Alek! 👋 What can I help you with today?"}]
        }},
        {"type": "result", "subtype": "success",
         "result": "Hi Alek! 👋 What can I help you with today?",
         "num_turns": 1, "duration_ms": 2881, "total_cost_usd": 0.189105,
         "usage": {"input_tokens": 3091, "output_tokens": 21}},
    ]
    classified = [classify(f) for f in frames]
    roles = [c["role"] for c in classified]
    assert roles == ["system", "system", "system", "assistant", "final"]

    # The assistant text and the final text should be identical — that's what
    # will trigger the HTML view's "↑ echoes the previous assistant" note.
    assert classified[3]["text"] == classified[4]["text"]

    # The verbose init frame reduced to one short summary line
    assert len(classified[2]["text"]) < 100


class _CaptureEmitter:
    """Enough of an Emitter to record every emit() call."""
    def __init__(self):
        self.calls: list[dict] = []
        self._n = 0
    def next_seq(self, _corr, start=None):
        self._n += 1
        return self._n
    def seed_seq(self, *_a, **_kw): pass
    def emit(self, **kw):
        self.calls.append(kw)
    def close(self): pass


def _run_pump(lines: list[str], *, cfg: Cfg, tmp: pathlib.Path,
              last_assistant: dict | None = None):
    stream = io.StringIO("\n".join(lines) + "\n")
    saw = threading.Event()
    emitter = _CaptureEmitter()
    log = tmp / "stdout.jsonl"
    _pump(stream, "test-otter-0001", "sess", "stdout", emitter, log, cfg, saw, last_assistant)
    return emitter.calls, saw


def _default_cfg(**overrides) -> Cfg:
    c = Cfg()
    c.tmpdir = "/tmp/fake"
    for k, v in overrides.items():
        setattr(c, k, v)
    return c


def test_pump_dedupes_final_text_that_echoes_assistant(tmp_path: pathlib.Path):
    """The dark-moose regression: final-frame text = assistant text → strip it."""
    reply = "Hi Alek! 👋 What can I help you with today?"
    frames = [
        json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": reply}]}}),
        json.dumps({"type": "result", "subtype": "success", "result": reply,
                    "num_turns": 1, "duration_ms": 100, "usage": {"input_tokens": 1, "output_tokens": 2}}),
    ]
    cfg = _default_cfg(dedupe_final_text=True, emit_system_hooks=False)
    calls, saw = _run_pump(frames, cfg=cfg, tmp=tmp_path, last_assistant={"text": None})

    assert saw.is_set(), "should have seen claude's type=result frame"
    assert len(calls) == 2

    assistant = calls[0]["data"]
    final = calls[1]["data"]
    assert assistant["role"] == "assistant" and assistant["text"] == reply
    assert final["role"] == "final"
    assert final["text"] is None, "duplicate text should be stripped from final event"
    assert final.get("text_echoes_prior_assistant") is True
    assert final["stats"]["num_turns"] == 1        # stats still there
    # And the terminal event is phase=result final=true
    assert calls[1]["phase"] == "result" and calls[1]["final"] is True


def test_pump_keeps_text_when_dedupe_disabled(tmp_path: pathlib.Path):
    reply = "Hello."
    frames = [
        json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": reply}]}}),
        json.dumps({"type": "result", "subtype": "success", "result": reply, "num_turns": 1, "duration_ms": 50}),
    ]
    cfg = _default_cfg(dedupe_final_text=False)
    calls, _saw = _run_pump(frames, cfg=cfg, tmp=tmp_path, last_assistant={"text": None})
    assert calls[1]["data"]["text"] == reply, "with dedupe off, text stays"
    assert not calls[1]["data"].get("text_echoes_prior_assistant")


def test_pump_keeps_different_final_text(tmp_path: pathlib.Path):
    """When the final's text is genuinely different from the assistant, keep both."""
    frames = [
        json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "half the answer"}]}}),
        json.dumps({"type": "result", "subtype": "success", "result": "SUMMARY", "num_turns": 1, "duration_ms": 50}),
    ]
    cfg = _default_cfg(dedupe_final_text=True)
    calls, _ = _run_pump(frames, cfg=cfg, tmp=tmp_path, last_assistant={"text": None})
    assert calls[1]["data"]["text"] == "SUMMARY", "different final text must be preserved"


def test_pump_filters_hook_frames_by_default(tmp_path: pathlib.Path):
    frames = [
        json.dumps({"type": "system", "subtype": "hook_started", "hook_name": "SessionStart:startup"}),
        json.dumps({"type": "system", "subtype": "hook_response", "hook_name": "SessionStart:startup"}),
        json.dumps({"type": "system", "subtype": "init", "model": "claude-opus-5", "tools": []}),
        json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}}),
        json.dumps({"type": "result", "subtype": "success", "result": "hi", "num_turns": 1}),
    ]
    cfg = _default_cfg()  # emit_system_hooks=False by default
    calls, _ = _run_pump(frames, cfg=cfg, tmp=tmp_path, last_assistant={"text": None})
    kinds = [c["data"]["role"] for c in calls]
    assert kinds == ["system", "assistant", "final"], f"hook frames leaked: {kinds}"


def test_pump_keeps_hooks_when_env_enabled(tmp_path: pathlib.Path):
    frames = [
        json.dumps({"type": "system", "subtype": "hook_started", "hook_name": "SessionStart:startup"}),
        json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}}),
    ]
    cfg = _default_cfg(emit_system_hooks=True)
    calls, _ = _run_pump(frames, cfg=cfg, tmp=tmp_path, last_assistant={"text": None})
    assert [c["data"]["role"] for c in calls] == ["system", "assistant"]
