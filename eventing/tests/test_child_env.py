"""EventRunner forwards a curated env allowlist to the claude subprocess."""
import io
import os

import pytest

from eventrunner.runner import (_CLAUDE_ROUTING, child_env, log_forwarded_env,
                                _redact)


def test_only_allowlisted_vars_are_forwarded(monkeypatch):
    # Vars that should pass through
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setenv("HOME", "/tmp/home")
    monkeypatch.setenv("ANTHROPIC_BASE_URL", "https://proxy.example/api")
    monkeypatch.setenv("ANTHROPIC_MODEL", "claude-sonnet-5")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "sk-ant-something-secret")
    monkeypatch.setenv("CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS", "1")
    # A var that must NOT pass through
    monkeypatch.setenv("MY_SECRET_UNRELATED", "should-be-dropped")

    env = child_env()

    assert env["PATH"] == "/usr/bin:/bin"
    assert env["HOME"] == "/tmp/home"
    assert env["ANTHROPIC_BASE_URL"] == "https://proxy.example/api"
    assert env["ANTHROPIC_MODEL"] == "claude-sonnet-5"
    assert env["ANTHROPIC_AUTH_TOKEN"] == "sk-ant-something-secret"
    assert env["CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS"] == "1"

    assert "MY_SECRET_UNRELATED" not in env


def test_unset_vars_are_omitted(monkeypatch):
    monkeypatch.setenv("PATH", "/usr/bin")
    for k in _CLAUDE_ROUTING:
        monkeypatch.delenv(k, raising=False)

    env = child_env()
    assert "PATH" in env
    assert "ANTHROPIC_BASE_URL" not in env
    assert "ANTHROPIC_MODEL" not in env
    assert "ANTHROPIC_AUTH_TOKEN" not in env


def test_empty_string_treated_as_unset(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_MODEL", "")
    env = child_env()
    assert "ANTHROPIC_MODEL" not in env


def test_secret_values_redacted_in_log(monkeypatch, capsys):
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "sk-ant-supersecret-1234567890")
    monkeypatch.setenv("ANTHROPIC_BASE_URL",   "https://proxy.example")
    log_forwarded_env()
    out = capsys.readouterr().out
    assert "ANTHROPIC_BASE_URL=https://proxy.example" in out
    assert "sk-ant-supersecret-1234567890" not in out, "raw token must not leak into logs"
    assert "ANTHROPIC_AUTH_TOKEN=sk-a" in out           # first 4 chars only


def test_log_notes_when_nothing_is_set(monkeypatch, capsys):
    # Derived from the module's own allowlist rather than a hardcoded copy: Phase 1
    # added CLAUDE_CONFIG_DIR / CLAUDE_CODE_PROJECT_DIR_NAME to it (§16 Gap B),
    # and a hardcoded list here would silently stop clearing the environment.
    for k in _CLAUDE_ROUTING:
        monkeypatch.delenv(k, raising=False)
    log_forwarded_env()
    out = capsys.readouterr().out
    assert "no ANTHROPIC_*/CLAUDE_CODE_* env vars set" in out


def test_redact_helper():
    tok = "sk-ant-abcdefghijklmnop"
    assert _redact("ANTHROPIC_AUTH_TOKEN", tok) == f"sk-a…({len(tok)} chars)"
    assert _redact("ANTHROPIC_BASE_URL", "https://x") == "https://x"     # non-secret: passthrough
    assert _redact("ANTHROPIC_AUTH_TOKEN", "shortie") == "…(7 chars)"    # short secret: no head
