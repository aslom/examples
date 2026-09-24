"""Mock mode is auto-selected when no Anthropic credential is present.

Explicit ER_MOCK_CLAUDE always wins over the auto-detection so a host with
`claude login` credentials (no env var) can still force real mode.
"""
import pytest

from eventrunner.config import API_KEY_VARS, has_api_credentials, load


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    """Start every case from a known-empty credential + mode environment."""
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.delenv("ER_MOCK_CLAUDE", raising=False)
    for k in API_KEY_VARS:
        monkeypatch.delenv(k, raising=False)


def test_no_credentials_defaults_to_mock():
    cfg = load()
    assert cfg.mock_claude is True
    assert "auto" in cfg.mock_reason
    for k in API_KEY_VARS:
        assert k in cfg.mock_reason, "reason should name the vars it looked for"


@pytest.mark.parametrize("var", API_KEY_VARS)
def test_any_single_credential_selects_real_claude(monkeypatch, var):
    monkeypatch.setenv(var, "sk-ant-whatever")
    cfg = load()
    assert cfg.mock_claude is False
    assert cfg.mock_reason == "auto: API credential present"


def test_blank_credential_counts_as_absent(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "   ")
    cfg = load()
    assert cfg.mock_claude is True, "whitespace-only key must not enable real mode"


def test_explicit_false_overrides_missing_credentials():
    # The `claude login` case: authenticated, but nothing in the environment.
    import os
    os.environ["ER_MOCK_CLAUDE"] = "false"
    try:
        cfg = load()
    finally:
        del os.environ["ER_MOCK_CLAUDE"]
    assert cfg.mock_claude is False
    assert cfg.mock_reason == "explicit ER_MOCK_CLAUDE=false"


def test_explicit_true_overrides_present_credentials(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-real-key")
    monkeypatch.setenv("ER_MOCK_CLAUDE", "true")
    cfg = load()
    assert cfg.mock_claude is True
    assert cfg.mock_reason == "explicit ER_MOCK_CLAUDE=true"


def test_blank_mode_var_falls_back_to_autodetect(monkeypatch):
    monkeypatch.setenv("ER_MOCK_CLAUDE", "  ")
    cfg = load()
    assert cfg.mock_claude is True
    assert cfg.mock_reason.startswith("auto:")


def test_has_api_credentials_accepts_an_explicit_mapping():
    assert has_api_credentials({"ANTHROPIC_API_KEY": "k"}) is True
    assert has_api_credentials({"ANTHROPIC_AUTH_TOKEN": "t"}) is True
    assert has_api_credentials({"ANTHROPIC_API_KEY": ""}) is False
    assert has_api_credentials({}) is False
    assert has_api_credentials({"ANTHROPIC_BASE_URL": "https://gw"}) is False
