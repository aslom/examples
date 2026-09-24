"""Skill CLI output shape — regression tests for the terminal-clickable URL bug.

Terminals auto-linkify URLs and will include a trailing `)` if the URL is
wrapped in parens. The `run` command must therefore print URLs without any
wrapping punctuation so a click opens the correct address.
"""
import os
import pathlib
import re
import shlex
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
# The skill lives IN the repository at eventing/skills/eventbridge. Claude Code sees
# it through a symlink from the user's skills directory, so there is exactly one copy
# to edit and it is version-controlled with the code it drives. These three tests
# failed for months only because the skill used to live outside the repo entirely.
SKILL_DIR = ROOT / "skills" / "eventbridge"
CLI = SKILL_DIR / "eventbridge-cli.py"
SKILL = SKILL_DIR / "SKILL.md"


def test_cli_never_prints_url_wrapped_in_parens():
    src = CLI.read_text()
    # Any print statement that produces `(<space>?http…)` is the terminal-
    # linkify trap that made "click the URL" go to /agents/xxxx) (404).
    assert not re.search(r'print[^"\']*\(\s*http', src), \
        "CLI must not print '(URL' — terminal auto-link includes the trailing ')'"
    assert not re.search(r'\(\{BASE\}', src), \
        "CLI must not wrap {BASE} URL in parens for the same reason"


def test_cli_run_line_uses_middle_dot_separator():
    """Prints correlationid and URL separated by ' · ' — no wrapping punct."""
    src = CLI.read_text()
    # The run handler prints one line with the URL at the end
    assert '▸ agent {corr} · {BASE}/v0/agents/{corr}' in src


def test_skill_md_sample_output_has_no_url_in_parens():
    """SKILL.md sample output should model the same click-safe format."""
    md = SKILL.read_text()
    assert not re.search(r'\(https?://', md), \
        "SKILL.md must not show '(URL)' in its sample output"


# ---- the CLI echoes the exact command it runs --------------------------------
#
# An agent driving this CLI narrates in prose, and prose is not reproducible: the
# reader cannot tell `--count 100` from `--count 10`, nor see which endpoint was
# actually used. Echoing argv makes every transcript replayable by hand.


def _run(args, env_extra=None):
    env = dict(os.environ, EVENTBRIDGE_ECHO="1")
    env.pop("EVENTBRIDGE_URL", None)
    env.update(env_extra or {})
    return subprocess.run([sys.executable, str(CLI), *args], capture_output=True,
                          text=True, env=env, timeout=30)


def test_the_cli_echoes_its_own_command_first():
    # `group list` against a port nothing listens on: the echo must print before the
    # request is attempted, so it survives any failure that follows.
    out = _run(["group", "list", "--base-url", "http://127.0.0.1:1"]).stdout
    first = out.splitlines()[0]
    assert first.startswith("$ python3 ")
    assert "group list" in first
    assert "--base-url http://127.0.0.1:1" in first


def test_the_echo_is_shell_quoted_so_it_can_be_pasted_back():
    """A prompt contains spaces and a template contains braces — unquoted, the echoed
    line would be a different command from the one that ran."""
    out = _run(["group", "run", "--template", "Say Hello {n}", "--count", "3",
                "--no-watch", "--base-url", "http://127.0.0.1:1"]).stdout
    line = out.splitlines()[0]
    assert "--template 'Say Hello {n}'" in line, line


def test_a_prompt_with_a_quote_in_it_stays_pasteable():
    out = _run(["run", "It's fine", "--no-watch",
                "--base-url", "http://127.0.0.1:1"]).stdout
    line = out.splitlines()[0]
    # The whole echoed command must parse back to the argv that produced it.
    parsed = shlex.split(line[2:])
    assert parsed[2:] == ["run", "It's fine", "--no-watch",
                          "--base-url", "http://127.0.0.1:1"], parsed


def test_the_echo_can_be_turned_off():
    out = _run(["group", "list", "--base-url", "http://127.0.0.1:1"],
               {"EVENTBRIDGE_ECHO": "0"}).stdout
    assert not out.startswith("$ ")


def test_the_echo_redacts_userinfo_in_a_url():
    """Nothing passes a credential as an argument today, but a base URL is free-form,
    and an echoed transcript is the wrong place to find out otherwise."""
    out = _run(["group", "list", "--base-url", "http://user:secret@127.0.0.1:1"]).stdout
    line = out.splitlines()[0]
    assert "secret" not in line
    assert "***@127.0.0.1:1" in line


def test_help_does_not_echo():
    """`--help` exits inside argparse, before dispatch — an echo there would be noise
    above the usage text."""
    out = _run(["--help"]).stdout
    assert not out.startswith("$ ")


def test_the_skill_tells_the_agent_to_show_the_command():
    body = SKILL.read_text()
    assert "EVENTBRIDGE_ECHO" in body, \
        "the skill must document the echo, including how to silence it"
    assert "$ python3" in body, "and show what the echoed line looks like"


# ---- the endpoint comes from three sources and nothing else -------------------
#
# A run that silently went to localhost reads as "the agent never replied", and an
# agent that cannot see which endpoint was used starts hunting for one — shelling out
# to kubectl, curling /healthz. Both are removed by saying the endpoint every time.

def test_the_endpoint_is_always_reported_with_its_source():
    out = _run(["group", "list", "--base-url", "http://127.0.0.1:1"]).stdout
    assert "· endpoint http://127.0.0.1:1 (from --base-url)" in out


def test_the_default_endpoint_is_reported_too():
    """The case that used to print nothing at all."""
    out = _run(["group", "list"], {"EVENTBRIDGE_URL": ""}).stdout
    assert "· endpoint http://127.0.0.1:8080 (from default)" in out


def test_eventbridge_url_is_used_when_no_flag_is_given():
    out = _run(["group", "list"],
               {"EVENTBRIDGE_URL": "http://127.0.0.1:1"}).stdout
    assert "· endpoint http://127.0.0.1:1 (from $EVENTBRIDGE_URL)" in out


def test_a_bare_hostname_in_eventbridge_url_is_normalized():
    """The value people export is a Route host copied out of kubectl. Unnormalized,
    urllib rejects it outright with `unknown url type` — this was a real bug."""
    out = _run(["group", "list"],
               {"EVENTBRIDGE_URL": "eventbridge.example.com"}).stdout
    assert "· endpoint https://eventbridge.example.com (from $EVENTBRIDGE_URL)" in out


def test_base_url_outranks_eventbridge_url():
    out = _run(["group", "list", "--base-url", "http://127.0.0.1:1"],
               {"EVENTBRIDGE_URL": "https://wrong.example.com"}).stdout
    assert "(from --base-url)" in out
    assert "wrong.example.com" not in out


def test_an_empty_eventbridge_url_falls_back_instead_of_crashing():
    out = _run(["group", "list"], {"EVENTBRIDGE_URL": "   "}).stdout
    assert "(from default)" in out


def test_an_unreachable_endpoint_exits_2_and_says_how_to_fix_it():
    r = _run(["group", "list", "--base-url", "http://127.0.0.1:1"])
    assert r.returncode == 2, r.stdout + r.stderr
    assert "cannot reach EventBridge" in r.stderr
    assert "endpoint came from: --base-url" in r.stderr
    assert "/healthz" in r.stderr
    assert "Traceback" not in r.stderr


def test_the_default_endpoint_failure_names_eventbridge_url():
    """The message an agent sees when nothing was configured has to point at the fix,
    not at a missing server to go looking for."""
    r = _run(["group", "list"], {"EVENTBRIDGE_URL": "http://127.0.0.1:1"})
    assert r.returncode == 2
    assert "EVENTBRIDGE_URL" in r.stderr


def test_the_cli_cannot_shell_out_at_all():
    """No discovery is possible if no process can be started. Mentioning kubectl in a
    comment is fine — running it is not."""
    src = CLI.read_text()
    for forbidden in ("import subprocess", "os.system", "os.popen", "os.exec",
                      "shutil.which"):
        assert forbidden not in src, \
            f"the CLI must speak plain HTTP only; found {forbidden!r}"


def test_the_skill_forbids_endpoint_discovery():
    body = SKILL.read_text()
    assert "Never discover the endpoint" in body
    assert "$EVENTBRIDGE_URL" in body
    # Every surviving mention of kubectl must be a prohibition, not an instruction.
    for line in body.splitlines():
        if "kubectl" in line:
            assert any(w in line for w in ("no`", "**no**", "Never", "never", "Do not",
                                           "not `kubectl`", "no `kubectl`")), \
                f"SKILL.md still teaches kubectl: {line}"


# ---- progress has to be flushed, not just printed -----------------------------

def test_stdout_is_line_buffered():
    """Python block-buffers an 8 KiB pipe, so under a harness a progress line written
    at t=3s can still be unflushed at t=180s — and is lost outright if the command is
    killed at a timeout. That is what "the CLI reported nothing" actually means."""
    src = CLI.read_text()
    assert "line_buffering=True" in src


def test_partial_output_survives_being_killed():
    """The regression this guards is invisible without a kill: before line buffering,
    the same run returned 238 bytes or 0 depending on timing."""
    import time
    env = dict(os.environ)
    env.pop("PYTHONUNBUFFERED", None)          # must not depend on the environment
    env["EVENTBRIDGE_URL"] = "http://127.0.0.1:1"
    proc = subprocess.Popen(
        [sys.executable, str(CLI), "watch", "cool-ox-1234", "--timeout", "20"],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env)
    try:
        time.sleep(2.5)
        proc.kill()
        out = proc.stdout.read()
    finally:
        proc.wait(timeout=10)
    assert "· endpoint" in out, \
        f"nothing survived the kill, so buffering regressed; got {out!r}"


def test_the_skill_prescribes_submit_then_poll():
    body = SKILL.read_text()
    assert "Submit, then poll" in body
    assert "--no-watch" in body
    assert "2>&1" in body, "the skill must say not to merge the streams"
    i = body.index("2>&1")
    assert "Do not append" in body[max(0, i - 120):i + 40], \
        "the 2>&1 mention must be the prohibition, not an example to copy"
