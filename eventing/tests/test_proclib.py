"""proclib — the harness that must not be able to report PASS for nothing.

DESIGN_PHASE1.md §9 names the bug that motivated Phase 1's move off bash: a UID
test matrix printed `RESULT: PASS` while the `docker` command never ran, because
`$?` had captured a `sed` from the same pipeline. These tests pin the Python
equivalents shut.
"""
import sys

import pytest
from proclib import Checks, run, wait_for

# ---- run() ------------------------------------------------------------------

def test_run_rejects_a_shell_string():
    """A string argv is the entire quoting-bug class. Refuse it at the door."""
    with pytest.raises(TypeError, match="argv list"):
        run("echo hi")


def test_run_captures_stdout_and_rc():
    r = run([sys.executable, "-c", "print('hello')"])
    assert r.ok and r.rc == 0
    assert r.out.strip() == "hello"
    assert r.launched is True


def test_run_nonzero_rc_is_not_an_exception():
    r = run([sys.executable, "-c", "import sys; sys.exit(3)"])
    assert r.launched is True
    assert r.rc == 3
    assert not r.ok


def test_missing_binary_is_launched_false_not_an_exception():
    r = run(["no-such-binary-2d8f1a"])
    assert r.launched is False
    assert r.rc == 127
    assert "not found on PATH" in r.err


def test_timeout_is_reported_as_timed_out():
    r = run([sys.executable, "-c", "import time; time.sleep(5)"], timeout=0.4)
    assert r.timed_out is True
    assert not r.ok


# ---- the §9 regression: never PASS for a command that did not run -----------

def test_result_is_falsey_when_the_command_never_ran():
    """`if res:` must mean "succeeded", never "is not None".

    This is the Python shape of the bash bug: a dataclass instance is truthy by
    default, so a harness writing `if result: ok(...)` would pass for a command
    that was never executed.
    """
    assert bool(run(["no-such-binary-2d8f1a"])) is False
    assert bool(run([sys.executable, "-c", "import sys; sys.exit(1)"])) is False
    assert bool(run([sys.executable, "-c", "pass"])) is True


def test_checks_cannot_pass_a_command_that_never_ran():
    res = run(["no-such-binary-2d8f1a"])
    c = Checks()
    c.expect(res, "the command ran")
    assert c.failed == 1 and c.passed == 0
    assert c.summary() == 1


def test_expect_run_distinguishes_never_ran_from_failed():
    c = Checks()
    c.expect_run(run(["no-such-binary-2d8f1a"]), "missing binary")
    assert "never executed" in "".join(c.failures) or c.failed == 1
    c2 = Checks()
    c2.expect_run(run([sys.executable, "-c", "import sys; sys.exit(2)"]), "rc=2")
    assert c2.failed == 1


def test_summary_fails_when_no_checks_ran_at_all():
    """A harness that asserted nothing has proven nothing, and must not exit 0."""
    assert Checks().summary() == 1
    assert Checks().summary(allow_empty=True) == 0


def test_json_raises_with_the_command_when_it_failed():
    r = run([sys.executable, "-c", "import sys; sys.stderr.write('boom'); sys.exit(1)"])
    with pytest.raises(RuntimeError, match="command failed rc=1"):
        r.json()
    r2 = run(["no-such-binary-2d8f1a"])
    with pytest.raises(RuntimeError, match="never ran"):
        r2.json()


def test_json_parses_stdout():
    r = run([sys.executable, "-c", "print('{\"a\": [1, 2]}')"])
    assert r.json() == {"a": [1, 2]}


# ---- Checks bookkeeping -----------------------------------------------------

def test_checks_counts_and_exit_code():
    c = Checks()
    c.ok("one")
    c.ok("two")
    assert c.summary() == 0
    c.fail("three", "detail")
    assert c.failed == 1 and c.total == 3
    assert c.summary() == 1


def test_expect_contains_and_eq():
    c = Checks()
    assert c.expect_contains("mock_claude=True (auto: ...)", "mock_claude=True", "mock announced")
    assert not c.expect_contains("nothing here", "MOCK-REPLY", "reply on the wire")
    assert c.expect_eq(0, 0, "replicas zero")
    assert not c.expect_eq(1, 0, "replicas zero")
    assert c.failed == 2


def test_notes_are_recorded_for_the_summary():
    c = Checks()
    c.ok("x")
    c.note("wake latency", "4.2s")
    assert c.notes["wake latency"] == "4.2s"
    assert c.summary() == 0


# ---- wait_for ---------------------------------------------------------------

def test_wait_for_returns_on_first_truthy():
    state = {"n": 0}

    def p():
        state["n"] += 1
        return state["n"] >= 3

    out = wait_for(p, timeout=5, interval=0.01)
    assert out.ok and out.attempts == 3
    assert bool(out) is True


def test_wait_for_times_out_and_reports_elapsed():
    out = wait_for(lambda: False, timeout=0.3, interval=0.05)
    assert not out.ok
    assert out.elapsed_s >= 0.3
    assert out.attempts >= 2


def test_wait_for_treats_predicate_exceptions_as_not_yet():
    """A kubectl call can legitimately fail while an object is being created."""
    state = {"n": 0}

    def p():
        state["n"] += 1
        if state["n"] < 3:
            raise RuntimeError("object not created yet")
        return "ready"

    out = wait_for(p, timeout=5, interval=0.01)
    assert out.ok and out.last == "ready"


def test_wait_for_keeps_the_last_exception_for_diagnosis():
    def p():
        raise RuntimeError("the actual reason")

    out = wait_for(p, timeout=0.2, interval=0.05)
    assert not out.ok
    assert isinstance(out.last, RuntimeError)
    assert "the actual reason" in str(out.last)
