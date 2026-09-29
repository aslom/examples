"""Subprocess + assertion helpers shared by every script in this directory.

Phase 1 replaced the bash scripts with Python (DESIGN_PHASE1.md §9). The reason
was not taste: a bash harness reported `RESULT: PASS` for a `docker` command that
never ran, because `$?` had captured a `sed` in the same pipeline. A test harness
that passes when the command did not execute is worse than no harness.

So this module is built around making that specific bug unrepresentable:

  * `run()` NEVER uses a shell and NEVER takes a string — argv is a list, so
    there is no quoting layer to get wrong.
  * A command that could not even be launched comes back as a `Result` with
    `launched=False`, and `Result.__bool__` is False. `if res:` is therefore
    correct rather than always-true, which is the Python shape of the bash bug.
  * `Checks.summary()` fails a run in which **zero** assertions executed. A
    harness that silently skipped all its work does not get to exit 0.

Stdlib only.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

# ---- output -----------------------------------------------------------------

def _tty() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    return sys.stdout.isatty()


_GREEN = "\033[32m"
_RED   = "\033[31m"
_YEL   = "\033[33m"
_OFF   = "\033[0m"


def _paint(colour: str, text: str) -> str:
    return f"{colour}{text}{_OFF}" if _tty() else text


# ---- run --------------------------------------------------------------------

@dataclass
class Result:
    """The outcome of one subprocess call.

    `launched` distinguishes "ran and failed" from "never ran" — the binary was
    missing, the argv was malformed, or it timed out before exec. Both are
    failures, but only the second means the assertion tested nothing.
    """
    argv: tuple[str, ...]
    rc: int
    out: str
    err: str
    duration_s: float
    launched: bool = True
    timed_out: bool = False

    @property
    def ok(self) -> bool:
        return self.launched and self.rc == 0

    def __bool__(self) -> bool:
        # Deliberate: a Result must not be unconditionally truthy. `if res:` is
        # the natural thing to write, and it has to mean "the command succeeded".
        return self.ok

    @property
    def cmd(self) -> str:
        return " ".join(self.argv)

    def json(self) -> Any:
        """Parse stdout as JSON. Raises on a failed command rather than on a
        confusing JSONDecodeError twenty lines later."""
        import json
        if not self.launched:
            raise RuntimeError(f"command never ran: {self.cmd}: {self.err}")
        if self.rc != 0:
            raise RuntimeError(f"command failed rc={self.rc}: {self.cmd}\n{self.err.strip()}")
        return json.loads(self.out or "null")

    def tail(self, n: int = 20) -> str:
        """Last n lines of stderr, else stdout — for failure messages."""
        src = self.err.strip() or self.out.strip()
        lines = src.splitlines()
        return "\n".join(lines[-n:])


def run(argv: Sequence[str], *, timeout: float = 120.0, stdin: str | None = None,
        env: dict[str, str] | None = None, cwd: str | None = None,
        merge_stderr: bool = False) -> Result:
    """Run argv (a LIST — never a shell string) and capture its output.

    Never raises for a non-zero exit code; inspect `Result.ok`. Raises nothing
    for a missing binary either — that comes back as `launched=False`, so a
    caller that forgets to check still records a FAIL rather than a PASS.
    """
    if isinstance(argv, (str, bytes)):
        raise TypeError("run() takes an argv list, not a shell string — "
                        "that is the quoting bug this module exists to avoid")
    argv = tuple(str(a) for a in argv)
    started = time.monotonic()
    try:
        p = subprocess.run(
            argv, capture_output=not merge_stderr,
            stdout=subprocess.PIPE if merge_stderr else None,
            stderr=subprocess.STDOUT if merge_stderr else None,
            input=stdin, text=True, timeout=timeout,
            env={**os.environ, **env} if env else None, cwd=cwd,
        )
    except FileNotFoundError as e:
        return Result(argv, 127, "", f"{argv[0]}: not found on PATH ({e})",
                      time.monotonic() - started, launched=False)
    except PermissionError as e:
        return Result(argv, 126, "", f"{argv[0]}: not executable ({e})",
                      time.monotonic() - started, launched=False)
    except subprocess.TimeoutExpired as e:
        return Result(argv, 124, _text(e.stdout), f"timed out after {timeout}s",
                      time.monotonic() - started, launched=True, timed_out=True)
    return Result(argv, p.returncode, p.stdout or "", p.stderr or "",
                  time.monotonic() - started)


def _text(v: Any) -> str:
    if v is None:
        return ""
    return v.decode(errors="replace") if isinstance(v, (bytes, bytearray)) else str(v)


def have(binary: str) -> bool:
    return shutil.which(binary) is not None


# ---- assertions -------------------------------------------------------------

@dataclass
class Checks:
    """Accumulates PASS/FAIL lines and owns the process exit code.

    Preserves the Phase 0 output contract exactly — `[e2e] PASS …` /
    `[e2e] FAIL …` — so converted scripts read identically in CI.
    """
    prefix: str = "e2e"
    passed: int = 0
    failed: int = 0
    failures: list[str] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)

    # ---- plain logging ----
    def log(self, msg: str) -> None:
        print(f"[{self.prefix}] {msg}", flush=True)

    def warn(self, msg: str) -> None:
        print(f"[{self.prefix}] {_paint(_YEL, 'WARN')} {msg}", flush=True)

    def section(self, title: str) -> None:
        print(f"\n[{self.prefix}] ── {title} ──", flush=True)

    # ---- assertions ----
    def ok(self, desc: str) -> bool:
        self.passed += 1
        print(f"[{self.prefix}] {_paint(_GREEN, 'PASS')} {desc}", flush=True)
        return True

    def fail(self, desc: str, detail: str = "") -> bool:
        self.failed += 1
        self.failures.append(desc)
        print(f"[{self.prefix}] {_paint(_RED, 'FAIL')} {desc}", flush=True)
        if detail:
            for line in str(detail).rstrip().splitlines():
                print(f"[{self.prefix}]      {line}", flush=True)
        return False

    def expect(self, cond: Any, desc: str, detail: str = "") -> bool:
        """Assert a condition. A `Result` passed here is evaluated via its
        `__bool__`, i.e. launched-and-rc-0, never "is not None"."""
        if isinstance(cond, Result) and not detail:
            detail = "" if cond.ok else f"$ {cond.cmd}\n{cond.tail()}"
        return self.ok(desc) if bool(cond) else self.fail(desc, detail)

    def expect_run(self, res: Result, desc: str) -> bool:
        """Assert a command both ran and succeeded, with its own diagnostics."""
        if not res.launched:
            return self.fail(desc, f"command never executed: {res.cmd}\n{res.err}")
        if res.timed_out:
            return self.fail(desc, f"timed out: {res.cmd}")
        if res.rc != 0:
            return self.fail(desc, f"rc={res.rc}: {res.cmd}\n{res.tail()}")
        return self.ok(desc)

    def expect_contains(self, haystack: str, needle: str, desc: str) -> bool:
        if needle in (haystack or ""):
            return self.ok(desc)
        return self.fail(desc, f"{needle!r} not found in {len(haystack or '')} chars of output")

    def expect_eq(self, actual: Any, expected: Any, desc: str) -> bool:
        if actual == expected:
            return self.ok(desc)
        return self.fail(desc, f"expected {expected!r}, got {actual!r}")

    def note(self, key: str, value: Any) -> None:
        """Record a measurement to print in the summary (e.g. wake latency)."""
        self.notes[key] = value

    # ---- verdict ----
    @property
    def total(self) -> int:
        return self.passed + self.failed

    def summary(self, *, allow_empty: bool = False) -> int:
        """Print the verdict and return the process exit code.

        Exit 0 requires every assertion to have passed **and** at least one
        assertion to have run. A harness that asserted nothing has not proven
        anything, and must not look like a success.
        """
        print()
        if self.notes:
            for k, v in self.notes.items():
                print(f"[{self.prefix}] {k}: {v}", flush=True)
        if self.total == 0 and not allow_empty:
            print(f"[{self.prefix}] {_paint(_RED, 'FAIL')} no checks ran — "
                  f"nothing was verified", flush=True)
            return 1
        if self.failed == 0:
            if self.total == 0:
                self.log("nothing to do — no checks were needed")
            else:
                self.log(f"ALL {self.passed} CHECKS PASSED")
            return 0
        self.log(f"{self.failed} of {self.total} check(s) FAILED:")
        for f in self.failures:
            print(f"[{self.prefix}]   - {f}", flush=True)
        return 1


def die(msg: str, *, prefix: str = "e2e", code: int = 1) -> None:
    """Abort for a precondition that is not an assertion failure — a missing
    binary, an unreachable daemon. Distinct from FAIL on purpose: it means the
    test could not run, not that the system under test is broken."""
    print(f"[{prefix}] {_paint(_RED, 'ABORT')} {msg}", file=sys.stderr, flush=True)
    sys.exit(code)


# ---- polling ----------------------------------------------------------------

@dataclass
class WaitOutcome:
    ok: bool
    elapsed_s: float
    attempts: int
    last: Any = None

    def __bool__(self) -> bool:
        return self.ok


def wait_for(predicate: Callable[[], Any], *, timeout: float, interval: float = 1.0,
             desc: str = "", on_tick: Callable[[int, Any], None] | None = None) -> WaitOutcome:
    """Poll `predicate` until it returns something truthy, or until timeout.

    "Wait for X, up to N seconds, and say exactly why if it never happens" is
    most of the logic in every deploy script; in bash it is a copy-pasted
    `for i in $(seq …)` each time. Exceptions from the predicate are treated as
    "not yet" — a kubectl call can legitimately fail while an object is being
    created — but the last one is kept for the failure message.
    """
    started = time.monotonic()
    attempts = 0
    last: Any = None
    while True:
        attempts += 1
        try:
            last = predicate()
            if last:
                return WaitOutcome(True, time.monotonic() - started, attempts, last)
        except Exception as e:  # noqa: BLE001 - transient API errors are expected
            last = e
        if on_tick:
            on_tick(attempts, last)
        if time.monotonic() - started >= timeout:
            return WaitOutcome(False, time.monotonic() - started, attempts, last)
        time.sleep(interval)


__all__ = ["Result", "run", "have", "Checks", "die", "wait_for", "WaitOutcome"]
