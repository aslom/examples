#!/usr/bin/env python3
"""Stop the local demo daemons using their PID files. Converted from
`stop-demo.sh` (T0.5).

Escalates SIGINT (graceful) → SIGTERM → SIGKILL, and reclaims stale PID files.
Only relevant to a laptop run; the Kubernetes equivalent is
`k8s_teardown.py`.

  python3 scripts/stop_demo.py
  python3 scripts/stop_demo.py --name eventrunner
  python3 scripts/stop_demo.py --timeout 20
"""
from __future__ import annotations

import argparse
import errno
import os
import pathlib
import signal
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from proclib import Checks  # noqa: E402
from shared.pidfile import default_dir  # noqa: E402

PREFIX = "stop"
NAMES = ("eventbridge", "eventrunner")


def pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as e:
        return e.errno == errno.EPERM     # exists but not ours
    return True


def read_pid(path: pathlib.Path) -> int:
    try:
        return int(path.read_text().strip() or "0")
    except (OSError, ValueError):
        return 0


def wait_gone(pid: int, timeout: float, interval: float = 0.25) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not pid_alive(pid):
            return True
        time.sleep(interval)
    return not pid_alive(pid)


def stop_one(c: Checks, name: str, directory: pathlib.Path, timeout: float) -> None:
    pidfile = directory / f"{name}.pid"
    if not pidfile.exists():
        c.log(f"[{name}] no pidfile at {pidfile} — not running?")
        return
    pid = read_pid(pidfile)
    if not pid or not pid_alive(pid):
        c.log(f"[{name}] stale pidfile (pid={pid}); removing")
        pidfile.unlink(missing_ok=True)
        return

    for sig, label, wait in ((signal.SIGINT, "SIGINT", timeout),
                             (signal.SIGTERM, "SIGTERM", 5.0),
                             (signal.SIGKILL, "SIGKILL", 3.0)):
        c.log(f"[{name}] sending {label} to {pid}")
        try:
            os.kill(pid, sig)
        except OSError as e:
            c.log(f"[{name}] {label} failed: {e}")
        if wait_gone(pid, wait):
            c.ok(f"{name} (pid {pid}) exited after {label}")
            pidfile.unlink(missing_ok=True)
            return
        c.log(f"[{name}] still alive after {wait:.0f}s")

    c.fail(f"{name} (pid {pid}) survived SIGKILL")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", action="append", choices=list(NAMES),
                    help="stop only this daemon (repeatable)")
    ap.add_argument("--timeout", type=float, default=10.0,
                    help="seconds to wait for a graceful SIGINT exit")
    ap.add_argument("--dir", default=None, help="pidfile directory")
    args = ap.parse_args(argv)

    c = Checks(prefix=PREFIX)
    directory = pathlib.Path(args.dir) if args.dir else default_dir()
    c.log(f"pidfile dir: {directory}")
    for name in (args.name or NAMES):
        stop_one(c, name, directory, args.timeout)
    # Nothing running is a legitimate no-op, not a failed run.
    return c.summary(allow_empty=True)


if __name__ == "__main__":
    sys.exit(main())
