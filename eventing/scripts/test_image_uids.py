#!/usr/bin/env python3
"""Verify both images run under every UID scheme we deploy to.

Three targets, three different UID behaviours:

  laptop / plain Docker  runs as the image's declared uid 10001
  OpenShift              the SCC replaces that uid with an arbitrary one from
                         the namespace range; the process always gets gid 0
  vanilla Kubernetes     restricted Pod Security wants runAsNonRoot: true, so
                         the uid is set explicitly and must be numeric

The image must be able to write $TMPDIR and $HOME, and import its own package,
in all three. Run after building:

    python3 scripts/test_image_uids.py
    python3 scripts/test_image_uids.py --registry quay.io/aslomnet/ --tag dev

Exit code 0 only if every combination passed.

No shell is involved: every argument is a separate argv element, so there is no
quoting layer to get wrong.
"""
from __future__ import annotations

import argparse
import subprocess
import sys

# Runs inside the container. Passed as a single argv element, so quoting is a
# non-issue.
PROBE = r"""
import os, pathlib, sys
print("uid=%s gid=%s groups=%s HOME=%s TMPDIR=%s" % (
    os.getuid(), os.getgid(), os.getgroups(),
    os.environ.get("HOME"), os.environ.get("TMPDIR")))
bad = []
for label in ("TMPDIR", "HOME"):
    base = os.environ.get(label)
    if not base:
        print("%s unset" % label); bad.append(label); continue
    try:
        d = pathlib.Path(base) / "uidprobe"
        d.mkdir(parents=True, exist_ok=True)
        (d / "f").write_text("ok")
        print("write %-6s %-12s OK" % (label, base))
    except Exception as exc:
        print("write %-6s %-12s FAIL %s" % (label, base, exc)); bad.append(label)
mod = sys.argv[1] if len(sys.argv) > 1 else ""
try:
    __import__(mod + ".config")
    cfg = sys.modules[mod + ".config"].load()
    print("config OK (%s)" % mod)
except Exception as exc:
    print("config FAIL %s" % exc); bad.append("config")
sys.exit(1 if bad else 0)
"""

# (user spec passed to --user, human label). None means "use the image default".
UID_MODES = [
    (None, "laptop-default (uid 10001 from the image)"),
    ("1000910000:0", "openshift-arbitrary (SCC-assigned uid, gid 0)"),
    ("10001:0", "vanilla-k8s-explicit (runAsUser + gid 0)"),
]

SERVICES = [("rossoctl-eventbridge", "eventbridge"), ("rossoctl-eventrunner", "eventrunner")]


def run_case(docker: str, image: str, module: str, user: str | None) -> tuple[bool, str]:
    cmd = [docker, "run", "--rm", "-e", "PYTHONPATH=/app"]
    if user:
        cmd += ["--user", user]
    cmd += [image, "python", "-c", PROBE, module]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    return p.returncode == 0, (p.stdout + p.stderr).strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default="quay.io/aslomnet/")
    ap.add_argument("--tag", default="dev")
    ap.add_argument("--docker", default="docker")
    args = ap.parse_args()

    prefix = args.registry if args.registry.endswith("/") else args.registry + "/"
    failures = 0
    for repo, module in SERVICES:
        image = f"{prefix}{repo}:{args.tag}"
        print(f"\n=== {image} ===")
        for user, label in UID_MODES:
            ok, out = run_case(args.docker, image, module, user)
            print(f"  {'PASS' if ok else 'FAIL'}  {label}")
            for line in out.splitlines():
                print(f"          {line}")
            if not ok:
                failures += 1

    total = len(SERVICES) * len(UID_MODES)
    print()
    if failures:
        print(f"FAILED: {failures} of {total} combinations")
        return 1
    print(f"OK: all {total} combinations pass")
    return 0


if __name__ == "__main__":
    sys.exit(main())
