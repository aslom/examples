"""Liveness probe: has the consumer loop made progress recently?

DESIGN_PHASE1.md §8.4. The failure this exists to catch is a *live process with a
dead thread*: if the Kafka consumer thread raises, EventRunner's main thread keeps
running, so the pod stays `Running` and reports healthy while consuming nothing.
A process check cannot see it, and EventRunner has no HTTP server to probe.

Used as an `exec` liveness probe in the Deployment:

    livenessProbe:
      exec:
        command: ["python", "-m", "eventrunner.healthcheck"]

Exit 0 = the heartbeat file was touched within ER_HEARTBEAT_MAX_AGE_S.
Exit 1 = missing or stale; the kubelet restarts the pod.

Prints its verdict either way, so `kubectl describe pod` shows the reason rather
than a bare exit code.
"""
from __future__ import annotations

import argparse
import sys

from shared.heartbeat import Heartbeat

from eventrunner.config import load


def main(argv: list[str] | None = None) -> int:
    cfg = load()
    ap = argparse.ArgumentParser(description="EventRunner liveness check")
    ap.add_argument("--max-age", type=float, default=cfg.heartbeat_max_age_s,
                    help="seconds of silence tolerated (default: ER_HEARTBEAT_MAX_AGE_S)")
    ap.add_argument("--path", default=cfg.heartbeat_path,
                    help="heartbeat file (default: ER_HEARTBEAT_PATH)")
    args = ap.parse_args(argv)

    hb = Heartbeat(args.path)
    print(hb.describe(args.max_age))
    return 1 if hb.is_stale(args.max_age) else 0


if __name__ == "__main__":
    sys.exit(main())
