#!/usr/bin/env python3
"""Watch the requests/responses topics and the consumer group, refreshed every 2s.

A demo-friendly answer to "how many events, and how much is actually done?" — and to the
harder question behind it: which records are **claimed by a consumer but not yet
committed as processed**.

What Kafka can and cannot tell you:

  * It stores COMMITTED offsets, never a live consumer's in-memory position. So there is
    no server-side "this record is claimed and half-processed" flag to read.
  * It does tell you, per partition, whether a live group member currently OWNS it
    (`kafka-consumer-groups.sh --describe` prints a CONSUMER-ID, or `-` when nobody
    holds it). Combined with lag that splits pending work two ways:
        lag on an ASSIGNED partition    -> claimed, being worked, not yet committed
        lag on an UNASSIGNED partition  -> not claimed by anyone yet (queued)
  * And in THIS system lag means more than "unread". RQ-1 defers the offset commit until
    the agent's terminal event, so a record stays uncommitted for the whole run. Lag is
    therefore "work not finished", which is exactly the number worth showing — and it is
    also the number KEDA scales on.

For the precise running-vs-queued split, EventBridge's own group API is authoritative
(`--group <id>` adds it), because it tracks per-member state rather than inferring it
from offsets.

  python3 scripts/watch_topics.py
  python3 scripts/watch_topics.py --group latest        # follows whatever runs next
  python3 scripts/watch_topics.py --group quiet-whale-6700
  python3 scripts/watch_topics.py --once
  KUBECONFIG=.kube/config-kind python3 scripts/watch_topics.py --namespace kev1
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from k8slib import Kubectl  # noqa: E402
from proclib import die  # noqa: E402

KAFKA_NS = "kafka"
BROKER_SELECTOR = "strimzi.io/name=my-cluster-kafka"
CLEAR = "\033[H\033[J"
DIM, BOLD, OFF = "\033[2m", "\033[1m", "\033[0m"
GREEN, YELLOW, BLUE = "\033[32m", "\033[33m", "\033[34m"


def _color(on: bool):
    if on:
        return DIM, BOLD, OFF, GREEN, YELLOW, BLUE
    return ("",) * 6


def _num(v: str) -> int | None:
    return int(v) if v.lstrip("-").isdigit() else None


class Watcher:
    def __init__(self, k: Kubectl, args) -> None:
        self.k = k
        self.args = args
        self.broker = self._find_broker()
        self.prev: dict[str, int] = {}

    def _find_broker(self) -> str:
        pods = self.k.pod_names(BROKER_SELECTOR, namespace=KAFKA_NS)
        if not pods:
            die(f"no Kafka broker pod matching {BROKER_SELECTOR} in ns {KAFKA_NS}",
                prefix="watch")
        return pods[0]

    def _exec(self, argv: list[str], timeout: float = 30.0) -> str:
        res = self.k.call(["exec", self.broker, "--", *argv],
                          namespace=KAFKA_NS, timeout=timeout)
        return res.out if res.ok else ""

    # ---- offsets -----------------------------------------------------------
    def offsets(self, when: str) -> dict[str, dict[int, int]]:
        """{topic: {partition: offset}} for `--time` (latest / earliest)."""
        regex = f"^({'|'.join(self.args.topics)})$"
        out = self._exec(["bin/kafka-get-offsets.sh",
                          "--bootstrap-server", "localhost:9092",
                          "--topic", regex, "--time", when])
        res: dict[str, dict[int, int]] = {}
        for line in out.splitlines():
            parts = line.strip().split(":")
            if len(parts) != 3 or not parts[2].lstrip("-").isdigit():
                continue
            topic, part, off = parts[0], int(parts[1]), int(parts[2])
            res.setdefault(topic, {})[part] = off
        return res

    def group_state(self, group: str) -> tuple[dict[int, dict], bool]:
        """Per-partition committed/end/lag plus whether a live member owns it."""
        out = self._exec(["bin/kafka-consumer-groups.sh",
                          "--bootstrap-server", "localhost:9092",
                          "--describe", "--group", group])
        rows: dict[int, dict] = {}
        has_members = "has no active members" not in out
        for line in out.splitlines():
            f = line.split()
            if len(f) < 7 or f[0] != group:
                continue
            try:
                part = int(f[2])
            except ValueError:
                continue
            rows[part] = {"topic": f[1], "committed": _num(f[3]), "end": _num(f[4]),
                          "lag": _num(f[5]) or 0,
                          # '-' means no live member owns this partition right now.
                          "assigned": f[6] != "-"}
        return rows, has_members

    # ---- eventbridge (authoritative running/queued) -------------------------
    def group_status(self, url: str, groupid: str) -> dict | None:
        try:
            with urllib.request.urlopen(
                    f"{url.rstrip('/')}/v0/groups/{groupid}/status", timeout=8) as r:
                return json.loads(r.read())
        except (urllib.error.URLError, OSError, ValueError):
            return None

    def latest_group(self, url: str) -> str | None:
        """The most recently created group.

        Re-resolved on every frame when `--group latest` is used, so the watcher can be
        started BEFORE the batch and pick it up when it appears. Otherwise a demo has a
        copy-paste step in the middle: you cannot know the id until `group run` has
        printed it, by which point the interesting first seconds are gone.
        """
        try:
            with urllib.request.urlopen(f"{url.rstrip('/')}/v0/groups", timeout=8) as r:
                groups = json.loads(r.read()).get("groups") or []
        except (urllib.error.URLError, OSError, ValueError):
            return None
        return groups[0]["groupid"] if groups else None

    # ---- render ------------------------------------------------------------
    def render(self) -> str:
        dim, bold, off, green, yellow, blue = _color(not self.args.no_color)
        latest = self.offsets("-1")
        earliest = self.offsets("-2")
        lines = [f"{bold}topics{off}   {dim}(end − earliest = records still retained){off}"]
        lines.append(f"  {'TOPIC':<18}{'RECORDS':>9}{'PRODUCED':>10}{'PARTS':>7}{'RATE/s':>9}")
        for topic in self.args.topics:
            ends, starts = latest.get(topic, {}), earliest.get(topic, {})
            end_sum = sum(ends.values())
            retained = end_sum - sum(starts.values())
            prev = self.prev.get(topic)
            rate = ""
            if prev is not None and self.args.interval > 0:
                d = (end_sum - prev) / self.args.interval
                rate = f"{d:+.1f}" if d else "·"
            self.prev[topic] = end_sum
            lines.append(f"  {topic:<18}{retained:>9}{end_sum:>10}{len(ends):>7}{rate:>9}")

        rows, has_members = self.group_state(self.args.group_id)
        total_lag = sum(r["lag"] for r in rows.values())
        claimed = sum(r["lag"] for r in rows.values() if r["assigned"])
        unclaimed = total_lag - claimed
        owners = sum(1 for r in rows.values() if r["assigned"])
        lines.append("")
        lines.append(f"{bold}consumer group{off} {self.args.group_id} "
                     f"{dim}({'active' if has_members else 'no live members'}){off}")
        lines.append(f"  {'lag (work not finished)':<30}{total_lag:>8}   "
                     f"{dim}RQ-1 defers the commit to the terminal event,{off}")
        lines.append(f"  {'  ├─ claimed, in flight':<30}{green}{claimed:>8}{off}   "
                     f"{dim}so lag means unfinished, not merely unread{off}")
        lines.append(f"  {'  └─ not claimed yet':<30}{yellow}{unclaimed:>8}{off}")
        lines.append(f"  {'partitions owned by a member':<30}{owners:>8} / {len(rows)}")

        if self.args.group:
            gid = self.args.group
            if gid in ("latest", "-"):
                gid = self.latest_group(self.args.url)
            p = self.group_status(self.args.url, gid) if gid else None
            lines.append("")
            if gid is None:
                lines.append(f"{dim}no groups yet — waiting for one to be created{off}")
            elif p is None:
                lines.append(f"{dim}group {gid}: EventBridge unreachable{off}")
            else:
                lines.append(f"{bold}group{off} {p.get('label') or gid} "
                             f"{dim}{gid} (EventBridge — per-member truth){off}")
                lines.append(f"  {p.get('terminal')}/{p.get('denominator')} done · "
                             f"{blue}{p.get('running')} running{off} · "
                             f"{p.get('queued')} queued · "
                             f"{p.get('failed')} failed · {p.get('elapsed')}"
                             + (f" · {p.get('eta')} left" if p.get("eta") else ""))
        return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--namespace", "-n", default=os.environ.get("NS", "kev1"),
                    help="namespace the topics are named after (kev1 -> kev1-requests)")
    ap.add_argument("--kubeconfig", default=None)
    ap.add_argument("--context", default=None)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--once", action="store_true", help="print one snapshot and exit")
    ap.add_argument("--group", default=None, metavar="GROUPID",
                    help="also show EventBridge's per-member running/queued split. Pass "
                         "'latest' to follow the most recent group, re-resolved every "
                         "frame — so you can start this BEFORE launching the batch.")
    ap.add_argument("--url", default=None, help="EventBridge base URL (for --group)")
    ap.add_argument("--no-color", action="store_true")
    args = ap.parse_args(argv)

    args.topics = [f"{args.namespace}-requests", f"{args.namespace}-responses"]
    args.group_id = f"{args.namespace}-eventrunner"
    k = Kubectl(context=args.context, namespace=args.namespace,
                kubeconfig=args.kubeconfig)
    if args.group and not args.url:
        # Discover the Route so --group needs one flag, not two.
        r = k.get("route", "eventbridge", namespace=args.namespace, missing_ok=True)
        if r:
            host = (r.get("spec") or {}).get("host")
            if host:
                args.url = f"https://{host}"
        args.url = args.url or "http://127.0.0.1:8080"

    w = Watcher(k, args)
    if args.once:
        print(w.render())
        return 0
    try:
        while True:
            frame = w.render()
            # Redraw over the previous frame instead of scrolling: this is meant to sit
            # on a second screen during a demo.
            sys.stdout.write(CLEAR + frame + "\n")
            sys.stdout.flush()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
