#!/usr/bin/env python3
"""Minimal Kubernetes end-to-end test. DESIGN_PHASE1.md §14, task T5.1.

Mirrors docker_e2e_test.py in structure, flags and output style — same
`[e2e] PASS` lines, same "exit 0 only if every assertion passed" contract. Runs
from a laptop with `kubectl`, so it needs no in-cluster image.

Assertions 4, 6, 7 and 11 are the Phase 1 point: they test KEDA rather than the
wire. The rest carry over from Phase 0 and guard against regression.

   1 preflight passes                        7 a pod starts, log says mock auto:
   2 both KafkaTopics Ready                  8 final=true within 120s
   3 /healthz 200 on the PUBLIC url          9 the reply contains MOCK-REPLY
   4 idle: Ready/Active=False, 0 replicas   10 events.jsonl + /turns
   5 POST /v0/agents -> 202                 11 scale back to ZERO
                                            12 cold-start latency recorded

Mock mode is guaranteed by the test overlay mounting no credential, so runs are
free and deterministic.

  ./scripts/k8s-e2e-test.sh                    # detect, deploy if needed, test
  ./scripts/k8s-e2e-test.sh --namespace kev2
  ./scripts/k8s-e2e-test.sh --force-deploy
  ./scripts/k8s-e2e-test.sh --no-deploy        # fail if not already deployed
  ./scripts/k8s-e2e-test.sh --keep             # skip the teardown
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

import k8s_deploy  # noqa: E402
import k8s_preflight  # noqa: E402
from imagelib import DEFAULT_REGISTRY, DEFAULT_TAG  # noqa: E402
from k8slib import Kubectl, condition_is, condition_reason, dig, wait_for  # noqa: E402
from proclib import Checks  # noqa: E402

PREFIX = "e2e"
KAFKA_NS = "kafka"
PROMPT = "k8s-e2e probe"


def get_json(url: str, *, timeout: float = 8.0, method: str = "GET",
             body: bytes | None = None, content_type: str = "application/json"):
    req = urllib.request.Request(url, data=body, method=method)
    if body is not None:
        req.add_header("Content-Type", content_type)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, (json.loads(raw) if raw else None)


def get_bytes(url: str, *, timeout: float = 10.0) -> tuple[int, bytes]:
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read()


class E2E:
    def __init__(self, k: Kubectl, c: Checks, args) -> None:
        self.k = k
        self.c = c
        self.args = args
        self.ns = args.namespace
        self.base = ""
        self.corr = ""

    # ---- 1 ----
    def assert_preflight(self) -> None:
        self.c.section("1. preflight")
        skip = {int(s) for s in self.args.skip.split(",") if s.strip()}
        k8s_preflight.run_pre(k8s_preflight.Preflight(self.k, self.c, self.args), skip)

    def deploy_if_needed(self) -> None:
        self.c.section("deploy (only if the manifests changed — §14.1)")
        overlay = k8s_deploy.overlay_path(self.args.overlay)
        needed, reason = k8s_deploy.deploy_needed(self.k, self.c, overlay, self.ns)
        self.c.log(f"change detection: {'NEEDED' if needed else 'not needed'} — {reason}")
        if not needed and not self.args.force_deploy:
            self.c.ok("no deploy needed (manifests unchanged, no cluster-side drift)")
            return
        if self.args.no_deploy:
            self.c.fail("already deployed and up to date (--no-deploy)", reason)
            return
        d = k8s_deploy.Deployer(self.k, self.c, self.args)
        d.step1_namespace()
        d.step2_topics()
        d.step3_apply_overlay()
        d.step4_publish_url()
        d.step5_verify_idle()
        if not self.c.failed:
            d.stamp_digest()

    # ---- 2 ----
    def assert_topics(self) -> None:
        self.c.section("2. both KafkaTopics Ready")
        for topic in (f"{self.ns}-requests", f"{self.ns}-responses"):
            t = self.k.get("kafkatopic", topic, namespace=KAFKA_NS, missing_ok=True)
            self.c.expect(t is not None and condition_is(t, "Ready"),
                          f"KafkaTopic {topic} is Ready",
                          condition_reason(t or {}, "Ready") if t else "not found")

    # ---- 3 ----
    def assert_public_url(self) -> None:
        self.c.section("3. EventBridge available on the PUBLIC url")
        pf = k8s_preflight.Preflight(self.k, self.c, self.args)
        self.base = self.args.public_url or pf.route_url()
        self.c.expect(bool(self.base), f"a public URL exists: {self.base or '(none)'}")
        if not self.base:
            return
        self.c.expect(self.k.ready_replicas("eventbridge", namespace=self.ns) >= 1,
                      "deploy/eventbridge has a ready replica")
        pf.check_public_url(self.base)

    # ---- 4 ----
    def assert_idle(self) -> None:
        """The idle state is ZERO replicas. That is correct, not a failure."""
        self.c.section("4. idle state: Ready=True, Active=False, 0 replicas")
        so = self.k.get("scaledobject", "eventrunner", namespace=self.ns, missing_ok=True) or {}
        self.c.expect(condition_is(so, "Ready"), "ScaledObject Ready=True",
                      condition_reason(so, "Ready"))

        # `spec.replicas == 0` is NOT the same as "no consumer is running": the pod
        # survives until it exits or its terminationGracePeriodSeconds expires, and
        # until then it is still a member of the consumer group and still consuming.
        #
        # This matters for assertion 6, not just for tidiness. If a leftover pod
        # consumes the request we are about to post, lag never rises, KEDA correctly
        # never scales up, and "KEDA activates within 60s" fails while the system is
        # behaving perfectly. Both Kind runs failed at exactly ~60s for this reason
        # before the wait below was added.
        def truly_idle():
            if self.k.replicas("eventrunner", namespace=self.ns) != 0:
                return False
            if self.k.pod_names("app.kubernetes.io/name=eventrunner", namespace=self.ns):
                return False
            return condition_is(self.k.get("scaledobject", "eventrunner",
                                           namespace=self.ns, missing_ok=True) or {},
                                "Active", "False")

        settled = wait_for(truly_idle, timeout=self.args.idle_timeout, interval=5)
        so = self.k.get("scaledobject", "eventrunner", namespace=self.ns, missing_ok=True) or {}
        pods = self.k.pod_names("app.kubernetes.io/name=eventrunner", namespace=self.ns)
        self.c.expect(settled,
                      f"Active=False, 0 replicas AND no lingering eventrunner pod "
                      f"(settled after {settled.elapsed_s:.0f}s)",
                      f"Active={condition_reason(so, 'Active')} "
                      f"replicas={self.k.replicas('eventrunner', namespace=self.ns)} "
                      f"pods={pods}\n"
                      f"  A pod still terminating is still consuming. Left-over lag "
                      f"from an earlier run also keeps it Active; that is real work "
                      f"pending, not a fault.")

    # ---- 5, 6, 7, 12 ----
    def assert_wake(self) -> None:
        self.c.section("5. POST /v0/agents")
        posted = time.monotonic()
        try:
            status, body = get_json(f"{self.base}/v0/agents", method="POST", timeout=20,
                                    body=json.dumps({"prompt": PROMPT,
                                                     "max_turns": 1}).encode())
        except (urllib.error.URLError, OSError) as e:
            self.c.fail("POST /v0/agents returned 202", str(e))
            return
        self.corr = (body or {}).get("correlationid", "")
        self.c.expect(status == 202 and self.corr,
                      f"POST /v0/agents -> {status} correlationid={self.corr}")
        if not self.corr:
            return

        self.c.section("6. KEDA activates within 60s")
        woke = wait_for(
            lambda: (condition_is(self.k.get("scaledobject", "eventrunner",
                                             namespace=self.ns, missing_ok=True) or {},
                                  "Active")
                     or self.k.replicas("eventrunner", namespace=self.ns) >= 1),
            timeout=60, interval=2)
        so = self.k.get("scaledobject", "eventrunner", namespace=self.ns, missing_ok=True) or {}
        replicas = self.k.replicas("eventrunner", namespace=self.ns)
        self.c.expect(woke, f"KEDA went Active and scaled up "
                            f"(after {woke.elapsed_s:.1f}s, replicas={replicas})",
                      f"Active={condition_reason(so, 'Active')} replicas={replicas}")
        self.c.note("wake latency (POST -> scale-up)", f"{woke.elapsed_s:.1f}s")
        # RQ-4 assumed < 10s on a warm node. §16 Gap A records a floor of ~3s from
        # the broker's group.initial.rebalance.delay.ms alone.
        if woke.ok and woke.elapsed_s > 10:
            self.c.warn(f"wake took {woke.elapsed_s:.1f}s, over RQ-4's 10s assumption "
                        f"(first pull on a cold node, or the 3s rebalance floor)")

        self.c.section("7. a pod starts and announces auto-selected mock mode")
        got_pod = wait_for(
            lambda: bool(self.k.pod_names("app.kubernetes.io/name=eventrunner",
                                          namespace=self.ns)),
            timeout=90, interval=2)
        pods = self.k.pod_names("app.kubernetes.io/name=eventrunner", namespace=self.ns)
        if not self.c.expect(got_pod and pods, f"an eventrunner pod exists: {pods}"):
            return
        # Block until the startup banner has been written, then read it once.
        wait_for(lambda: "mock_claude=" in self.k.logs(pods[0], namespace=self.ns,
                                                       tail=60),
                 timeout=120, interval=3)
        text = self.k.logs(pods[0], namespace=self.ns, tail=80)
        self.c.expect_contains(text, "mock_claude=True",
                               "the pod announced mock mode")
        self.c.expect_contains(text, "auto:",
                               "the log explains WHY mock mode was chosen "
                               "(credential auto-detection ran in-cluster)")
        self.c.expect_contains(text, "connected to",
                               "the consumer connected to Kafka (§8.1 retry loop)")

        self.c.section("12. cold-start latency: POST -> first phase=stdout event")
        first = wait_for(lambda: self._first_stdout_seen(), timeout=120, interval=1)
        self.c.expect(first, "a phase=stdout event arrived")
        if first:
            self.c.note("cold start (POST -> first event)",
                        f"{time.monotonic() - posted:.1f}s")

    def _first_stdout_seen(self) -> bool:
        try:
            _, body = get_json(f"{self.base}/v0/agents/{self.corr}/events", timeout=6)
        except (urllib.error.URLError, OSError):
            return False
        return any(e.get("phase") == "stdout" for e in (body or {}).get("events") or [])

    # ---- 8, 9, 10 ----
    def assert_reply(self) -> None:
        self.c.section("8. a final event arrives within 120s")
        events: list[dict] = []

        def final_seen():
            nonlocal events
            try:
                _, body = get_json(f"{self.base}/v0/agents/{self.corr}/events", timeout=6)
            except (urllib.error.URLError, OSError):
                return False
            events = (body or {}).get("events") or []
            return any(e.get("final") for e in events)

        got = wait_for(final_seen, timeout=120, interval=2)
        if not self.c.expect(got, f"final=true received (after {got.elapsed_s:.1f}s)"):
            self.dump_triage()
            return
        for e in events:
            d = e.get("data") or {}
            self.c.log(f"  seq={e.get('sequence')} phase={e.get('phase')} "
                       f"role={d.get('role')} text={(d.get('text') or '')[:60]!r}")

        self.c.section("9. the reply contains MOCK-REPLY")
        self.c.expect_contains(json.dumps(events), "MOCK-REPLY",
                               "the mock assistant reply rode the wire")

        self.c.section("10. events.jsonl and /turns")
        try:
            status, raw = get_bytes(f"{self.base}/v0/agents/{self.corr}/events.jsonl")
            lines = [json.loads(ln) for ln in raw.decode().splitlines() if ln.strip()]
            self.c.expect(status == 200 and lines,
                          f"events.jsonl is non-empty ({len(lines)} lines)")
            self.c.expect(all(e.get("causationid") for e in lines),
                          "every response event carries ce_causationid (§11)")
        except (urllib.error.URLError, OSError, ValueError) as e:
            self.c.fail("events.jsonl served", str(e))
        try:
            _, turns = get_json(f"{self.base}/v0/agents/{self.corr}/turns")
            rows = (turns or {}).get("turns") or []
            self.c.expect(rows and rows[0].get("prompt") == PROMPT,
                          "/turns pairs the prompt with its turn", f"{rows[:1]}")
        except (urllib.error.URLError, OSError) as e:
            self.c.fail("/turns served", str(e))

    # ---- 11 ----
    def assert_scale_back_to_zero(self) -> None:
        """The half that is easy to forget to test. This is what proves
        scale-TO-zero rather than merely scale-FROM-zero, and it is why the test
        overlay sets cooldownPeriod: 30."""
        self.c.section("11. scale back to ZERO after the cooldown")
        so = self.k.get("scaledobject", "eventrunner", namespace=self.ns, missing_ok=True) or {}
        cooldown = int(dig(so, "spec", "cooldownPeriod", default=300))
        budget = cooldown + 90
        self.c.log(f"cooldownPeriod={cooldown}s, waiting up to {budget}s")
        back = wait_for(lambda: self.k.replicas("eventrunner", namespace=self.ns) == 0,
                        timeout=budget, interval=5)
        replicas = self.k.replicas("eventrunner", namespace=self.ns)
        self.c.expect(back, f"eventrunner returned to 0 replicas "
                            f"(after {back.elapsed_s:.0f}s)",
                      f"replicas={replicas} after {budget}s. If lag never reached 0, "
                      f"the offsets were not committed — check that the terminal "
                      f"event was emitted (RQ-1 defers the commit until then).")
        if back:
            self.c.note("scale-to-zero", f"{back.elapsed_s:.0f}s after idle "
                                         f"(cooldown {cooldown}s)")
        inactive = wait_for(
            lambda: condition_is(self.k.get("scaledobject", "eventrunner",
                                            namespace=self.ns, missing_ok=True) or {},
                                 "Active", "False"),
            timeout=90, interval=5)
        so = self.k.get("scaledobject", "eventrunner", namespace=self.ns, missing_ok=True) or {}
        self.c.expect(inactive, "ScaledObject is Active=False again",
                      condition_reason(so, "Active"))

    # ---- §14.3 ----
    def dump_triage(self) -> None:
        self.c.log("")
        self.c.log("── failure triage (§14.3) ──")
        self.c.log(f"$ kubectl -n {self.ns} describe scaledobject eventrunner")
        self.c.log(self.k.describe("scaledobject", "eventrunner", namespace=self.ns)[-2500:])
        pods = self.k.pod_names("app.kubernetes.io/name=eventrunner", namespace=self.ns)
        for p in pods:
            self.c.log(f"$ kubectl -n {self.ns} logs {p} --tail=100")
            self.c.log(self.k.logs(p, namespace=self.ns, tail=100))
        if not pods:
            self.c.log("(no eventrunner pod — a dead consumer thread shows as a "
                       "traceback followed by silence, so check a previous pod)")
        self.c.log("$ kubectl -n keda logs deploy/keda-operator --tail=50")
        self.c.log(self.k.logs("deploy/keda-operator", namespace="keda", tail=50)[-2500:])
        self.c.log(f"$ kubectl -n {self.ns} get events --sort-by=.lastTimestamp | tail -20")
        self.c.log(self.k.events(namespace=self.ns, tail=20))


def build_args(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--namespace", "-n", default=os.environ.get("NS", "kev1"))
    ap.add_argument("--overlay", default="test", choices=("test", "kind", "demo"))
    ap.add_argument("--kubeconfig", default=None,
                    help="kubeconfig file (e.g. .kube/config-kind for a Kind cluster)")
    ap.add_argument("--context", default=None)
    ap.add_argument("--yes", action="store_true", default=True,
                    help="accept the ambient current-context (default for the test)")
    ap.add_argument("--registry", default=os.environ.get("REGISTRY_PREFIX", DEFAULT_REGISTRY))
    ap.add_argument("--tag", default=os.environ.get("TAG", DEFAULT_TAG))
    ap.add_argument("--force-deploy", action="store_true")
    ap.add_argument("--no-deploy", action="store_true")
    ap.add_argument("--keep", action="store_true",
                    help="skip assertion 11's wait and leave the demo running")
    ap.add_argument("--skip", default="6,7",
                    help="preflight checks to skip (default: the two slow pod probes)")
    ap.add_argument("--ingress-port", type=int, default=30080,
                    help="host port the ingress controller is reachable on (Kind)")
    ap.add_argument("--public-url", default=None)
    ap.add_argument("--public-url-timeout", type=float, default=120.0)
    ap.add_argument("--idle-timeout", type=float, default=240.0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-preflight", action="store_true")
    ap.add_argument("--phase", default="pre")
    ap.add_argument("--url", default=None)
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = build_args(argv)
    c = Checks(prefix=PREFIX)
    k = Kubectl(context=args.context, namespace=args.namespace,
                kubeconfig=args.kubeconfig)
    e = E2E(k, c, args)

    c.log(f"context={k.current_context()} namespace={args.namespace} "
          f"overlay={args.overlay}")

    if not args.skip_preflight:
        e.assert_preflight()
        if c.failed:
            c.log("preflight failed — not proceeding")
            return c.summary()
    e.deploy_if_needed()
    e.assert_topics()
    e.assert_public_url()
    if not e.base:
        return c.summary()
    e.assert_idle()
    e.assert_wake()
    if e.corr:
        e.assert_reply()
    if args.keep:
        c.log("--keep: skipping assertion 11 (scale back to zero) and leaving the "
              "demo up")
    else:
        e.assert_scale_back_to_zero()

    if c.failed and e.corr:
        e.dump_triage()
    if e.corr:
        c.note("transcript", f"{e.base}/v0/agents/{e.corr}")
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
