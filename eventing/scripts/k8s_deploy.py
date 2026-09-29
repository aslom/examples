#!/usr/bin/env python3
"""Deploy in the §13 order, with the public URL as a hard gate. Tasks T4.2, T4.3.

  0. Preflight (§12 checks 1-8, 10-12). Abort on any failure.
  1. Namespace.
  2. Topics — in ns `kafka`, NOT the target namespace (Finding 1).
  3. The overlay: EventBridge + Service + Route + EventRunner + ScaledObject.
  4. Publish and VERIFY the public URL (§12 check 9). This is the gate: goal 1 is
     that a human can see the demo, so it is proven before anything else counts.
  5. Verify the idle state — which is ZERO EventRunner replicas. That is correct,
     not a failure.

  python3 scripts/k8s_deploy.py                        # test overlay into kev1
  python3 scripts/k8s_deploy.py --overlay demo         # real claude + credential
  python3 scripts/k8s_deploy.py --check-only           # is a deploy needed?
  python3 scripts/k8s_deploy.py --force-deploy
  python3 scripts/k8s_deploy.py --no-deploy            # fail if not already deployed
  python3 scripts/k8s_deploy.py --namespace kev2

CREDENTIALS (demo overlay). Nothing secret is committed in this repository. The
overlay references a Secret named `anthropic-credentials`; this script creates it
from your environment:

    export ANTHROPIC_BASE_URL=https://ete-litellm.ai-models.vpc.res.ibm.com
    export ANTHROPIC_AUTH_TOKEN=<your-litellm-virtual-key>
    export ANTHROPIC_MODEL=claude-sonnet-4-5-20250929        # optional
    python3 scripts/k8s_deploy.py --overlay demo

Note on that base URL: the `vpc` host is reachable from pods on this cluster; the
`vpc-int` host is NOT (it resolves to IBM-internal 9.x addresses that AWS worker
nodes cannot route). A laptop on the VPN can reach both, so a value that works
locally can still fail in-cluster.

Exit 0 only if every step and check passed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import k8s_preflight  # noqa: E402
from imagelib import DEFAULT_REGISTRY, DEFAULT_TAG  # noqa: E402
from k8slib import Kubectl, condition_is, condition_reason, dig, wait_for  # noqa: E402
from proclib import Checks, die  # noqa: E402

PREFIX = "deploy"
HERE = pathlib.Path(__file__).resolve().parent
K8S = HERE.parent / "k8s"
TOPICS = K8S / "topics" / "kafkatopics.yaml"
KAFKA_NS = "kafka"

DIGEST_ANNOTATION = "rossoctl.dev/manifest-sha256"
SECRET_NAME = "anthropic-credentials"
NTFY_SECRET_NAME = "eventbridge-ntfy"

NTFY_HELP = f"""\
Phone notifications are off unless NTFY_TOPIC is exported:

    export NTFY_TOPIC=<your-ntfy-topic>
    export NTFY_TOKEN=<token>                 # only for a protected topic
    python3 scripts/k8s_deploy.py --yes

It is stored as Secret/{NTFY_SECRET_NAME} and never written to a file, because an
ntfy topic name is a capability: anyone who knows it can read and publish your
notifications.
"""

CREDENTIAL_HELP = f"""\
The demo overlay needs a credential, and none is committed. Export one, then
re-run:

    export ANTHROPIC_BASE_URL=https://ete-litellm.ai-models.vpc.res.ibm.com
    export ANTHROPIC_AUTH_TOKEN=<your-litellm-virtual-key>
    export ANTHROPIC_MODEL=claude-sonnet-4-5-20250929      # optional
    python3 scripts/k8s_deploy.py --overlay demo

This script applies them as Secret/{SECRET_NAME} via stdin — deliberately not
`kubectl create secret --from-literal=`, which would put the token in the process
argv where `ps` can read it.

To run the free, deterministic path instead, use the test overlay (mock mode, no
credential): python3 scripts/k8s_deploy.py --overlay test
"""


def overlay_path(name: str) -> pathlib.Path:
    return K8S / "overlays" / name


# ---- §14.1 change detection -------------------------------------------------

def manifest_digest(k: Kubectl, overlay: pathlib.Path) -> str:
    return hashlib.sha256(k.kustomize(str(overlay)).encode()).hexdigest()


def stamped_digest(k: Kubectl, ns: str) -> str:
    obj = k.get("namespace", ns, namespace=False, missing_ok=True)
    if obj is None:
        return ""
    return dig(obj, "metadata", "annotations", DIGEST_ANNOTATION, default="")


def deploy_needed(k: Kubectl, c: Checks, overlay: pathlib.Path, ns: str) -> tuple[bool, str]:
    """(needed, reason). Two mechanisms, used together (§14.1).

    The digest is the cheap gate: one API call, and a definite "identical
    manifests" answer. `kubectl diff` is the AUTHORITATIVE one, because it also
    catches cluster-side drift — someone hand-editing a Deployment — which a file
    hash cannot see.
    """
    if k.get("namespace", ns, namespace=False, missing_ok=True) is None:
        return True, f"namespace {ns} does not exist yet"
    try:
        digest = manifest_digest(k, overlay)
    except RuntimeError as e:
        return True, f"could not render the overlay ({e})"
    stamped = stamped_digest(k, ns)
    if stamped != digest:
        return True, (f"manifest digest changed "
                      f"({(stamped or 'unstamped')[:12]} -> {digest[:12]})")
    rc, text = k.diff_kustomize(str(overlay))
    if rc == 0:
        return False, f"digest matches ({digest[:12]}) and no cluster-side drift"
    if rc == 1:
        head = "\n".join(text.splitlines()[:20])
        return True, f"cluster-side drift detected by kubectl diff:\n{head}"
    return True, f"kubectl diff errored (rc={rc}): {text[:300]}"


# ---- credentials ------------------------------------------------------------

def ensure_credentials_secret(k: Kubectl, c: Checks, ns: str, *, dry_run: bool) -> bool:
    """Create/replace Secret/anthropic-credentials from the environment.

    The value is passed on stdin and never appears in argv, and only its length
    and a short prefix are ever logged.
    """
    token = (os.environ.get("ANTHROPIC_AUTH_TOKEN")
             or os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not token:
        c.fail("a credential is available for the demo overlay", CREDENTIAL_HELP)
        return False

    string_data: dict[str, str] = {}
    if os.environ.get("ANTHROPIC_AUTH_TOKEN", "").strip():
        string_data["ANTHROPIC_AUTH_TOKEN"] = os.environ["ANTHROPIC_AUTH_TOKEN"].strip()
    if os.environ.get("ANTHROPIC_API_KEY", "").strip():
        string_data["ANTHROPIC_API_KEY"] = os.environ["ANTHROPIC_API_KEY"].strip()
    for optional in ("ANTHROPIC_BASE_URL", "ANTHROPIC_MODEL"):
        val = os.environ.get(optional, "").strip()
        if val:
            string_data[optional] = val

    manifest = {
        "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
        "metadata": {"name": SECRET_NAME, "namespace": ns,
                     "labels": {"app.kubernetes.io/part-of": "rossoctl-eventing"}},
        "stringData": string_data,
    }
    shown = {kk: (f"{vv[:4]}…({len(vv)} chars)" if "TOKEN" in kk or "KEY" in kk else vv)
             for kk, vv in string_data.items()}
    c.log(f"Secret/{SECRET_NAME} keys: {json.dumps(shown)}")
    if "ANTHROPIC_BASE_URL" in string_data and \
            "vpc-int" in string_data["ANTHROPIC_BASE_URL"]:
        c.warn("ANTHROPIC_BASE_URL points at a *vpc-int* host. Those resolve to "
               "IBM-internal 9.x addresses which this cluster's AWS worker nodes "
               "cannot route — it works from your laptop and times out in a pod. "
               "Use the vpc (public) host instead.")
    if dry_run:
        c.log("--dry-run: the Secret was not applied")
        return True
    res = k.apply_stdin(json.dumps(manifest), namespace=ns)
    return c.expect_run(res, f"Secret/{SECRET_NAME} applied from the environment")


def ensure_ntfy_secret(k: Kubectl, c: Checks, ns: str, *, dry_run: bool) -> None:
    """Create/replace Secret/eventbridge-ntfy from NTFY_TOPIC in the environment.

    An ntfy topic name is a capability, not a label: anyone who learns it can read
    every notification AND publish to it. So it is treated exactly like the API
    credential — never committed, applied via stdin so it stays out of `ps`, and
    logged only as a prefix and a length.

    Absent NTFY_TOPIC this is a no-op and whatever is already deployed is left
    alone, so a redeploy does not silently switch notifications off.
    """
    topic = os.environ.get("NTFY_TOPIC", "").strip()
    if not topic:
        existing = k.get("secret", NTFY_SECRET_NAME, namespace=ns, missing_ok=True)
        if existing is not None:
            c.log(f"NTFY_TOPIC not set; leaving the existing "
                  f"Secret/{NTFY_SECRET_NAME} in place")
        return

    data = {"NTFY_ENABLED": "true", "NTFY_TOPIC": topic}
    for optional in ("NTFY_TOKEN", "NTFY_BASE_URL"):
        val = os.environ.get(optional, "").strip()
        if val:
            data[optional] = val
    manifest = {
        "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
        "metadata": {"name": NTFY_SECRET_NAME, "namespace": ns,
                     "labels": {"app.kubernetes.io/part-of": "rossoctl-eventing"}},
        "stringData": data,
    }
    c.log(f"Secret/{NTFY_SECRET_NAME}: NTFY_TOPIC={topic[:4]}…({len(topic)} chars)"
          + (" +NTFY_TOKEN" if "NTFY_TOKEN" in data else ""))
    if dry_run:
        c.log("--dry-run: the ntfy Secret was not applied")
        return
    c.expect_run(k.apply_stdin(json.dumps(manifest), namespace=ns),
                 f"Secret/{NTFY_SECRET_NAME} applied from the environment")


# ---- the steps --------------------------------------------------------------

class Deployer:
    def __init__(self, k: Kubectl, c: Checks, args) -> None:
        self.k = k
        self.c = c
        self.args = args
        self.ns = args.namespace
        self.overlay = overlay_path(args.overlay)

    def step1_namespace(self) -> None:
        self.c.section("step 1: namespace")
        res = self.k.ensure_namespace(self.ns)
        self.c.expect_run(res, f"namespace {self.ns} exists")

    def step2_topics(self) -> None:
        """In ns kafka, before the app. §8.1's retry loop mitigates the dependency
        but does not remove it, and KEDA needs the topic to compute lag at all."""
        self.c.section("step 2: topics (in ns kafka, not the target namespace)")
        # §5 says the namespace is configurable, but topics/kafkatopics.yaml names
        # its two topics literally (it cannot be templated: it targets ns kafka and
        # so cannot take part in an overlay's namespace rewrite). Catch the mismatch
        # here, because the alternative is a 180s wait for a topic nothing created.
        wanted = f"{self.ns}-requests"
        if wanted not in TOPICS.read_text():
            self.c.fail(
                f"{TOPICS.name} declares the topics for namespace {self.ns!r}",
                f"it does not contain {wanted!r}. The KafkaTopic names are literal "
                f"because that file targets ns kafka and cannot participate in the "
                f"overlay's namespace rewrite. Either deploy into kev1, or copy the "
                f"file and change both topic names to {self.ns}-requests / "
                f"{self.ns}-responses (and the ScaledObject trigger + ConfigMap to "
                f"match).")
            return
        res = self.k.apply_file(str(TOPICS), namespace=False)
        if not self.c.expect_run(res, "KafkaTopics applied"):
            return
        for topic in (f"{self.ns}-requests", f"{self.ns}-responses"):
            ready = wait_for(
                lambda t=topic: condition_is(
                    self.k.get("kafkatopic", t, namespace=KAFKA_NS, missing_ok=True) or {},
                    "Ready"),
                timeout=180, interval=3)
            live = self.k.get("kafkatopic", topic, namespace=KAFKA_NS, missing_ok=True) or {}
            self.c.expect(ready, f"KafkaTopic {topic} is Ready "
                                 f"(after {ready.elapsed_s:.0f}s)",
                          f"{condition_reason(live, 'Ready')}\n"
                          f"  If there is no condition at all, Strimzi is not watching "
                          f"ns {KAFKA_NS} — see preflight check 3.")

    def step3_apply_overlay(self) -> None:
        """Applies EventBridge, its Service and Route, EventRunner and the
        ScaledObject together.

        §13 wants EventBridge proven reachable before EventRunner starts, and that
        holds here without a second apply: minReplicaCount is 0 and a fresh topic
        has no lag, so the EventRunner Deployment sits at zero replicas until a
        request is posted — which cannot happen until the URL works.
        """
        self.c.section(f"step 3: apply the {self.args.overlay} overlay")
        if self.args.overlay == "demo":
            if not ensure_credentials_secret(self.k, self.c, self.ns,
                                             dry_run=self.args.dry_run):
                return
        # Any overlay may want phone notifications; the Secret is optional in the
        # manifest so this is safe when NTFY_TOPIC is unset.
        ensure_ntfy_secret(self.k, self.c, self.ns, dry_run=self.args.dry_run)
        res = self.k.apply_kustomize(str(self.overlay), server_dry_run=self.args.dry_run)
        if not self.c.expect_run(res, f"overlay {self.args.overlay} applied"):
            self.c.log(res.tail(15))
            return
        for line in (res.out or "").splitlines():
            self.c.log(f"  {line}")
        if self.args.dry_run:
            return
        self.c.expect_run(self.k.rollout_status("deploy/eventbridge",
                                               namespace=self.ns, timeout=240),
                          "deploy/eventbridge rolled out")

    def step4_publish_url(self) -> str:
        """Set EVENT_BRIDGE_PUBLIC_BASE_URL to the Route host, then PROVE it.

        `kubectl set env` rather than a manifest field on purpose: the hostname
        does not exist until the Route does, and the pod cannot discover it (§8.7 —
        no Kubernetes client, by §1.1). Because the ConfigMap never declares this
        key, `kubectl apply` merges rather than reverts it, so §14.1's diff-based
        change detection stays clean. That is verified by --check-only after a
        deploy.
        """
        self.c.section("step 4: publish and verify the public URL (the gate)")
        pf = k8s_preflight.Preflight(self.k, self.c, self.args)
        url = self.args.public_url or pf.route_url()
        if not url:
            self.c.fail("a public URL is available",
                        f"no Route/eventbridge in ns {self.ns}. On a cluster without "
                        f"Routes, use k8s/overlays/ingress-example.yaml and pass "
                        f"--public-url.")
            return ""
        self.c.log(f"public base URL: {url}")
        res = self.k.set_env("deploy/eventbridge",
                             f"EVENT_BRIDGE_PUBLIC_BASE_URL={url}", namespace=self.ns)
        if not self.c.expect_run(res, "EVENT_BRIDGE_PUBLIC_BASE_URL set on the Deployment"):
            return url
        self.c.expect_run(self.k.rollout_status("deploy/eventbridge",
                                               namespace=self.ns, timeout=240),
                          "eventbridge rolled out with the public URL")
        if not pf.check_public_url(url):
            # Hard gate: discovering an ingress problem after a full deploy wastes
            # the whole cycle, which is the entire reason goal 1 is goal 1.
            self.c.log("  the public URL is the gate — later steps are still run so "
                       "you get the full picture, but this run has failed")
        return url

    def step5_verify_idle(self) -> None:
        """The correct idle state is ZERO EventRunner replicas."""
        self.c.section("step 5: verify the idle state (0 replicas is correct)")
        # An earlier teardown may have pinned the ScaledObject at zero (§17).
        self.unpause()
        ready = wait_for(
            lambda: condition_is(self.k.get("scaledobject", "eventrunner",
                                            namespace=self.ns, missing_ok=True) or {},
                                 "Ready"),
            timeout=180, interval=3)
        so = self.k.get("scaledobject", "eventrunner", namespace=self.ns, missing_ok=True) or {}
        self.c.expect(ready, "ScaledObject/eventrunner is Ready",
                      f"{condition_reason(so, 'Ready')}\n"
                      f"  Trigger errors live in these conditions; also check "
                      f"`kubectl -n keda logs deploy/keda-operator`.")
        self.c.expect(condition_is(so, "Paused", "False") or not condition_is(so, "Paused"),
                      "ScaledObject is not paused",
                      condition_reason(so, "Paused"))
        zero = wait_for(lambda: self.k.replicas("eventrunner", namespace=self.ns) == 0,
                        timeout=120, interval=3)
        replicas = self.k.replicas("eventrunner", namespace=self.ns)
        self.c.expect(zero, "eventrunner is idle at 0 replicas (scale-to-zero works)",
                      f"replicas={replicas}; with an empty topic and "
                      f"offsetResetPolicy=earliest this should be 0. Retained "
                      f"messages in the topic legitimately count as lag and would "
                      f"scale it up — see §7.1.")

    def unpause(self) -> None:
        """Clear the §17 teardown annotations so a redeploy actually scales again."""
        res = self.k.annotate("scaledobject", "eventrunner",
                              "autoscaling.keda.sh/paused-replicas-",
                              "autoscaling.keda.sh/paused-",
                              namespace=self.ns)
        if res.ok and "annotated" in (res.out or ""):
            self.c.log("  cleared a previous teardown's pause annotations")

    def stamp_digest(self) -> None:
        try:
            digest = manifest_digest(self.k, self.overlay)
        except RuntimeError:
            return
        self.k.annotate("namespace", self.ns, f"{DIGEST_ANNOTATION}={digest}",
                        namespace=False)
        self.c.log(f"stamped {DIGEST_ANNOTATION}={digest[:12]} on ns/{self.ns}")


def build_args(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--namespace", "-n", default=os.environ.get("NS", "kev1"))
    ap.add_argument("--overlay", default="test", choices=("test", "kind", "demo"))
    ap.add_argument("--kubeconfig", default=None,
                    help="kubeconfig file (e.g. .kube/config-kind for a Kind cluster)")
    ap.add_argument("--context", default=None)
    ap.add_argument("--yes", action="store_true",
                    help="accept the ambient current-context without pinning it")
    ap.add_argument("--registry", default=os.environ.get("REGISTRY_PREFIX", DEFAULT_REGISTRY))
    ap.add_argument("--tag", default=os.environ.get("TAG", DEFAULT_TAG))
    ap.add_argument("--force-deploy", action="store_true",
                    help="apply even when nothing changed")
    ap.add_argument("--no-deploy", action="store_true",
                    help="fail instead of deploying if a deploy is needed")
    ap.add_argument("--check-only", action="store_true",
                    help="report whether a deploy is needed, change nothing")
    ap.add_argument("--skip-preflight", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="server dry-run the overlay; mutate nothing")
    ap.add_argument("--ingress-port", type=int, default=30080,
                    help="host port the ingress controller is reachable on (Kind)")
    ap.add_argument("--public-url", default=None,
                    help="use this URL instead of the Route host (Ingress clusters)")
    ap.add_argument("--public-url-timeout", type=float, default=180.0)
    ap.add_argument("--skip", default="", help="preflight check numbers to skip")
    ap.add_argument("--phase", default="pre")     # consumed by Preflight
    ap.add_argument("--url", default=None)        # consumed by Preflight
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = build_args(argv)
    c = Checks(prefix=PREFIX)
    k = Kubectl(context=args.context, namespace=args.namespace,
                kubeconfig=args.kubeconfig)
    d = Deployer(k, c, args)

    if not overlay_path(args.overlay).is_dir():
        die(f"no overlay at {overlay_path(args.overlay)}", prefix=PREFIX)

    needed, reason = deploy_needed(k, c, d.overlay, args.namespace)
    c.log(f"change detection: deploy {'NEEDED' if needed else 'not needed'} — {reason}")

    if args.check_only:
        c.note("deploy_needed", needed)
        return c.summary(allow_empty=True)

    if not needed and not args.force_deploy:
        c.ok("manifests are already applied and unchanged — skipping the deploy")
        c.log("  pass --force-deploy to apply anyway")
        d.step5_verify_idle()
        return c.summary()

    if args.no_deploy:
        c.fail("the cluster is already up to date (--no-deploy)", reason)
        return c.summary()

    if not args.skip_preflight:
        c.section("step 0: preflight (§12 checks 1-8, 10-12)")
        skip = {int(s) for s in args.skip.split(",") if s.strip()}
        k8s_preflight.run_pre(k8s_preflight.Preflight(k, c, args), skip)
        if c.failed:
            c.log("preflight failed — refusing to deploy. Fix the above, or re-run "
                  "with --skip-preflight if you know better.")
            return c.summary()

    d.step1_namespace()
    d.step2_topics()
    d.step3_apply_overlay()
    if args.dry_run:
        c.log("--dry-run: stopping before the public-URL gate")
        return c.summary()
    url = d.step4_publish_url()
    d.step5_verify_idle()
    if not c.failed:
        d.stamp_digest()

    if url:
        c.note("public URL", url)
        c.note("watch scaling", f"kubectl -n {args.namespace} get deploy eventrunner -w")
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
