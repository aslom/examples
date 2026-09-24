#!/usr/bin/env python3
"""Pre-deployment checks. DESIGN_PHASE1.md §12, task T4.1.

Every check corresponds to a failure that is slow or confusing to diagnose later,
so every one fails loudly WITH the remediation. Goal 1 (§2) — a reachable public
URL — is why checks 8 and 9 exist before and around the deploy rather than after.

The checks split into two phases because check 9 can only run once EventBridge is
up:

    --phase pre     checks 1-8 and 10-12, before anything is applied
    --phase public  check 9 only, the gate between deploy steps 4 and 5
    --phase all     both (default when run standalone against a live deploy)

  python3 scripts/k8s_preflight.py
  python3 scripts/k8s_preflight.py --namespace kev2 --overlay demo
  python3 scripts/k8s_preflight.py --skip 6,7        # skip the slow pod probes
  python3 scripts/k8s_preflight.py --phase public

Exit 0 only if every check that ran passed.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import socket
import sys
import urllib.error
import urllib.request

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from imagelib import DEFAULT_REGISTRY, DEFAULT_TAG, Images, manifest_platforms, registry_manifest  # noqa: E402
from k8slib import Kubectl, condition_is, condition_reason, dig, wait_for  # noqa: E402
from proclib import Checks, die  # noqa: E402

PREFIX = "preflight"
HERE = pathlib.Path(__file__).resolve().parent
K8S = HERE.parent / "k8s"

# The namespace holding the shared Strimzi operator and Kafka cluster.
KAFKA_NS = "kafka"
KAFKA_CLUSTER = "my-cluster"
BOOTSTRAP = f"{KAFKA_CLUSTER}-kafka-bootstrap.{KAFKA_NS}.svc:9092"

# The probe that caught Finding 3 (arbitrary-UID writability), reproduced exactly.
UID_PROBE = r"""
import os, pathlib, sys
print(f"uid={os.getuid()} gid={os.getgid()} groups={os.getgroups()} "
      f"HOME={os.environ.get('HOME')} TMPDIR={os.environ.get('TMPDIR')}")
for label in ("TMPDIR", "HOME"):
    d = os.environ.get(label)
    if not d:
        print(f"{label} is unset"); sys.exit(1)
    p = pathlib.Path(d, ".rossoctl-uid-probe")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("ok"); p.unlink()
    print(f"write {label} {d} OK")
import eventrunner.config as c
f = c.load()
print(f"FIX-VERIFIED mock={f.mock_claude}")
# A CA bundle is required for any outbound HTTPS (ntfy fan-out, the selftest's
# public-tunnel probes). debian:*-slim ships none, and without it every HTTPS
# request dies with CERTIFICATE_VERIFY_FAILED — silently, at runtime.
import ssl
print("CA-BUNDLE", ssl.get_default_verify_paths().cafile or "MISSING")
"""


class Preflight:
    def __init__(self, k: Kubectl, c: Checks, args) -> None:
        self.k = k
        self.c = c
        self.args = args
        self.ns = args.namespace
        self.images = Images(args.registry, args.tag)
        self.request_topic = f"{self.ns}-requests"
        self.response_topic = f"{self.ns}-responses"
        self.consumer_group = f"{self.ns}-eventrunner"
        self.node_arches: set[str] = set()

    # ---- 1 --------------------------------------------------------------
    def check_cluster(self) -> None:
        """Deploying into the wrong cluster is the worst outcome available here,
        and it is silent — so the context is printed and must be acknowledged."""
        ctx = self.k.current_context() or "(none)"
        self.c.log(f"target context: {ctx}")
        self.c.log(f"target namespace: {self.ns}")
        ver = self.k.call(["version", "-o", "json"], namespace=False, timeout=60)
        if not self.c.expect_run(ver, "cluster is reachable"):
            die("cannot reach the cluster — check your kubeconfig / oc login",
                prefix=PREFIX)
        try:
            v = json.loads(ver.out)
            self.c.log(f"server: {dig(v, 'serverVersion', 'gitVersion', default='?')}")
        except (ValueError, KeyError):
            pass
        if not (self.args.context or self.args.yes):
            self.c.fail(
                "the target cluster was explicitly confirmed",
                f"refusing to act on the ambient current-context.\n"
                f"  Re-run with --context {ctx!r} to pin it, or --yes to accept it.")
        else:
            self.c.ok("the target cluster was explicitly confirmed")

    # ---- 2 --------------------------------------------------------------
    def check_crds(self) -> None:
        versions = self.k.crd_served_versions("kafkatopics.kafka.strimzi.io")
        self.c.expect("v1" in versions,
                      "KafkaTopic CRD serves kafka.strimzi.io/v1",
                      f"served versions: {versions or 'CRD absent'}. Strimzi 1.0.x "
                      f"serves ONLY v1; a v1beta2 manifest fails outright.")
        so = self.k.crd_served_versions("scaledobjects.keda.sh")
        self.c.expect(bool(so), "ScaledObject CRD is present",
                      "KEDA is not installed in this cluster")

    # ---- 3 --------------------------------------------------------------
    def check_strimzi_watch_scope(self) -> None:
        """Finding 1. STRIMZI_NAMESPACE is a downward-API fieldRef to the
        operator's own namespace, so it watches ONLY that namespace. A KafkaTopic
        created anywhere else is accepted by the API server and then silently
        ignored — no reconcile, no event, no error."""
        dep = self.k.get("deploy", "strimzi-cluster-operator",
                         namespace=KAFKA_NS, missing_ok=True)
        if dep is None:
            self.c.fail("the Strimzi cluster operator was found",
                        f"no deploy/strimzi-cluster-operator in ns {KAFKA_NS}")
            return
        watched: str | None = None
        for container in dig(dep, "spec", "template", "spec", "containers", default=[]):
            for env in container.get("env") or []:
                if env.get("name") != "STRIMZI_NAMESPACE":
                    continue
                if env.get("value"):
                    watched = env["value"]
                else:
                    fp = dig(env, "valueFrom", "fieldRef", "fieldPath", default="")
                    # A fieldRef to metadata.namespace means "my own namespace".
                    watched = KAFKA_NS if fp == "metadata.namespace" else f"<{fp}>"
        self.c.expect(watched == KAFKA_NS,
                      f"Strimzi watches ns {KAFKA_NS}, where the topics are created",
                      f"STRIMZI_NAMESPACE resolves to {watched!r}; KafkaTopics in "
                      f"{KAFKA_NS} would be ignored silently")
        kafka = self.k.get("kafka", KAFKA_CLUSTER, namespace=KAFKA_NS, missing_ok=True)
        if kafka is None:
            self.c.fail(f"Kafka cluster {KAFKA_CLUSTER} exists in ns {KAFKA_NS}", "not found")
        else:
            self.c.expect(condition_is(kafka, "Ready"),
                          f"Kafka/{KAFKA_CLUSTER} is Ready",
                          condition_reason(kafka, "Ready"))
            # Not a failure: this cluster uses ephemeral storage on purpose, and
            # the CR reports Warning=True forever because of it.
            if condition_is(kafka, "Warning"):
                self.c.warn(f"Kafka/{KAFKA_CLUSTER} reports "
                            f"{condition_reason(kafka, 'Warning')[:90]} — expected on "
                            f"an ephemeral single-node test cluster; a restart loses "
                            f"all topic data")

    # ---- 4 + 5 ----------------------------------------------------------
    def check_images(self) -> None:
        """Finding 2. Visibility is a registry setting a human can flip back, and
        the failure mode is a slow, confusing ImagePullBackOff deep into a deploy.
        So this is an UNAUTHENTICATED manifest request — exactly what the kubelet
        does with no pull secret — and it hard-fails. There is no pull-secret
        fallback path: if it fails, make the repositories public again."""
        self.node_arches = self.k.node_architectures()
        self.c.expect(bool(self.node_arches),
                      f"node architectures read: {sorted(self.node_arches)}")
        want = {f"linux/{a}" for a in self.node_arches}
        refs = self.images.all(with_claude=(self.args.overlay == "demo"))
        for ref in refs:
            status, body, err = registry_manifest(ref)
            if not self.c.expect(status == 200,
                                 f"{ref} is anonymously pullable (HTTP 200)",
                                 f"status={status} {err}\n"
                                 f"  Make the repository public again; there is no "
                                 f"pull-secret fallback by design."):
                continue
            plats = manifest_platforms(body)
            if not plats:
                # A single manifest rather than a list: cannot confirm the arch
                # from the index, so say so instead of pretending.
                self.c.warn(f"{ref} is a single-platform manifest — cannot verify it "
                            f"matches {sorted(want)} without pulling it")
                continue
            self.c.expect(want <= plats,
                          f"{ref} covers the cluster's architecture {sorted(want)}",
                          f"manifest has {sorted(plats)}; an arch mismatch surfaces "
                          f"at runtime as `exec format error`")

    # ---- 6 --------------------------------------------------------------
    def check_arbitrary_uid(self) -> None:
        """Finding 3, re-run as the exact probe that caught it. OpenShift assigns
        an arbitrary UID from the namespace range and overrides the image's USER;
        the process always gets gid 0, so every writable path must be group-0
        owned and group-writable. Deliberately mounts NO volume — a mounted volume
        masks the problem."""
        # On a cluster where the namespace already exists (the reference cluster)
        # this is a no-op; on a fresh Kind cluster the namespace does not exist yet
        # and the probe pod would fail with `namespaces "kev1" not found`. Creating
        # it here is safe — deploy step 1 does exactly the same, idempotently.
        if self.k.get("namespace", self.ns, namespace=False, missing_ok=True) is None:
            self.c.log(f"creating namespace {self.ns} for the probe")
            if not self.c.expect_run(self.k.ensure_namespace(self.ns),
                                     f"namespace {self.ns} created for the probe"):
                return
        name = "rossoctl-preflight-uid"
        self.k.delete("pod", name, namespace=self.ns)
        pod = {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": self.ns,
                         "labels": {"app.kubernetes.io/part-of": "rossoctl-eventing"}},
            "spec": {
                "restartPolicy": "Never",
                "securityContext": {"runAsNonRoot": True,
                                    "seccompProfile": {"type": "RuntimeDefault"}},
                "containers": [{
                    "name": "probe",
                    "image": self.images.eventrunner,
                    "command": ["python", "-c", UID_PROBE],
                    "securityContext": {"allowPrivilegeEscalation": False,
                                        "capabilities": {"drop": ["ALL"]}},
                }],
            },
        }
        res = self.k.apply_stdin(json.dumps(pod), namespace=self.ns)
        if not self.c.expect_run(res, "arbitrary-UID probe pod created"):
            return
        try:
            out = wait_for(
                lambda: dig(self.k.get("pod", name, namespace=self.ns, missing_ok=True) or {},
                            "status", "phase", default="") in ("Succeeded", "Failed"),
                timeout=180, interval=3)
            phase = dig(self.k.get("pod", name, namespace=self.ns, missing_ok=True) or {},
                        "status", "phase", default="?")
            logs = self.k.logs(name, namespace=self.ns, tail=40)
            if not self.c.expect(out and phase == "Succeeded",
                                 "the image runs under an SCC-assigned arbitrary UID "
                                 "with NO volume mounted",
                                 f"phase={phase}\n{logs}\n"
                                 f"{self.k.describe('pod', name, namespace=self.ns)[-800:]}"):
                return
            for line in logs.splitlines():
                if line.startswith("uid="):
                    self.c.log(f"  {line}")
            self.c.expect_contains(logs, "write TMPDIR", "the probe wrote $TMPDIR")
            self.c.expect_contains(logs, "write HOME", "the probe wrote $HOME")
            self.c.expect_contains(logs, "FIX-VERIFIED",
                                   "the image can load its own config")
            self.c.expect("CA-BUNDLE MISSING" not in logs,
                          "the image has a CA bundle for outbound HTTPS",
                          "ssl.get_default_verify_paths().cafile is None — every "
                          "HTTPS request (ntfy, selftest probes) will fail with "
                          "CERTIFICATE_VERIFY_FAILED. Install ca-certificates.")
        finally:
            self.k.delete("pod", name, namespace=self.ns)

    # ---- 7 --------------------------------------------------------------
    def check_keda_reaches_kafka(self) -> None:
        """Resolves RQ-3, re-measured at deploy time because it can change.

        A numeric lag value from the external metrics API can only come from KEDA
        having connected to the broker in ns kafka from ns keda and queried the
        topic. A broken scaler means silent no-scaling.

        Uses a throwaway Deployment as the scale target: KEDA's admission webhook
        rejects a second ScaledObject pointing at an existing one.
        """
        probe_topic, temporary = self._probe_topic()
        if probe_topic is None:
            self.c.fail("a topic was available to probe KEDA's Kafka reachability",
                        f"no Ready KafkaTopic in ns {KAFKA_NS}, and one could not be "
                        f"created — check the Strimzi operator is reconciling")
            return
        dep_name = so_name = "rossoctl-preflight-scaler"
        self.k.delete("scaledobject", so_name, namespace=self.ns)
        self.k.delete("deploy", dep_name, namespace=self.ns)
        target = {
            "apiVersion": "apps/v1", "kind": "Deployment",
            "metadata": {"name": dep_name, "namespace": self.ns},
            "spec": {"replicas": 0,
                     "selector": {"matchLabels": {"app": dep_name}},
                     "template": {"metadata": {"labels": {"app": dep_name}},
                                  "spec": {"containers": [
                                      {"name": "pause",
                                       "image": self.images.eventrunner,
                                       "command": ["sleep", "3600"]}]}}},
        }
        so = {
            "apiVersion": "keda.sh/v1alpha1", "kind": "ScaledObject",
            "metadata": {"name": so_name, "namespace": self.ns},
            "spec": {"scaleTargetRef": {"name": dep_name},
                     "minReplicaCount": 0, "maxReplicaCount": 1,
                     "pollingInterval": 5,
                     "triggers": [{"type": "kafka", "metadata": {
                         "bootstrapServers": BOOTSTRAP,
                         "consumerGroup": "rossoctl-preflight-probe",
                         "topic": probe_topic,
                         "lagThreshold": "1",
                         "offsetResetPolicy": "earliest"}}]},
        }
        try:
            if not self.c.expect_run(self.k.apply_stdin(json.dumps(target), namespace=self.ns),
                                     "probe scale target created"):
                return
            if not self.c.expect_run(self.k.apply_stdin(json.dumps(so), namespace=self.ns),
                                     "probe ScaledObject accepted by the KEDA webhook"):
                return
            ready = wait_for(
                lambda: condition_is(self.k.get("scaledobject", so_name,
                                                namespace=self.ns, missing_ok=True) or {},
                                     "Ready"),
                timeout=120, interval=3)
            live = self.k.get("scaledobject", so_name, namespace=self.ns, missing_ok=True) or {}
            if not self.c.expect(ready,
                                 f"KEDA reaches Kafka at {BOOTSTRAP} (ScaledObject Ready)",
                                 f"Ready={condition_reason(live, 'Ready')}\n"
                                 f"  Cross-namespace DNS or a NetworkPolicy is blocking "
                                 f"the scaler."):
                return
            metric = f"s0-kafka-{probe_topic}"
            path = (f"/apis/external.metrics.k8s.io/v1beta1/namespaces/{self.ns}/"
                    f"{metric}?labelSelector=scaledobject.keda.sh%2Fname%3D{so_name}")
            got = wait_for(lambda: self._metric_value(path) is not None,
                           timeout=90, interval=5)
            value = self._metric_value(path)
            self.c.expect(got and value is not None,
                          f"the external metrics API returns a numeric lag "
                          f"({metric} = {value})",
                          "no numeric value — KEDA's metrics adapter is not serving "
                          "this trigger")
        finally:
            self.k.delete("scaledobject", so_name, namespace=self.ns)
            self.k.delete("deploy", dep_name, namespace=self.ns)
            if temporary:
                self.k.delete("kafkatopic", probe_topic, namespace=KAFKA_NS)

    def _metric_value(self, path: str):
        try:
            body = self.k.raw(path)
        except RuntimeError:
            return None
        for item in (body or {}).get("items") or []:
            v = item.get("value")
            if v is None:
                continue
            try:
                return int(str(v).rstrip("m") or 0)
            except ValueError:
                return None
        return None

    def _probe_topic(self) -> tuple[str | None, bool]:
        """A topic to point the probe trigger at. Returns (name, we_created_it).

        Prefers our own request topic, then any Ready topic, and finally creates a
        throwaway one. The last case is what a brand-new cluster needs: with no
        topic at all the check could only be skipped, and skipping the one check
        that proves KEDA can reach Kafka is the wrong trade — a broken scaler means
        silent no-scaling.
        """
        topics = self.k.items("kafkatopic", namespace=KAFKA_NS)
        ready = [t for t in topics if condition_is(t, "Ready")]
        for t in ready:
            if dig(t, "metadata", "name") == self.request_topic:
                return self.request_topic, False
        if ready:
            return dig(ready[0], "metadata", "name"), False

        name = "rossoctl-preflight-probe"
        self.c.log(f"no Ready KafkaTopic in ns {KAFKA_NS} — creating {name} for the probe")
        topic = {
            "apiVersion": "kafka.strimzi.io/v1", "kind": "KafkaTopic",
            "metadata": {"name": name, "namespace": KAFKA_NS,
                         "labels": {"strimzi.io/cluster": KAFKA_CLUSTER,
                                    "app.kubernetes.io/part-of": "rossoctl-eventing"}},
            "spec": {"partitions": 1, "replicas": 1},
        }
        if not self.k.apply_stdin(json.dumps(topic), namespace=KAFKA_NS).ok:
            return None, False
        ok = wait_for(lambda: condition_is(
            self.k.get("kafkatopic", name, namespace=KAFKA_NS, missing_ok=True) or {},
            "Ready"), timeout=120, interval=3)
        if not ok:
            self.k.delete("kafkatopic", name, namespace=KAFKA_NS)
            return None, False
        return name, True

    # ---- 8 --------------------------------------------------------------
    def check_ingress_capability(self) -> None:
        """Goal 1: fail before deploying, not after. An ingress problem discovered
        at the end wastes the whole cycle."""
        # Aggregated API, not a CRD — see Kubectl.api_group_serves.
        has_route = self.k.api_group_serves("route.openshift.io/v1", "Route")
        classes = [dig(c, "metadata", "name") for c in self.k.items("ingressclass",
                                                                   namespace=False)]
        self.c.expect(has_route or classes,
                      f"the cluster can serve ingress (Route CRD={has_route}, "
                      f"IngressClasses={classes})",
                      "no Route CRD and no IngressClass — nothing can expose "
                      "EventBridge publicly")
        domain = self._apps_domain()
        if not domain:
            self.c.warn("could not read the cluster apps domain "
                        "(ingresses.config.openshift.io/cluster) — skipping the "
                        "wildcard DNS check")
            return
        self.c.log(f"  apps domain: {domain}")
        host = f"eventbridge-{self.ns}.{domain}"
        try:
            addrs = {a[4][0] for a in socket.getaddrinfo(host, 443, proto=socket.IPPROTO_TCP)}
            self.c.ok(f"wildcard DNS resolves {host} -> {sorted(addrs)}")
        except socket.gaierror as e:
            self.c.fail(f"wildcard DNS resolves {host}",
                        f"{e}\n  The Route will be created but nothing will reach it.")

    def _apps_domain(self) -> str:
        obj = self.k.get("ingresses.config.openshift.io", "cluster",
                         namespace=False, missing_ok=True)
        return dig(obj or {}, "spec", "domain", default="")

    # ---- 9 (own phase) --------------------------------------------------
    def check_public_url(self, url: str | None = None) -> bool:
        """The gate between deploy steps 4 and 5. Goal 1, proven not assumed:
        `GET https://<route>/healthz` from OUTSIDE the cluster must return 200."""
        base = url or self.route_url()
        if not base:
            self.c.fail("the public URL is reachable from outside the cluster",
                        f"no Route/eventbridge in ns {self.ns} yet")
            return False
        target = f"{base.rstrip('/')}/healthz"

        def probe():
            try:
                with urllib.request.urlopen(target, timeout=5) as r:
                    return r.status == 200
            except (urllib.error.URLError, OSError):
                return False

        out = wait_for(probe, timeout=self.args.public_url_timeout, interval=3)
        return self.c.expect(
            out, f"public URL answers 200: {target} (after {out.elapsed_s:.0f}s)",
            "the Route exists but is not routable. Check the router pods, the "
            "Route's status.ingress conditions, and that the Service has endpoints.")

    def route_url(self) -> str:
        """The externally reachable base URL, from a Route or an Ingress.

        Both are supported because the two target environments differ only here:
        OpenShift has the Route API, Kind has ingress-nginx (§3.2). Discovering it
        rather than requiring --public-url means the same command line works on
        both.
        """
        r = self.k.get("route", "eventbridge", namespace=self.ns, missing_ok=True)
        if r is not None:
            host = dig(r, "spec", "host", default="")
            if host:
                scheme = "https" if dig(r, "spec", "tls", default=None) else "http"
                return f"{scheme}://{host}"
        return self.ingress_url()

    def ingress_url(self) -> str:
        """Base URL from the Ingress, including the host port Kind maps.

        An Ingress records the host it serves but not the port the controller is
        reachable on from outside the cluster — on Kind that is an
        `extraPortMappings` entry in the cluster config, which the API cannot tell
        us. So the port comes from --ingress-port (default 30080, matching
        k8s/kind/kind-cluster.yaml).
        """
        ing = self.k.get("ingress", "eventbridge", namespace=self.ns, missing_ok=True)
        if ing is None:
            return ""
        rules = dig(ing, "spec", "rules", default=[])
        host = dig(rules[0], "host", default="") if rules else ""
        if not host:
            return ""
        tls = dig(ing, "spec", "tls", default=None)
        scheme = "https" if tls else "http"
        port = int(getattr(self.args, "ingress_port", 0) or 30080)
        default_port = 443 if tls else 80
        suffix = "" if port == default_port else f":{port}"
        return f"{scheme}://{host}{suffix}"

    # ---- 10 -------------------------------------------------------------
    def check_topic_collisions(self) -> None:
        """Resolves RQ-5. Consuming someone else's topic would look like a working
        deploy that mysteriously processes foreign events."""
        for name in (self.request_topic, self.response_topic):
            t = self.k.get("kafkatopic", name, namespace=KAFKA_NS, missing_ok=True)
            if t is None:
                self.c.ok(f"topic name {name} is free (or ours to create)")
                continue
            owned = dig(t, "metadata", "labels", "app.kubernetes.io/part-of",
                        default="") == "rossoctl-eventing"
            self.c.expect(owned, f"pre-existing KafkaTopic {name} is ours",
                          f"a KafkaTopic named {name} exists in ns {KAFKA_NS} without "
                          f"our app.kubernetes.io/part-of label — someone else may own "
                          f"it. Pick a different namespace prefix (--namespace) or "
                          f"label the topic if it really is yours.")

    # ---- 11 -------------------------------------------------------------
    def check_namespace(self) -> None:
        ns = self.k.get("namespace", self.ns, namespace=False, missing_ok=True)
        if ns is None:
            self.c.log(f"namespace {self.ns} does not exist yet; deploy will create it")
            res = self.k.apply_stdin(
                json.dumps({"apiVersion": "v1", "kind": "Namespace",
                            "metadata": {"name": self.ns}}),
                namespace=False, server_dry_run=True)
            self.c.expect_run(res, f"namespace {self.ns} is creatable")
            return
        self.c.ok(f"namespace {self.ns} exists")
        uid_range = dig(ns, "metadata", "annotations",
                        "openshift.io/sa.scc.uid-range", default="")
        if uid_range:
            self.c.log(f"  SCC uid-range: {uid_range} (arbitrary-UID containers)")
        for q in self.k.items("resourcequota", namespace=self.ns):
            hard = dig(q, "status", "hard", default={})
            used = dig(q, "status", "used", default={})
            self.c.log(f"  quota {dig(q, 'metadata', 'name')}: used={used} hard={hard}")

    # ---- 12 -------------------------------------------------------------
    def check_storageclass(self) -> None:
        """Only relevant when the overlay asks for a PVC; otherwise a missing
        provisioner is not this deploy's problem."""
        if self.args.overlay != "demo":
            self.c.log("overlay does not request a PVC — skipping the StorageClass check")
            return
        defaults = [dig(s, "metadata", "name") for s in self.k.items("storageclass",
                                                                    namespace=False)
                    if dig(s, "metadata", "annotations",
                           "storageclass.kubernetes.io/is-default-class",
                           default="false") == "true"]
        self.c.expect(bool(defaults),
                      f"a default StorageClass exists: {defaults}",
                      "the demo overlay's PVC would stay Pending forever")


    # ---- 13 -------------------------------------------------------------
    def check_claude_cli_image(self) -> None:
        """T2.4's gate. The demo needs an image that actually carries the CLI;
        without it every real run fails with `claude binary not found`, and mock
        mode is exactly what stops the e2e test from catching that.

        This runs on a real cluster node rather than at build time on purpose: the
        CLI bundles a `bun` binary that aborts under QEMU user-mode emulation, so a
        cross-built image cannot execute it on an arm64 builder. Here it runs
        natively.
        """
        if self.args.overlay != "demo":
            self.c.log("overlay does not use the claude image — skipping")
            return
        ref = self.images.eventrunner_claude
        status, body, err = registry_manifest(ref)
        if not self.c.expect(status == 200, f"{ref} is anonymously pullable",
                             f"status={status} {err}\n"
                             f"  Build it: python3 scripts/build_images.py --with-claude"):
            return
        name = "rossoctl-preflight-claude"
        self.k.delete("pod", name, namespace=self.ns)
        pod = {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": self.ns},
            "spec": {"restartPolicy": "Never",
                     "securityContext": {"runAsNonRoot": True,
                                         "seccompProfile": {"type": "RuntimeDefault"}},
                     "containers": [{
                         "name": "probe", "image": ref,
                         "command": ["sh", "-c",
                                     "claude --version && python -c "
                                     "'import eventrunner.config as c; "
                                     "print(\"config-ok\", c.load().mock_claude)'"],
                         "securityContext": {"allowPrivilegeEscalation": False,
                                             "capabilities": {"drop": ["ALL"]}},
                     }]},
        }
        if not self.c.expect_run(self.k.apply_stdin(json.dumps(pod), namespace=self.ns),
                                 "claude-image probe pod created"):
            return
        try:
            wait_for(lambda: dig(self.k.get("pod", name, namespace=self.ns,
                                            missing_ok=True) or {},
                                 "status", "phase", default="") in ("Succeeded", "Failed"),
                     timeout=240, interval=3)
            phase = dig(self.k.get("pod", name, namespace=self.ns, missing_ok=True) or {},
                        "status", "phase", default="?")
            logs = self.k.logs(name, namespace=self.ns, tail=20)
            self.c.expect(phase == "Succeeded",
                          "the derived image runs `claude --version` on a real node",
                          f"phase={phase}\n{logs}")
            for line in logs.splitlines():
                self.c.log(f"  {line}")
            self.c.expect_contains(logs, "Claude Code", "the CLI reported its version")
            self.c.expect_contains(logs, "config-ok",
                                   "the image still loads EventRunner's config")
        finally:
            self.k.delete("pod", name, namespace=self.ns)


CHECKS = {
    1: ("cluster reachable + context confirmed", "check_cluster"),
    2: ("Strimzi + KEDA CRDs", "check_crds"),
    3: ("Strimzi watch scope + Kafka cluster", "check_strimzi_watch_scope"),
    4: ("images anonymously pullable + arch", "check_images"),
    6: ("arbitrary-UID writability", "check_arbitrary_uid"),
    7: ("KEDA reaches Kafka + metrics API", "check_keda_reaches_kafka"),
    8: ("ingress capability + wildcard DNS", "check_ingress_capability"),
    10: ("topic-name collisions", "check_topic_collisions"),
    11: ("namespace + quota", "check_namespace"),
    12: ("default StorageClass (demo overlay)", "check_storageclass"),
    13: ("claude CLI image runs on a real node (demo overlay)", "check_claude_cli_image"),
}
# Check 5 (node arch vs image platforms) is folded into check 4: it needs the same
# manifest fetch, and splitting it would mean fetching twice.


def run_pre(pf: Preflight, skip: set[int]) -> None:
    for num, (title, method) in CHECKS.items():
        if num in skip:
            pf.c.log(f"skipping check {num} ({title})")
            continue
        pf.c.section(f"check {num}: {title}")
        getattr(pf, method)()


def build_args(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--namespace", "-n", default=os.environ.get("NS", "kev1"))
    ap.add_argument("--kubeconfig", default=None,
                    help="kubeconfig file (e.g. .kube/config-kind for a Kind cluster)")
    ap.add_argument("--context", default=None, help="pin the kube context explicitly")
    ap.add_argument("--yes", action="store_true",
                    help="accept the ambient current-context without pinning it")
    ap.add_argument("--overlay", default="test", choices=("test", "kind", "demo"))
    ap.add_argument("--registry", default=os.environ.get("REGISTRY_PREFIX", DEFAULT_REGISTRY))
    ap.add_argument("--tag", default=os.environ.get("TAG", DEFAULT_TAG))
    ap.add_argument("--phase", default="pre", choices=("pre", "public", "all"))
    ap.add_argument("--skip", default="", help="comma-separated check numbers to skip")
    ap.add_argument("--public-url-timeout", type=float, default=120.0)
    ap.add_argument("--ingress-port", type=int, default=30080,
                    help="host port the ingress controller is reachable on "
                         "(Kind maps 80 -> 30080 in k8s/kind/kind-cluster.yaml)")
    ap.add_argument("--url", default=None, help="check 9: probe this URL instead of the Route")
    return ap.parse_args(argv)


def main(argv=None) -> int:
    args = build_args(argv)
    c = Checks(prefix=PREFIX)
    k = Kubectl(context=args.context, namespace=args.namespace,
                kubeconfig=args.kubeconfig)
    pf = Preflight(k, c, args)
    skip = {int(s) for s in args.skip.split(",") if s.strip()}

    if args.phase in ("pre", "all"):
        run_pre(pf, skip)
    if args.phase in ("public", "all"):
        c.section("check 9: public URL reachable end-to-end")
        pf.check_public_url(args.url)
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
