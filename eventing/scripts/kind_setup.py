#!/usr/bin/env python3
"""Bring a local Kind cluster up to what Phase 1 needs. DESIGN_PHASE1.md §3.2.

The reference cluster already had Strimzi, KEDA and an ingress router; a fresh Kind
cluster has none of them, so `k8s_preflight.py` fails it on exactly three checks.
This script closes those three gaps and nothing else:

  1. the cluster itself, from k8s/kind/kind-cluster.yaml (only with --recreate);
  2. ingress-nginx, the kind-provider variant;
  3. Strimzi, watching namespace `kafka`, plus a single-node KRaft Kafka named
     `my-cluster` — the same name and bootstrap address as the reference cluster,
     which is what lets the application manifests stay identical;
  4. KEDA.

  python3 scripts/kind_setup.py --check              # report the gaps, change nothing
  python3 scripts/kind_setup.py                      # install into the existing cluster
  python3 scripts/kind_setup.py --recreate           # delete and rebuild the cluster first
  python3 scripts/kind_setup.py --kubeconfig .kube/config-kind

Then the normal flow works, pointed at that kubeconfig:

  python3 scripts/k8s_deploy.py  --kubeconfig .kube/config-kind --overlay kind --yes
  python3 scripts/k8s_e2e_test.py --kubeconfig .kube/config-kind --overlay kind

Exit 0 only if every step and check passed.
"""
from __future__ import annotations

import argparse
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from k8slib import Kubectl, condition_is, condition_reason, dig, wait_for  # noqa: E402
from proclib import Checks, die, have, run  # noqa: E402

PREFIX = "kind"
HERE = pathlib.Path(__file__).resolve().parent
K8S = HERE.parent / "k8s"
KIND_CONFIG = K8S / "kind" / "kind-cluster.yaml"
KAFKA_CR = K8S / "kind" / "kafka-cluster.yaml"

KAFKA_NS = "kafka"
KEDA_NS = "keda"
INGRESS_NS = "ingress-nginx"

# Pinned so a local run is reproducible. Strimzi's `install/latest` is used
# because strimzi.io/install/<version> is not a valid URL (it 404s) and the
# GitHub release asset hard-codes a `myproject` namespace that would need
# rewriting; `?namespace=kafka` does that rewrite server-side. Preflight check 2
# independently verifies the CRD actually serves kafka.strimzi.io/v1, so an
# unexpected Strimzi version fails loudly rather than silently.
STRIMZI_URL = "https://strimzi.io/install/latest?namespace=kafka"
KEDA_VERSION = "2.20.0"
KEDA_URL = (f"https://github.com/kedacore/keda/releases/download/"
            f"v{KEDA_VERSION}/keda-{KEDA_VERSION}.yaml")
INGRESS_VERSION = "controller-v1.12.1"
INGRESS_URL = (f"https://raw.githubusercontent.com/kubernetes/ingress-nginx/"
               f"{INGRESS_VERSION}/deploy/static/provider/kind/deploy.yaml")

# Matches k8s/kind/kind-cluster.yaml. The Ingress host uses nip.io so no
# /etc/hosts editing is needed.
PUBLIC_URL = "http://eventbridge.127.0.0.1.nip.io:30080"

MIN_CPU = 4
MIN_MEM_GIB = 8
REC_CPU = 6
REC_MEM_GIB = 12


def parse_quantity_cpu(v: str) -> float:
    v = str(v)
    return float(v[:-1]) / 1000 if v.endswith("m") else float(v)


def parse_quantity_mem_gib(v: str) -> float:
    v = str(v)
    units = {"Ki": 1 / (1024 ** 2), "Mi": 1 / 1024, "Gi": 1.0, "Ti": 1024.0}
    for suffix, mult in units.items():
        if v.endswith(suffix):
            return float(v[: -len(suffix)]) * mult
    return float(v) / (1024 ** 3)


class Kind:
    def __init__(self, c: Checks, args) -> None:
        self.c = c
        self.args = args
        self.name = args.name
        self.kubeconfig = args.kubeconfig
        self.k = Kubectl(kubeconfig=args.kubeconfig)

    # ---- the cluster ----
    def clusters(self) -> list[str]:
        res = run(["kind", "get", "clusters"], timeout=60)
        return [ln.strip() for ln in (res.out or "").splitlines() if ln.strip()]

    def exists(self) -> bool:
        return self.name in self.clusters()

    def create(self) -> bool:
        if self.exists():
            if not self.args.recreate:
                self.c.log(f"cluster {self.name!r} already exists")
                return True
            self.c.log(f"--recreate: deleting cluster {self.name!r}")
            self.c.expect_run(run(["kind", "delete", "cluster", "--name", self.name],
                                  timeout=300),
                              f"deleted the old {self.name!r} cluster")
        if not KIND_CONFIG.exists():
            die(f"{KIND_CONFIG} is missing", prefix=PREFIX)
        self.c.log(f"creating cluster {self.name!r} from {KIND_CONFIG.name} "
                   f"(this pulls a node image the first time)")
        res = run(["kind", "create", "cluster", "--name", self.name,
                   "--config", str(KIND_CONFIG),
                   "--kubeconfig", self.kubeconfig], timeout=900, merge_stderr=True)
        if not self.c.expect_run(res, f"cluster {self.name!r} created"):
            self.c.log(res.tail(20))
            return False
        return True

    # ---- resources ----
    def check_resources(self) -> None:
        """Kind has no resource knobs of its own — each node is a container and the
        ceiling is the Docker/Rancher/Colima VM. So this reports the VM's size and
        says what to change if it is too small, because a short VM shows up as
        Pending pods or an OOMKilled broker rather than as an error from kind."""
        self.c.section("resources")
        info = run(["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"], timeout=60)
        if info.ok and len(info.out.split()) == 2:
            cpus, mem = info.out.split()
            vm_gib = int(mem) / (1024 ** 3)
            self.c.log(f"container VM: {cpus} CPUs / {vm_gib:.1f} GiB")

        nodes = self.k.items("nodes", namespace=False)
        if not nodes:
            self.c.fail("the cluster reports at least one node", "none found")
            return
        total_cpu = sum(parse_quantity_cpu(dig(n, "status", "allocatable", "cpu"))
                        for n in nodes)
        total_mem = sum(parse_quantity_mem_gib(dig(n, "status", "allocatable", "memory"))
                        for n in nodes)
        arches = {dig(n, "status", "nodeInfo", "architecture") for n in nodes}
        self.c.log(f"allocatable across {len(nodes)} node(s): "
                   f"{total_cpu:g} CPU / {total_mem:.1f} GiB, arch={sorted(arches)}")

        self.c.expect(total_cpu >= MIN_CPU,
                      f"at least {MIN_CPU} allocatable CPU (have {total_cpu:g})",
                      f"Strimzi + KEDA + ingress-nginx + both workloads need about "
                      f"{MIN_CPU}; pods will sit Pending. Raise the VM's CPU count "
                      f"(Rancher/Docker Desktop preferences, or "
                      f"`colima start --cpu {REC_CPU}`).")
        self.c.expect(total_mem >= MIN_MEM_GIB,
                      f"at least {MIN_MEM_GIB} GiB allocatable memory "
                      f"(have {total_mem:.1f})",
                      f"a short VM OOMKills the Kafka broker rather than failing "
                      f"visibly. Raise the VM's memory to at least {MIN_MEM_GIB} GiB "
                      f"({REC_MEM_GIB} recommended).")
        if total_cpu < REC_CPU or total_mem < REC_MEM_GIB:
            self.c.warn(f"above the minimum but below the recommended "
                        f"{REC_CPU} CPU / {REC_MEM_GIB} GiB — fine for the mock e2e, "
                        f"tight once real `claude` subprocesses run")

        # arm64 is the normal case on Apple Silicon and matters for one image only.
        if arches == {"arm64"}:
            self.c.log("  note: arm64 cluster. eventbridge/eventrunner are "
                       "multi-arch, but the derived claude image is built "
                       "linux/amd64 by default — for the demo overlay here, build "
                       "it with --claude-platforms linux/arm64")

    # ---- the three gaps ----
    def install_ingress(self) -> None:
        self.c.section("ingress-nginx")
        if self.k.get("ingressclass", "nginx", namespace=False, missing_ok=True):
            self.c.ok("IngressClass nginx already present")
        else:
            label = dig(self.k.items("nodes", namespace=False)[0],
                        "metadata", "labels", "ingress-ready", default="")
            if label != "true":
                self.c.fail(
                    "a node is labelled ingress-ready=true",
                    "the kind ingress-nginx controller schedules with a nodeSelector "
                    "on that label and binds host ports. This cluster was not created "
                    "from k8s/kind/kind-cluster.yaml — re-run with --recreate.")
                return
            res = self.k.call(["apply", "-f", INGRESS_URL], namespace=False, timeout=300)
            if not self.c.expect_run(res, f"applied ingress-nginx {INGRESS_VERSION}"):
                return
        # `kubectl wait --for=condition=Ready pod` fails with "no matching
        # resources found" when it runs before the Deployment has created a pod,
        # which apply does not wait for. rollout status handles the gap.
        rollout = self.k.rollout_status("deploy/ingress-nginx-controller",
                                        namespace=INGRESS_NS, timeout=300)
        if not self.c.expect_run(rollout, "the ingress controller rolled out"):
            self.c.log(self.k.events(namespace=INGRESS_NS, tail=10))
            return
        cls = wait_for(lambda: self.k.get("ingressclass", "nginx", namespace=False,
                                          missing_ok=True) is not None,
                       timeout=60, interval=3)
        self.c.expect(cls, "IngressClass nginx is registered")

    def install_strimzi(self) -> None:
        self.c.section("Strimzi + a single-node Kafka")
        if "v1" in self.k.crd_served_versions("kafkatopics.kafka.strimzi.io"):
            self.c.ok("Strimzi CRDs already serve kafka.strimzi.io/v1")
        else:
            self.c.expect_run(self.k.ensure_namespace(KAFKA_NS),
                              f"namespace {KAFKA_NS} exists")
            # `create` rather than `apply`: the Strimzi bundle's CRDs exceed the
            # annotation size limit that client-side apply uses for
            # last-applied-configuration.
            res = self.k.call(["create", "-f", STRIMZI_URL],
                              namespace=KAFKA_NS, timeout=300)
            if not res.ok and "already exists" in (res.err or ""):
                res = self.k.call(["apply", "--server-side", "-f", STRIMZI_URL],
                                  namespace=KAFKA_NS, timeout=300)
            if not self.c.expect_run(res, "applied the Strimzi bundle"):
                return
        rollout = self.k.rollout_status("deploy/strimzi-cluster-operator",
                                        namespace=KAFKA_NS, timeout=300)
        self.c.expect_run(rollout, "the Strimzi cluster operator is running")

        res = self.k.apply_file(str(KAFKA_CR), namespace=False)
        if not self.c.expect_run(res, "applied Kafka/my-cluster + KafkaNodePool"):
            return
        self.c.log("waiting for the broker (first run pulls the Kafka image)")
        ready = wait_for(
            lambda: condition_is(self.k.get("kafka", "my-cluster", namespace=KAFKA_NS,
                                            missing_ok=True) or {}, "Ready"),
            timeout=600, interval=10)
        kafka = self.k.get("kafka", "my-cluster", namespace=KAFKA_NS, missing_ok=True) or {}
        if not self.c.expect(ready, f"Kafka/my-cluster is Ready "
                                    f"(after {ready.elapsed_s:.0f}s)",
                             f"{condition_reason(kafka, 'Ready')}\n"
                             f"  {self.k.events(namespace=KAFKA_NS, tail=8)}"):
            return
        self.c.log(f"  bootstrap: my-cluster-kafka-bootstrap.{KAFKA_NS}.svc:9092 "
                   f"(identical to the reference cluster, which is why the "
                   f"application manifests need no change)")

    def install_keda(self) -> None:
        self.c.section("KEDA")
        if self.k.crd_served_versions("scaledobjects.keda.sh"):
            self.c.ok("ScaledObject CRD already present")
        else:
            # --server-side: the KEDA bundle's CRDs are also too large for the
            # client-side last-applied annotation.
            res = self.k.call(["apply", "--server-side", "-f", KEDA_URL],
                              namespace=False, timeout=300)
            if not self.c.expect_run(res, f"applied KEDA {KEDA_VERSION}"):
                return
        # Names verified against a real 2.20.0 install: keda-operator,
        # keda-metrics-apiserver, keda-admission. (Not
        # "keda-operator-metrics-apiserver" — the Helm chart names it that way, the
        # release YAML does not.)
        for deploy in ("keda-operator", "keda-metrics-apiserver", "keda-admission"):
            self.c.expect_run(self.k.rollout_status(f"deploy/{deploy}",
                                                   namespace=KEDA_NS, timeout=300),
                              f"{deploy} is running")
        # The metrics APIService is what KEDA's scaling actually reads through; a
        # Ready operator with an unavailable APIService still cannot scale.
        avail = wait_for(
            lambda: condition_is(
                self.k.get("apiservice", "v1beta1.external.metrics.k8s.io",
                           namespace=False, missing_ok=True) or {}, "Available"),
            timeout=180, interval=5)
        self.c.expect(avail, "the external.metrics.k8s.io APIService is Available",
                      "KEDA cannot scale without it")

    # ---- report ----
    def check_only(self) -> None:
        self.c.section("gaps on this cluster")
        self.c.expect(self.exists(), f"a Kind cluster named {self.name!r} exists",
                      f"clusters: {self.clusters() or 'none'}")
        if not self.exists():
            return
        self.check_resources()
        self.c.section("what Phase 1 needs")
        self.c.expect("v1" in self.k.crd_served_versions("kafkatopics.kafka.strimzi.io"),
                      "Strimzi serves kafka.strimzi.io/v1")
        self.c.expect(self.k.get("kafka", "my-cluster", namespace=KAFKA_NS,
                                 missing_ok=True) is not None,
                      "Kafka/my-cluster exists in ns kafka")
        self.c.expect(bool(self.k.crd_served_versions("scaledobjects.keda.sh")),
                      "KEDA's ScaledObject CRD is present")
        self.c.expect(self.k.get("ingressclass", "nginx", namespace=False,
                                 missing_ok=True) is not None,
                      "an nginx IngressClass is present")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", default=os.environ.get("KIND_CLUSTER", "kev1"))
    ap.add_argument("--kubeconfig",
                    default=os.environ.get("KUBECONFIG", ".kube/config-kind"))
    ap.add_argument("--recreate", action="store_true",
                    help="delete and rebuild the cluster from k8s/kind/kind-cluster.yaml")
    ap.add_argument("--check", action="store_true",
                    help="report what is missing and change nothing")
    args = ap.parse_args(argv)

    c = Checks(prefix=PREFIX)
    if not have("kind"):
        die("kind is not on PATH — `brew install kind`", prefix=PREFIX)
    if not have("kubectl"):
        die("kubectl is not on PATH", prefix=PREFIX)
    c.log(f"cluster={args.name} kubeconfig={args.kubeconfig}")

    k = Kind(c, args)
    if args.check:
        k.check_only()
        return c.summary()

    if not k.create():
        return c.summary()
    k.check_resources()
    k.install_ingress()
    k.install_strimzi()
    k.install_keda()

    if not c.failed:
        c.log("")
        c.log("ready. Next:")
        c.log(f"  python3 scripts/k8s_deploy.py  --kubeconfig {args.kubeconfig} "
              f"--overlay kind --yes")
        c.log(f"  python3 scripts/k8s_e2e_test.py --kubeconfig {args.kubeconfig} "
              f"--overlay kind")
        c.note("public URL once deployed", PUBLIC_URL)
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
