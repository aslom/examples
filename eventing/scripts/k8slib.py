"""kubectl wrapper that returns data structures, not strings.

DESIGN_PHASE1.md §9, point 2: `kubectl -o jsonpath='{.status.foo}'` returns an
empty string and exits **0** when the field moves or the object is a different
shape than you assumed. Every such silent empty becomes a confusing failure
somewhere downstream. So:

  * everything goes through `-o json` and comes back as a dict;
  * `dig()` RAISES for a path that is not present, unless you pass an explicit
    default — making "this field moved" a loud error at the point of the
    mistake;
  * no shell, ever: argv lists all the way down (see proclib).

No Kubernetes client library — DESIGN_PHASE0.md §1.1 forbids new runtime deps,
and shelling out to kubectl inherits the user's kubeconfig, auth plugins and
OpenShift login without this code handling any of it.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Sequence

from proclib import Result, WaitOutcome, run, wait_for

_MISSING = object()


class FieldMissing(KeyError):
    """A dotted path was not present in an object. Deliberately an error."""


def dig(obj: Any, *path: str | int, default: Any = _MISSING) -> Any:
    """Walk nested dicts/lists. Raise FieldMissing unless a default is given.

    `dig(obj, "status", "conditions", 0, "type")` — ints index lists.
    """
    cur = obj
    for i, key in enumerate(path):
        trail = ".".join(str(p) for p in path[: i + 1])
        try:
            if isinstance(key, int):
                cur = cur[key]
            else:
                if not isinstance(cur, dict):
                    raise TypeError(f"{trail}: expected a mapping, got {type(cur).__name__}")
                cur = cur[key]
        except (KeyError, IndexError, TypeError) as e:
            if default is not _MISSING:
                return default
            raise FieldMissing(
                f"path {'.'.join(str(p) for p in path)!r} not present "
                f"(failed at {trail!r}: {e})"
            ) from None
    return cur


def conditions(obj: dict) -> dict[str, dict]:
    """`{condition type: condition}` for any object with status.conditions."""
    out: dict[str, dict] = {}
    for c in dig(obj, "status", "conditions", default=[]) or []:
        if isinstance(c, dict) and c.get("type"):
            out[c["type"]] = c
    return out


def condition_is(obj: dict, ctype: str, want: str = "True") -> bool:
    c = conditions(obj).get(ctype)
    return bool(c) and str(c.get("status", "")).lower() == want.lower()


def condition_reason(obj: dict, ctype: str) -> str:
    c = conditions(obj).get(ctype) or {}
    return f"{c.get('status', '?')}/{c.get('reason', '?')}: {c.get('message', '')}".strip()


@dataclass
class Kubectl:
    """Thin, explicit kubectl front end.

    `context` is carried on every call rather than relying on the ambient
    current-context, because §12 check 1 exists for the one failure nobody
    recovers from gracefully: deploying into the wrong cluster.
    """
    context: str | None = None
    namespace: str | None = None
    binary: str = "kubectl"
    # Explicit beats ambient: a Kind cluster usually lives in its own kubeconfig
    # (e.g. .kube/config-kind), and relying on the KUBECONFIG env var means the
    # target depends on how the script was invoked rather than on what it was told.
    kubeconfig: str | None = None
    timeout: float = 120.0
    dry_run: bool = False          # print instead of mutating (deploy --dry-run)

    # ---- argv plumbing ----
    def argv(self, args: Sequence[str], *, namespace: str | None | bool = None) -> list[str]:
        out = [self.binary]
        if self.kubeconfig:
            out += ["--kubeconfig", self.kubeconfig]
        if self.context:
            out += ["--context", self.context]
        # namespace=False suppresses -n entirely (cluster-scoped calls).
        ns = self.namespace if namespace is None else namespace
        if ns:
            out += ["-n", str(ns)]
        return out + [str(a) for a in args]

    def call(self, args: Sequence[str], *, namespace: str | None | bool = None,
             stdin: str | None = None, timeout: float | None = None) -> Result:
        return run(self.argv(args, namespace=namespace),
                   timeout=timeout or self.timeout, stdin=stdin)

    def json_call(self, args: Sequence[str], *, namespace: str | None | bool = None,
                  timeout: float | None = None) -> Any:
        res = self.call([*args, "-o", "json"], namespace=namespace, timeout=timeout)
        return res.json()

    # ---- reads ----
    def get(self, kind: str, name: str | None = None, *,
            namespace: str | None | bool = None, missing_ok: bool = False,
            selector: str | None = None) -> dict | None:
        args = ["get", kind]
        if name:
            args.append(name)
        if selector:
            args += ["-l", selector]
        res = self.call([*args, "-o", "json"], namespace=namespace)
        if not res.ok:
            low = (res.err or "").lower()
            if missing_ok and ("notfound" in low.replace(" ", "")
                               or "not found" in low
                               or "the server doesn't have a resource type" in low):
                return None
            raise RuntimeError(f"kubectl get {kind} {name or ''} failed: {res.tail(6)}")
        return res.json()

    def items(self, kind: str, **kw) -> list[dict]:
        obj = self.get(kind, **kw)
        if obj is None:
            return []
        return obj.get("items", [obj]) if "items" in obj else [obj]

    def raw(self, path: str) -> Any:
        """`kubectl get --raw <path>` — used for the external metrics API."""
        res = self.call(["get", "--raw", path], namespace=False)
        if not res.ok:
            raise RuntimeError(f"raw GET {path} failed: {res.tail(4)}")
        return json.loads(res.out or "null")

    def current_context(self) -> str:
        argv = [self.binary]
        if self.kubeconfig:
            argv += ["--kubeconfig", self.kubeconfig]
        res = run([*argv, "config", "current-context"], timeout=30)
        return res.out.strip() if res.ok else ""

    def api_resource_exists(self, name: str) -> bool:
        """True when the API server serves this resource (e.g. 'routes.route.openshift.io')."""
        res = self.call(["get", name, "--ignore-not-found"], namespace=False, timeout=60)
        if res.ok:
            return True
        low = (res.err or "").lower()
        return not ("the server doesn't have a resource type" in low
                    or "could not find the requested resource" in low)

    def api_group_serves(self, group_version: str, kind: str) -> bool:
        """True when the API server serves `kind` in `group_version`.

        Not the same question as "is there a CRD for it". On OpenShift, Route is
        served by an AGGREGATED api server (openshift-apiserver), so it never
        appears in `kubectl get crd` — a CRD-based check reports "no Routes" on a
        cluster where Routes are the only working ingress.
        """
        try:
            body = self.raw(f"/apis/{group_version}")
        except RuntimeError:
            return False
        return any(r.get("kind") == kind for r in (body or {}).get("resources") or [])

    def crd_served_versions(self, crd_name: str) -> list[str]:
        crd = self.get("crd", crd_name, namespace=False, missing_ok=True)
        if crd is None:
            return []
        return [v["name"] for v in dig(crd, "spec", "versions", default=[])
                if v.get("served")]

    def node_architectures(self) -> set[str]:
        return {dig(n, "status", "nodeInfo", "architecture", default="?")
                for n in self.items("nodes", namespace=False)}

    def replicas(self, deploy: str, *, namespace: str | None = None) -> int:
        """Current *spec* replicas — the number KEDA writes. 0 is a real answer,
        so a missing field must not read as 0: default to 0 only for a Deployment
        that legitimately omits it (KEDA-managed, before first scale)."""
        d = self.get("deploy", deploy, namespace=namespace, missing_ok=True)
        if d is None:
            return -1
        return int(dig(d, "spec", "replicas", default=0) or 0)

    def ready_replicas(self, deploy: str, *, namespace: str | None = None) -> int:
        d = self.get("deploy", deploy, namespace=namespace, missing_ok=True)
        if d is None:
            return -1
        return int(dig(d, "status", "readyReplicas", default=0) or 0)

    def pod_names(self, selector: str, *, namespace: str | None = None) -> list[str]:
        pods = self.get("pods", namespace=namespace, selector=selector, missing_ok=True) or {}
        return [dig(p, "metadata", "name") for p in pods.get("items", [])]

    def logs(self, target: str, *, namespace: str | None = None, tail: int = 200,
             container: str | None = None, previous: bool = False) -> str:
        args = ["logs", target, f"--tail={tail}"]
        if container:
            args += ["-c", container]
        if previous:
            args.append("--previous")
        res = self.call(args, namespace=namespace, timeout=60)
        return res.out or res.err

    def events(self, *, namespace: str | None = None, tail: int = 20) -> str:
        res = self.call(["get", "events", "--sort-by=.lastTimestamp"],
                        namespace=namespace, timeout=60)
        lines = (res.out or "").splitlines()
        return "\n".join(lines[-tail:])

    def describe(self, kind: str, name: str, *, namespace: str | None = None) -> str:
        res = self.call(["describe", kind, name], namespace=namespace, timeout=60)
        return res.out or res.err

    # ---- writes ----
    def apply_file(self, path: str, *, namespace: str | None | bool = None,
                   server_dry_run: bool = False) -> Result:
        args = ["apply", "-f", path]
        if server_dry_run:
            args.append("--dry-run=server")
        elif self.dry_run:
            args.append("--dry-run=client")
        return self.call(args, namespace=namespace, timeout=180)

    def apply_kustomize(self, path: str, *, server_dry_run: bool = False) -> Result:
        args = ["apply", "-k", path]
        if server_dry_run:
            args.append("--dry-run=server")
        elif self.dry_run:
            args.append("--dry-run=client")
        return self.call(args, namespace=False, timeout=300)

    def apply_stdin(self, manifest: str, *, namespace: str | None | bool = None,
                    server_dry_run: bool = False) -> Result:
        args = ["apply", "-f", "-"]
        if server_dry_run:
            args.append("--dry-run=server")
        return self.call(args, namespace=namespace, stdin=manifest, timeout=120)

    def kustomize(self, path: str) -> str:
        """Rendered overlay YAML. The input to the §14.1 manifest digest.

        No kubeconfig needed — rendering is local and must not depend on a cluster.
        """
        res = run([self.binary, "kustomize", path], timeout=120)
        if not res.ok:
            raise RuntimeError(f"kubectl kustomize {path} failed: {res.tail(8)}")
        return res.out

    def diff_kustomize(self, path: str) -> tuple[int, str]:
        """(exit code, diff text). 0 = identical, 1 = differs, >1 = error.

        Authoritative change detection (§14.1): a server-side dry-run compared
        against live objects, so it also catches cluster-side drift that a local
        file hash cannot see.
        """
        res = self.call(["diff", "-k", path], namespace=False, timeout=180)
        if not res.launched:
            return 2, res.err
        return res.rc, res.out or res.err

    def delete(self, kind: str, name: str, *, namespace: str | None | bool = None,
               missing_ok: bool = True, timeout: float = 120.0) -> Result:
        args = ["delete", kind, name]
        if missing_ok:
            args.append("--ignore-not-found=true")
        return self.call(args, namespace=namespace, timeout=timeout)

    def scale(self, target: str, replicas: int, *, namespace: str | None = None) -> Result:
        return self.call(["scale", target, f"--replicas={replicas}"], namespace=namespace)

    def annotate(self, kind: str, name: str, *annotations: str,
                 namespace: str | None | bool = None, overwrite: bool = True) -> Result:
        args = ["annotate", kind, name, *annotations]
        if overwrite:
            args.append("--overwrite")
        return self.call(args, namespace=namespace)

    def set_env(self, target: str, *pairs: str, namespace: str | None = None) -> Result:
        return self.call(["set", "env", target, *pairs], namespace=namespace, timeout=120)

    def rollout_status(self, target: str, *, namespace: str | None = None,
                       timeout: int = 180) -> Result:
        return self.call(["rollout", "status", target, f"--timeout={timeout}s"],
                         namespace=namespace, timeout=timeout + 30)

    def wait_condition(self, kind_name: str, condition: str, *,
                       namespace: str | None | bool = None, timeout: int = 120) -> Result:
        return self.call(["wait", f"--for=condition={condition}", kind_name,
                          f"--timeout={timeout}s"],
                         namespace=namespace, timeout=timeout + 30)

    def ensure_namespace(self, ns: str) -> Result:
        """Idempotent namespace create (the `--dry-run=client | apply -f -` idiom
        without the pipe — we already have the YAML)."""
        manifest = json.dumps({
            "apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns},
        })
        return self.apply_stdin(manifest, namespace=False)


# ---- convenience predicates for wait_for() ----------------------------------

def deploy_replicas_is(k: Kubectl, deploy: str, want: int, *, namespace: str | None = None):
    def _p():
        return k.replicas(deploy, namespace=namespace) == want
    return _p


def scaledobject_active(k: Kubectl, name: str, want: bool, *, namespace: str | None = None):
    def _p():
        so = k.get("scaledobject", name, namespace=namespace, missing_ok=True)
        if so is None:
            return False
        return condition_is(so, "Active", "True" if want else "False")
    return _p


__all__ = [
    "Kubectl", "dig", "FieldMissing", "conditions", "condition_is",
    "condition_reason", "deploy_replicas_is", "scaledobject_active",
    "wait_for", "WaitOutcome", "Result", "run",
]
