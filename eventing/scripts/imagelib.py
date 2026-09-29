"""Shared image naming + registry inspection. Used by build/push and preflight.

The registry query is here rather than in each script because §12 check 4 needs
exactly the same notion of "what ref are we talking about" as the build does — a
preflight that checks a different tag than the deploy uses is worse than no
preflight.
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass

from proclib import Result, run

DEFAULT_REGISTRY = "quay.io/aslomnet/"
DEFAULT_TAG = "dev"
DEFAULT_PLATFORMS = "linux/amd64,linux/arm64"
DEFAULT_BUILDER = "rossoctl-multi"

# Media types that mean "manifest list" — a multi-arch image.
_INDEX_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)
_ACCEPT = ", ".join((*_INDEX_TYPES,
                     "application/vnd.oci.image.manifest.v1+json",
                     "application/vnd.docker.distribution.manifest.v2+json"))


def normalize_prefix(prefix: str) -> str:
    """Guarantee the trailing slash so f'{prefix}name' renders correctly."""
    return prefix if prefix.endswith("/") else prefix + "/"


@dataclass(frozen=True)
class Images:
    registry: str
    tag: str

    @property
    def eventbridge(self) -> str:
        return f"{normalize_prefix(self.registry)}rossoctl-eventbridge:{self.tag}"

    @property
    def eventrunner(self) -> str:
        return f"{normalize_prefix(self.registry)}rossoctl-eventrunner:{self.tag}"

    @property
    def eventrunner_claude(self) -> str:
        """The derived image carrying the `claude` CLI (§15 / T2.4).

        A TAG on the existing runner repository rather than a repository of its
        own, which is not cosmetic: Quay creates new repositories **private** by
        default, so `rossoctl-eventrunner-claude` came back HTTP 401 to an
        anonymous manifest request and preflight check 4 hard-failed it — exactly
        as Finding 2 intends, since the alternative is a confusing
        ImagePullBackOff mid-deploy. Reusing the already-public repository keeps
        the images anonymously pullable with no human step in the registry UI and
        no pull-secret fallback.
        """
        return f"{normalize_prefix(self.registry)}rossoctl-eventrunner:claude-{self.tag}"

    def all(self, *, with_claude: bool = False) -> list[str]:
        out = [self.eventbridge, self.eventrunner]
        if with_claude:
            out.append(self.eventrunner_claude)
        return out


def split_ref(ref: str) -> tuple[str, str, str]:
    """`quay.io/ns/name:tag` -> (registry host, repository, tag)."""
    name, _, tag = ref.rpartition(":")
    if "/" not in name:
        raise ValueError(f"not a fully qualified image ref: {ref!r}")
    host, _, repo = name.partition("/")
    return host, repo, tag or "latest"


def registry_manifest(ref: str, *, timeout: float = 20.0) -> tuple[int, dict | None, str]:
    """Anonymous manifest GET. Returns (http status, parsed body, error text).

    §12 check 4 / Finding 2: image visibility is a Quay setting a human can flip
    back, and the failure mode is a slow, confusing ImagePullBackOff deep into a
    deploy. So the check is an *unauthenticated* request — exactly what the
    kubelet will do with no pull secret.
    """
    host, repo, tag = split_ref(ref)
    url = f"https://{host}/v2/{repo}/manifests/{tag}"
    req = urllib.request.Request(url, headers={"Accept": _ACCEPT})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null"), ""
    except urllib.error.HTTPError as e:
        return e.code, None, f"HTTP {e.code} {e.reason}"
    except (urllib.error.URLError, OSError, ValueError) as e:
        return 0, None, str(e)


def manifest_platforms(body: dict | None) -> set[str]:
    """`{'linux/amd64', 'linux/arm64'}` from a manifest list, or empty."""
    if not isinstance(body, dict):
        return set()
    out = set()
    for m in body.get("manifests") or []:
        p = m.get("platform") or {}
        os_, arch = p.get("os"), p.get("architecture")
        if os_ and arch and arch != "unknown":
            out.add(f"{os_}/{arch}")
    return out


def local_image_exists(ref: str, *, runtime: str = "docker") -> Result:
    return run([runtime, "image", "inspect", ref], timeout=60)


def runtime_ready(runtime: str) -> tuple[bool, str]:
    r = run([runtime, "info"], timeout=60)
    if not r.launched:
        return False, (f"'{runtime}' not found on PATH. Install Docker, "
                       f"or set DOCKER=podman.")
    if r.rc != 0:
        return False, (f"'{runtime}' is installed but its daemon is not "
                       f"reachable — is Docker running?")
    return True, ""
