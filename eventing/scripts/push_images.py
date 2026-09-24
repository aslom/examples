#!/usr/bin/env python3
"""Push the container images to their configured registry. Converted from
`push-images.sh` (T0.4).

Assumes build_images.py --local already tagged them at the same REGISTRY_PREFIX
and TAG. Multi-arch builds push as part of the build and do not need this.

  python3 scripts/push_images.py
  python3 scripts/push_images.py --tag v0.1.0
  python3 scripts/push_images.py --dry-run       # print the exact refs, push nothing
  REGISTRY_PREFIX=ghcr.io/aslom/ python3 scripts/push_images.py

You must be logged into the registry first: `docker login quay.io`.
Exit 0 means every image pushed.
"""
from __future__ import annotations

import argparse
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from imagelib import (  # noqa: E402
    DEFAULT_REGISTRY,
    DEFAULT_TAG,
    Images,
    local_image_exists,
    manifest_platforms,
    registry_manifest,
    runtime_ready,
)
from proclib import Checks, die, run  # noqa: E402

PREFIX = "push"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--registry", default=os.environ.get("REGISTRY_PREFIX", DEFAULT_REGISTRY))
    ap.add_argument("--tag", default=os.environ.get("TAG", DEFAULT_TAG))
    ap.add_argument("--docker", default=os.environ.get("DOCKER", "docker"))
    ap.add_argument("--with-claude", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the refs that would be pushed and exit 0")
    ap.add_argument("--verify", action="store_true",
                    help="after pushing, assert each ref is anonymously pullable")
    ap.add_argument("--log-dir", default=os.environ.get(
        "LOG_DIR", f"{os.environ.get('TMPDIR', '/tmp').rstrip('/')}/rossoctl-eventing-build"))
    args = ap.parse_args(argv)

    c = Checks(prefix=PREFIX)
    images = Images(args.registry, args.tag)
    refs = images.all(with_claude=args.with_claude)

    c.log("targets:")
    for ref in refs:
        c.log(f"  {ref}")
    if args.dry_run:
        c.log("--dry-run: nothing pushed")
        return c.summary(allow_empty=True)

    ok, why = runtime_ready(args.docker)
    if not ok:
        die(why, prefix=PREFIX)

    log_dir = pathlib.Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    for ref in refs:
        if not local_image_exists(ref, runtime=args.docker).ok:
            die(f"image {ref} not found locally — run "
                f"`python3 scripts/build_images.py --local` first", prefix=PREFIX)

    for ref in refs:
        name = ref.rsplit("/", 1)[-1].replace(":", "-")
        res = run([args.docker, "push", ref], timeout=3600, merge_stderr=True)
        (log_dir / f"push-{name}.log").write_text(res.out or "")
        c.expect_run(res, f"pushed {ref}")

    if args.verify:
        # The same anonymous check preflight makes (§12 check 4), run here so a
        # repository that silently went private is caught at push time.
        for ref in refs:
            status, body, err = registry_manifest(ref)
            c.expect(status == 200, f"{ref} is anonymously pullable",
                     f"status={status} {err}")
            if status == 200:
                plats = manifest_platforms(body)
                c.log(f"  {ref} platforms: {sorted(plats) or 'single-arch'}")
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
