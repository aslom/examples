#!/usr/bin/env python3
"""Build the container images at a configurable registry-prefixed tag.

Converted from `build-images.sh` (DESIGN_PHASE1.md §9, T0.3). Behaviour is
unchanged; the reason for the conversion is that the shell version's sibling
scripts had twice produced a harness reporting PASS for a command that never ran.

Defaults produce:
  quay.io/aslomnet/rossoctl-eventbridge:dev
  quay.io/aslomnet/rossoctl-eventrunner:dev

The default build is MULTI-ARCH (linux/amd64,linux/arm64) via docker buildx and
pushes atomically — a manifest list cannot live in a local docker daemon, so
multi-arch build and push are inseparable. `--local` does a single-arch host build
loaded into the local daemon (the dev-iteration path); `push_images.py` covers
publishing that.

  python3 scripts/build_images.py                     # multi-arch build+push
  python3 scripts/build_images.py --local             # single-arch host build
  python3 scripts/build_images.py --tag v0.1.0
  python3 scripts/build_images.py --with-claude       # also the §15 derived image
  REGISTRY_PREFIX=ghcr.io/aslom/ python3 scripts/build_images.py

Multi-arch needs an initialized buildx builder plus an amd64 emulator in the VM's
binfmt_misc. On Apple Silicon prefer Rosetta — it JITs x86_64 natively and is
roughly an order of magnitude faster than qemu-x86_64 for package installs:

  Rancher Desktop: Preferences → Virtual Machine → Emulation → Type VZ,
                   check "Rosetta support", apply, let the VM restart.
  Docker Desktop:  Settings → General → "Use Rosetta for x86_64/amd64 emulation"
  OrbStack:        Rosetta is the default.

One-time on a fresh host:
  docker buildx create --name rossoctl-multi --driver docker-container --use

Exit 0 means every requested image built (and pushed, in multi-arch mode).
"""
from __future__ import annotations

import argparse
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from imagelib import (  # noqa: E402
    DEFAULT_BUILDER,
    DEFAULT_PLATFORMS,
    DEFAULT_REGISTRY,
    DEFAULT_TAG,
    Images,
    runtime_ready,
)
from proclib import Checks, die, run  # noqa: E402

HERE = pathlib.Path(__file__).resolve().parent
CTX = HERE.parent                     # eventing/ — the Docker build context
PREFIX = "build"


def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--registry", default=os.environ.get("REGISTRY_PREFIX", DEFAULT_REGISTRY))
    ap.add_argument("--tag", default=os.environ.get("TAG", DEFAULT_TAG))
    ap.add_argument("--platforms", default=os.environ.get("PLATFORMS", DEFAULT_PLATFORMS))
    ap.add_argument("--builder", default=os.environ.get("BUILDER", DEFAULT_BUILDER))
    ap.add_argument("--docker", default=os.environ.get("DOCKER", "docker"))
    ap.add_argument("--local", action="store_true",
                    help="single-arch host build loaded into the local daemon")
    ap.add_argument("--with-claude", action="store_true",
                    help="also build the derived image carrying the claude CLI (§15)")
    ap.add_argument("--claude-platforms",
                    default=os.environ.get("CLAUDE_PLATFORMS", "linux/amd64"),
                    help="platforms for the derived claude image (default: linux/amd64 "
                         "only — the CLI bundles a bun binary that aborts under QEMU, "
                         "so cross-building it for extra arches buys nothing)")
    ap.add_argument("--no-emulator-probe", action="store_true")
    ap.add_argument("--log-dir", default=os.environ.get(
        "LOG_DIR", f"{os.environ.get('TMPDIR', '/tmp').rstrip('/')}/rossoctl-eventing-build"))
    return ap.parse_args(argv)


def probe_emulator(c: Checks, runtime: str, platforms: str) -> None:
    """Warn if amd64 emulation looks like slow QEMU rather than Rosetta.

    Rosetta completes a trivial x86_64 syscall in ~200ms; QEMU's cold JIT is 1s+.
    A heuristic, not a proof — but right in practice, and the alternative is a
    misconfigured VM silently taking twenty minutes per build.
    """
    if "linux/amd64" not in platforms:
        return
    uname = run(["uname", "-sm"])
    if uname.out.strip() != "Darwin arm64":
        return
    started = time.monotonic()
    r = run([runtime, "run", "--rm", "--platform=linux/amd64", "alpine", "true"],
            timeout=300)
    ms = int((time.monotonic() - started) * 1000)
    if not r.ok:
        die("amd64 emulation is not usable — enable Rosetta in Rancher/Docker "
            "Desktop, or install QEMU via 'tonistiigi/binfmt'.\n"
            f"  {r.tail(4)}", prefix=PREFIX)
    if ms > 800:
        c.warn(f"amd64 emulation took {ms}ms — likely QEMU, not Rosetta. "
               f"Enable Rosetta support (see this script's --help).")
    else:
        c.log(f"amd64 emulator ok ({ms}ms — consistent with Rosetta)")


def main(argv=None) -> int:
    args = parse_args(argv)
    c = Checks(prefix=PREFIX)
    images = Images(args.registry, args.tag)

    ok, why = runtime_ready(args.docker)
    if not ok:
        die(why, prefix=PREFIX)

    log_dir = pathlib.Path(args.log_dir)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        die(f"cannot create log dir {log_dir}: {e}", prefix=PREFIX)

    if args.local:
        mode = "local single-arch (host)"
    else:
        mode = f"multi-arch push [{args.platforms}]"
        if not run([args.docker, "buildx", "version"], timeout=60).ok:
            die("docker buildx not found. Install it from "
                "https://github.com/docker/buildx/releases into ~/.docker/cli-plugins/",
                prefix=PREFIX)
        if not run([args.docker, "buildx", "inspect", args.builder], timeout=120).ok:
            die(f"buildx builder {args.builder!r} not found. Create it:\n"
                f"  {args.docker} buildx create --name {args.builder} "
                f"--driver docker-container --use", prefix=PREFIX)
        if not args.no_emulator_probe:
            probe_emulator(c, args.docker, args.platforms)

    targets = [("Dockerfile-eventbridge", images.eventbridge, "eventbridge"),
               ("Dockerfile-eventrunner", images.eventrunner, "eventrunner")]
    if args.with_claude:
        targets.append(("Dockerfile-eventrunner-claude", images.eventrunner_claude,
                        "eventrunner-claude"))

    c.log(f"runtime={args.docker}  mode={mode}  context={CTX}  logs={log_dir}")
    c.log("targets:")
    for _, ref, _ in targets:
        c.log(f"  {ref}")

    for dockerfile, ref, name in targets:
        df = CTX / dockerfile
        if not df.exists():
            c.fail(f"built {name}", f"{df} does not exist")
            continue
        platforms = args.claude_platforms if name == "eventrunner-claude" else args.platforms
        if args.local:
            argv_build = [args.docker, "build", "-f", str(df), "-t", ref, str(CTX)]
        else:
            argv_build = [args.docker, "buildx", "build",
                          "--builder", args.builder,
                          "--platform", platforms,
                          "-f", str(df), "-t", ref, "--push", str(CTX)]
        # The derived claude image FROMs the runner image, so in multi-arch mode
        # the runner must already be in the registry — which it is, because the
        # list above is ordered and buildx pushes as it goes.
        c.log(f"building {name}"
              + ("" if args.local else f" [{platforms}]")
              + " (a first build downloads a free-threaded CPython — "
                "expect a few minutes)")
        logfile = log_dir / f"build-{name}.log"
        res = run(argv_build, timeout=3600, merge_stderr=True)
        logfile.write_text(res.out or "")
        if c.expect_run(res, f"built {name} -> {ref}"):
            continue
        c.log(f"  full log: {logfile}")

    if args.local:
        c.log(f"local tags created for {args.registry} — "
              f"run push_images.py to publish")
    else:
        c.log(f"verify with: {args.docker} buildx imagetools inspect {images.eventbridge}")
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
