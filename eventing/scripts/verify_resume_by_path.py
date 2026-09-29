#!/usr/bin/env python3
"""T1.7 — prove `claude --resume <absolute path to a .jsonl>` restores context.

This is the single empirical fact the Gap B fix rests on (DESIGN_PHASE1.md §16).
The headless docs say the path form works; nothing in this project had verified
it. If it does NOT work, the transcript-checkpoint design (T1.8/T1.9) is dropped
and Gap B falls back to option 3 (an RWO PVC plus maxReplicaCount: 1).

What it does:

  1. Runs a start turn in directory A with a fresh `--session-id <uuid>`, asking
     the model to remember a codeword.
  2. Locates the transcript `.jsonl` that turn wrote, and copies it to a path
     under directory B — a *different* cwd, which is what a different pod is.
  3. Runs a continue turn in directory B with `--resume <that absolute path>`
     and asserts the reply contains the codeword.
  4. Negative control: `--resume <session-uuid>` from directory B with a config
     dir that has never seen the session. This is the failure a scaled-to-zero
     pod hits today, and it must NOT silently succeed — otherwise step 3 proves
     nothing about the path form.

Costs two cheap model calls. Defaults to claude-haiku-4-5.

  python3 scripts/verify_resume_by_path.py
  python3 scripts/verify_resume_by_path.py --model claude-sonnet-4-5-20250929
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import sys
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from proclib import Checks, die, have, run  # noqa: E402

CODEWORD = "PLUM-8821"
# "Do not use any tools" matters: left to itself the model may Write the codeword
# to its memory directory, and that tool call consumes a turn. With --max-turns 1
# the run then ends as subtype=error_max_turns and the CLI exits 1 — a real exit
# code that has nothing to do with resume working.
_NO_TOOLS = " Do not use any tools; answer directly from the conversation."
START_PROMPT = (f"Remember this codeword for later: {CODEWORD}. "
                f"Reply with exactly the word OK and nothing else." + _NO_TOOLS)
RESUME_PROMPT = ("What codeword did I ask you to remember? "
                 "Reply with just the codeword and nothing else." + _NO_TOOLS)


def claude_argv(binary: str, prompt: str, model: str, *, session_id: str | None = None,
                resume: str | None = None, max_turns: int = 4) -> list[str]:
    argv = [binary, "-p", prompt,
            "--output-format", "stream-json", "--verbose",
            "--max-turns", str(max_turns), "--model", model]
    if session_id:
        argv += ["--session-id", session_id]
    if resume:
        argv += ["--resume", resume]
    return argv


def reply_text(stdout: str) -> str:
    """Concatenate assistant text + the result string from a stream-json run."""
    parts: list[str] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            frame = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(frame, dict):
            continue
        if frame.get("type") == "assistant":
            for c in (frame.get("message") or {}).get("content") or []:
                if isinstance(c, dict) and c.get("type") == "text":
                    parts.append(c.get("text") or "")
        elif frame.get("type") == "result" and frame.get("result"):
            parts.append(str(frame["result"]))
    return "\n".join(parts)


def find_transcript(config_dir: pathlib.Path, session_id: str) -> pathlib.Path | None:
    """Transcripts live at <config>/projects/<encoded-cwd>/<uuid>.jsonl. The
    encoding of cwd is lossy, so search for the name rather than reconstruct it."""
    hits = sorted(config_dir.glob(f"projects/*/{session_id}.jsonl"))
    return hits[0] if hits else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", default="claude-haiku-4-5-20251001")
    ap.add_argument("--claude-bin", default=os.environ.get("CLAUDE_BIN", "claude"))
    ap.add_argument("--workdir", default=None,
                    help="scratch root (default: $TMPDIR/phase1-t17)")
    ap.add_argument("--keep", action="store_true", help="leave the scratch dirs behind")
    args = ap.parse_args()

    c = Checks(prefix="t1.7")

    if not have(args.claude_bin):
        die(f"{args.claude_bin} not found on PATH — this gate needs the real CLI",
            prefix="t1.7")

    tmp = pathlib.Path(args.workdir or
                       f"{os.environ.get('TMPDIR', '/tmp').rstrip('/')}/phase1-t17")
    if tmp.exists() and not args.keep:
        shutil.rmtree(tmp)
    dir_a = tmp / "dirA"
    dir_b = tmp / "dirB"
    cfg_a = tmp / "cfgA"
    cfg_b = tmp / "cfgB"
    for d in (dir_a, dir_b, cfg_a, cfg_b):
        d.mkdir(parents=True, exist_ok=True)

    session_id = str(uuid.uuid4())
    c.log(f"model={args.model} session={session_id}")
    c.log(f"scratch={tmp}")

    ver = run([args.claude_bin, "--version"], timeout=60)
    c.expect_run(ver, f"claude CLI present ({ver.out.strip() or '?'})")

    # ---- 1. start turn in directory A -------------------------------------
    c.section("turn 1 — start, in directory A")
    env_a = {"CLAUDE_CONFIG_DIR": str(cfg_a)}
    r1 = run(claude_argv(args.claude_bin, START_PROMPT, args.model, session_id=session_id),
             cwd=str(dir_a), env=env_a, timeout=300, stdin="")
    if not c.expect_run(r1, "start turn exited 0"):
        c.log(f"stdout: {r1.out[-1200:]}")
        c.log(f"stderr: {r1.err[-600:]}")
        return c.summary()
    c.log(f"  reply: {reply_text(r1.out).strip()[:80]!r}")

    # ---- 2. locate and relocate the transcript ----------------------------
    c.section("transcript relocation")
    src = find_transcript(cfg_a, session_id)
    if not c.expect(src is not None, "transcript .jsonl written by the start turn",
                    f"no projects/*/{session_id}.jsonl under {cfg_a}"):
        return c.summary()
    lines = src.read_text(errors="replace").splitlines()
    c.expect(len(lines) > 0, f"transcript is non-empty ({len(lines)} lines, "
                             f"{src.stat().st_size} bytes)")
    c.log(f"  at {src}")

    restored = dir_b / f"restored-{session_id}.jsonl"
    shutil.copy2(src, restored)
    c.expect(restored.exists(), "transcript copied to a path under directory B")

    # ---- 3. the load-bearing claim: resume BY PATH from a different cwd ----
    c.section("turn 2 — resume by absolute path, in directory B, fresh config dir")
    env_b = {"CLAUDE_CONFIG_DIR": str(cfg_b)}
    r2 = run(claude_argv(args.claude_bin, RESUME_PROMPT, args.model,
                         resume=str(restored.resolve())),
             cwd=str(dir_b), env=env_b, timeout=300, stdin="")
    c.expect_run(r2, "resume-by-path turn exited 0")
    got = reply_text(r2.out)
    c.log(f"  reply: {got.strip()[:120]!r}")
    path_form_works = CODEWORD in got
    c.expect(path_form_works,
             f"resumed reply contains the codeword {CODEWORD} — context was restored",
             f"stdout tail:\n{r2.tail(12)}" if not path_form_works else "")

    # ---- 4. negative control ----------------------------------------------
    c.section("negative control — resume by session-id with no local transcript")
    cfg_c = tmp / "cfgC"
    cfg_c.mkdir(parents=True, exist_ok=True)
    r3 = run(claude_argv(args.claude_bin, RESUME_PROMPT, args.model, resume=session_id),
             cwd=str(dir_b), env={"CLAUDE_CONFIG_DIR": str(cfg_c)}, timeout=300, stdin="")
    got3 = reply_text(r3.out)
    control_failed = (not r3.ok) or (CODEWORD not in got3)
    c.expect(control_failed,
             "resume-by-ID on a filesystem that never saw the session does NOT "
             "recover context (this is Gap B, and it confirms the path form is "
             "what does the work)",
             f"rc={r3.rc} reply={got3.strip()[:120]!r}")
    c.log(f"  rc={r3.rc} reply={got3.strip()[:80]!r}")
    if not r3.ok:
        c.log(f"  stderr: {r3.tail(3)}")

    c.note("VERDICT", "path-form resume WORKS — T1.8/T1.9 (transcript checkpoint) proceed"
           if path_form_works else
           "path-form resume FAILED — fall back to Gap B option 3 (RWO PVC, maxReplicas 1)")
    if not args.keep:
        c.log(f"(scratch left at {tmp} for inspection; pass nothing to reuse)")
    return c.summary()


if __name__ == "__main__":
    sys.exit(main())
