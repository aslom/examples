"""Checkpoint `claude` session transcripts through EventBridge.

Resolves DESIGN_PHASE1.md §16 Gap B: `claude --resume <uuid>` needs the
transcript `.jsonl` on the local filesystem, and in Phase 1 that filesystem is a
pod's ephemeral layer. So turn 1 writes the transcript into pod A, the demo goes
idle, pod A is destroyed, and turn 2's `--resume` runs against a filesystem that
has never seen the session.

The chosen fix (Gap B option 1) rests on one verified fact — see
`scripts/verify_resume_by_path.py`, which proves it rather than citing it:

    `--resume` accepts an absolute path to a transcript .jsonl, not just a
    session ID, and restores context from a DIFFERENT cwd and a DIFFERENT
    CLAUDE_CONFIG_DIR.

So the runner can stay stateless with no shared filesystem:

  * after a turn reaches its terminal event, PUT the transcript to EventBridge;
  * before a `mode=continue` turn, GET it into a local scratch path and pass
    **that path** to `--resume`.

EventBridge is already the single-replica stateful component with a volume and a
SQLite store (§8.6), so this needs no new backing service. There is no
concurrent-writer problem either: requests are keyed by `correlationid`, so all
turns for one conversation land on one partition and therefore one pod, and the
per-corr router serializes them within it — exactly one writer per session.

Stdlib only (`urllib.request`); §1.1 forbids `requests`.
"""
from __future__ import annotations

import json
import pathlib
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass

# A single trivial one-turn transcript measured 222 KB (the system prompt and
# tool definitions dominate), so the cap has to be generous or it rejects the
# very first checkpoint. 32 MiB is roughly a very long conversation and still
# small enough to hold in memory and to store in SQLite.
DEFAULT_MAX_BYTES = 32 * 1024 * 1024


def log(msg: str) -> None:
    # stderr: unbuffered by default and interleaves correctly with tracebacks.
    print(f"[transcript] {msg}", file=sys.stderr, flush=True)


def find_local(config_dir: str | pathlib.Path, session_uuid: str) -> pathlib.Path | None:
    """Locate the transcript `claude` wrote for this session.

    Globs `projects/*/<uuid>.jsonl` rather than reconstructing the directory
    name. The `projects/<name>` segment is derived from cwd with a lossy encoding
    (every non-alphanumeric character becomes `-`), and although
    CLAUDE_CODE_PROJECT_DIR_NAME can pin it, globbing is correct either way and
    does not depend on a CLI version.
    """
    root = pathlib.Path(config_dir)
    hits = sorted(root.glob(f"projects/*/{session_uuid}.jsonl"),
                  key=lambda p: p.stat().st_mtime if p.exists() else 0)
    return hits[-1] if hits else None


@dataclass
class TranscriptStore:
    """HTTP client for EventBridge's `/v0/agents/<corr>/transcript` endpoints."""

    base_url: str
    timeout: float = 20.0
    max_bytes: int = DEFAULT_MAX_BYTES
    enabled: bool = True

    def _url(self, corr: str) -> str:
        return f"{self.base_url.rstrip('/')}/v0/agents/{corr}/transcript"

    # ---- sequence continuity across pods ----
    def last_sequence(self, corr: str) -> int | None:
        """The highest `sequence` EventBridge has stored for this correlation.

        Needed because `Emitter` keeps its per-correlation sequence counter IN
        MEMORY. Phase 0 had one long-lived runner process, so the counter was
        naturally monotonic. In Phase 1 every scale-from-zero is a NEW process
        whose counter restarts at 1 — and the store's
        `PRIMARY KEY (correlationid, sequence)` with INSERT OR REPLACE means a
        later turn then *overwrites* the rows of an earlier one.

        Observed before this fix: a three-turn conversation stored six rows
        instead of nine, and turn 1's events had been replaced by turn 3's.

        Returns None when the answer is unknown (EventBridge unreachable), which
        the caller must distinguish from a genuine 0.
        """
        url = f"{self.base_url.rstrip('/')}/v0/agents/{corr}/events"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as r:
                if r.status != 200:
                    return None
                body = json.loads(r.read() or b"{}")
        except urllib.error.HTTPError as e:
            # 404 is a real answer: this correlation has no events yet.
            return 0 if e.code == 404 else None
        except (urllib.error.URLError, OSError, ValueError) as e:
            log(f"could not read the last sequence for {corr}: {e}")
            return None
        seqs = [int(e.get("sequence") or 0) for e in body.get("events") or []]
        return max(seqs) if seqs else 0

    def seed_emitter(self, emitter, corr: str) -> int | None:
        """Align an Emitter's counter with what EventBridge already stored."""
        last = self.last_sequence(corr)
        if last:
            emitter.seed_seq(corr, last)
            log(f"resuming sequence numbering for {corr} at {last + 1}")
        return last

    # ---- upload ----
    def upload(self, corr: str, path: str | pathlib.Path) -> bool:
        """PUT the transcript. Returns True on success; never raises.

        A checkpoint failure must not fail the agent run — the turn already
        completed and its response events are already on the wire. It only means
        the NEXT turn cannot resume, which is logged loudly here and degrades to
        the Phase 0 behaviour rather than losing work.
        """
        if not self.enabled:
            return False
        p = pathlib.Path(path)
        if not p.exists():
            log(f"no local transcript to checkpoint for {corr} (looked at {p})")
            return False
        size = p.stat().st_size
        if size > self.max_bytes:
            log(f"transcript for {corr} is {size} bytes, over the {self.max_bytes} "
                f"cap — not checkpointed; /continue after a scale-to-zero will "
                f"not resume this session")
            return False
        body = p.read_bytes()
        req = urllib.request.Request(
            self._url(corr), data=body, method="PUT",
            headers={"Content-Type": "application/x-ndjson",
                     "Content-Length": str(len(body))},
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                ok = 200 <= r.status < 300
        except (urllib.error.URLError, OSError) as e:
            log(f"checkpoint upload failed for {corr}: {e}")
            return False
        if ok:
            log(f"checkpointed {size} bytes for {corr}")
        return ok

    # ---- download ----
    def download(self, corr: str, dest: str | pathlib.Path) -> pathlib.Path | None:
        """GET the stored transcript to `dest`. Returns the path, or None."""
        if not self.enabled:
            return None
        try:
            with urllib.request.urlopen(self._url(corr), timeout=self.timeout) as r:
                if r.status != 200:
                    return None
                body = r.read()
        except urllib.error.HTTPError as e:
            if e.code != 404:
                log(f"checkpoint fetch for {corr} failed: HTTP {e.code}")
            return None
        except (urllib.error.URLError, OSError) as e:
            log(f"checkpoint fetch for {corr} failed: {e}")
            return None
        if not body:
            return None
        d = pathlib.Path(dest)
        d.parent.mkdir(parents=True, exist_ok=True)
        d.write_bytes(body)
        log(f"restored {len(body)} bytes for {corr} to {d}")
        return d

    # ---- the two operations the runner actually calls ----
    def restore_for_resume(self, corr: str, session_uuid: str,
                           config_dir: str | pathlib.Path,
                           scratch_dir: str | pathlib.Path) -> str | None:
        """Return an absolute path suitable for `--resume`, or None.

        Prefers a transcript already on this pod's disk (a warm pod handling a
        second turn — no HTTP round trip, and it is the authoritative copy).
        Falls back to the checkpoint in EventBridge, which is the cold-pod case
        Gap B is about.
        """
        local = find_local(config_dir, session_uuid)
        if local is not None:
            return str(local.resolve())
        dest = pathlib.Path(scratch_dir) / f"{session_uuid}.jsonl"
        got = self.download(corr, dest)
        return str(got.resolve()) if got else None

    def checkpoint_after_turn(self, corr: str, session_uuid: str,
                              config_dir: str | pathlib.Path) -> bool:
        """Upload whatever transcript this turn just produced."""
        local = find_local(config_dir, session_uuid)
        if local is None:
            log(f"turn for {corr} left no transcript under {config_dir} — "
                f"nothing to checkpoint")
            return False
        return self.upload(corr, local)
