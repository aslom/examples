"""Transcript checkpointing — the §16 Gap B fix. Gates T1.8 and T1.9.

Gap B: `claude --resume` needs the session transcript on the local filesystem,
and in Phase 1 that is a pod's ephemeral layer. Turn 1 writes it into pod A, the
demo idles, pod A is destroyed, and turn 2's `--resume` runs against a filesystem
that has never seen the session.

The fix rests on one empirically verified fact — `--resume` accepts an absolute
path to a `.jsonl`, proven by `scripts/verify_resume_by_path.py` rather than
cited from docs. These tests cover the plumbing built on top of it:

  T1.8  EventBridge PUT/GET: round-trip, size-cap rejection, 404 for an unknown
        correlation, idempotent overwrite.
  T1.9  EventRunner: a `mode=continue` turn with no local transcript fetches and
        resumes by path; a completed turn triggers exactly one upload.
"""
import io
import json
import pathlib

import pytest
from eventbridge.config import Cfg as EbCfg
from eventbridge.handlers import Handlers
from eventbridge.store import Store
from eventrunner.config import Cfg as ErCfg
from eventrunner.runner import build_cmd, resolve_resume_target
from eventrunner.transcript import TranscriptStore, find_local
from shared import ce

TRANSCRIPT = b'{"type":"user","message":{"role":"user"}}\n{"type":"assistant"}\n'


# ---- a minimal WSGI harness -------------------------------------------------

class FakeProducer:
    def __init__(self):
        self.published = []

    def publish_request(self, **kw):
        self.published.append(kw)
        return "evt-id"


class FakeMinter:
    def mint(self):
        return "brave-otter-4718"

    def remember(self, corr):
        pass


def call(handler, *, body: bytes = b"", corr="brave-otter-4718", qs=""):
    """Invoke a WSGI handler and return (status, headers, body)."""
    captured = {}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = dict(headers)

    environ = {
        "wsgi.input": io.BytesIO(body),
        "CONTENT_LENGTH": str(len(body)) if body else "",
        "QUERY_STRING": qs,
        "REQUEST_METHOD": "PUT" if body else "GET",
    }
    out = handler(environ, start_response, correlationid=corr)
    return captured["status"], captured.get("headers", {}), b"".join(out)


@pytest.fixture
def bridge(tmp_path):
    store = Store(tmp_path / "eb")
    cfg = EbCfg(tmpdir=str(tmp_path), transcript_max_bytes=1024)
    h = Handlers(cfg, store, FakeProducer(), FakeMinter())
    store.upsert_session("brave-otter-4718", ce.session_uuid("brave-otter-4718"),
                         str(tmp_path / "work"), "hello")
    return h, store


# ---- T1.8: the EventBridge endpoints ---------------------------------------

def test_transcript_round_trips(bridge):
    h, _ = bridge
    status, _, body = call(h.put_transcript, body=TRANSCRIPT)
    assert status.startswith("200"), body
    meta = json.loads(body)
    assert meta["size"] == len(TRANSCRIPT)
    assert meta["checkpoints"] == 1

    status, headers, got = call(h.get_transcript)
    assert status.startswith("200")
    assert got == TRANSCRIPT, "the transcript must come back byte-identical"
    assert headers["Content-Type"] == "application/x-ndjson"
    assert meta["sha256"] in headers["ETag"]


def test_put_is_idempotent_and_the_newest_wins(bridge):
    h, _ = bridge
    call(h.put_transcript, body=b"turn one\n")
    status, _, body = call(h.put_transcript, body=b"turn one\nturn two\n")
    meta = json.loads(body)
    assert meta["checkpoints"] == 2, "checkpoints counts the writes"
    _, _, got = call(h.get_transcript)
    assert got == b"turn one\nturn two\n", "the transcript is cumulative; newest wins"


def test_oversized_transcript_is_rejected_with_413(bridge):
    h, _ = bridge
    status, _, body = call(h.put_transcript, body=b"x" * 2048)   # cap is 1024
    assert status.startswith("413"), status
    assert "over the" in json.loads(body)["error"]
    # And nothing was stored.
    assert call(h.get_transcript)[0].startswith("404")


def test_empty_body_is_a_400(bridge):
    h, _ = bridge
    status, _, body = call(h.put_transcript, body=b"")
    assert status.startswith("400")
    assert "empty" in json.loads(body)["error"]


def test_unknown_correlationid_is_a_404_on_put(bridge):
    """Accepting a transcript for an id we never minted would let anyone seed
    arbitrary resume state for a session."""
    h, _ = bridge
    status, _, body = call(h.put_transcript, body=TRANSCRIPT, corr="never-minted-9999")
    assert status.startswith("404")
    assert "unknown correlationid" in json.loads(body)["error"]


def test_get_before_any_checkpoint_is_a_404(bridge):
    h, _ = bridge
    status, _, body = call(h.get_transcript)
    assert status.startswith("404")
    assert "no transcript" in json.loads(body)["error"]


def test_a_bad_correlationid_shape_is_rejected(bridge):
    h, _ = bridge
    assert call(h.put_transcript, body=TRANSCRIPT, corr="Bad_Corr!")[0].startswith("400")
    assert call(h.get_transcript, corr="Bad_Corr!")[0].startswith("400")


def test_a_lying_content_length_cannot_exceed_the_cap(bridge):
    """The declared length is checked first so an oversized body is never
    buffered, but the actual read is capped too."""
    h, _ = bridge
    environ = {"wsgi.input": io.BytesIO(b"y" * 5000), "CONTENT_LENGTH": "10",
               "QUERY_STRING": "", "REQUEST_METHOD": "PUT"}
    captured = {}
    h.put_transcript(environ, lambda s, hh: captured.setdefault("s", s),
                     correlationid="brave-otter-4718")
    assert captured["s"].startswith("200")
    _, _, got = call(h.get_transcript)
    assert len(got) == 10, "only the declared length is read"


def test_store_level_meta(tmp_path):
    st = Store(tmp_path / "eb")
    assert st.transcript_meta("nope") is None
    st.put_transcript("c1", TRANSCRIPT)
    meta = st.transcript_meta("c1")
    assert meta["size"] == len(TRANSCRIPT)
    assert len(meta["sha256"]) == 64


# ---- the runner-side client -------------------------------------------------

def test_find_local_locates_the_transcript_by_globbing(tmp_path):
    """The `projects/<name>` segment is a lossy encoding of cwd, so the client
    globs for the uuid rather than trying to reconstruct the directory name."""
    uuid = "eb4c5fe6-d5c2-44b6-a795-f42db7cd2dd5"
    d = tmp_path / "projects" / "-Users-someone--tmp-work-brave-otter-4718"
    d.mkdir(parents=True)
    f = d / f"{uuid}.jsonl"
    f.write_bytes(TRANSCRIPT)
    assert find_local(tmp_path, uuid) == f


def test_find_local_returns_none_when_absent(tmp_path):
    assert find_local(tmp_path, "no-such-uuid") is None


def test_upload_of_a_missing_file_is_a_logged_no_op_not_an_exception(tmp_path):
    ts = TranscriptStore("http://127.0.0.1:1", timeout=0.2)
    assert ts.upload("corr", tmp_path / "nope.jsonl") is False


def test_upload_refuses_to_exceed_the_cap(tmp_path):
    big = tmp_path / "big.jsonl"
    big.write_bytes(b"x" * 5000)
    ts = TranscriptStore("http://127.0.0.1:1", timeout=0.2, max_bytes=1000)
    assert ts.upload("corr", big) is False, "client-side cap avoids a doomed PUT"


def test_a_disabled_store_does_nothing(tmp_path):
    f = tmp_path / "t.jsonl"
    f.write_bytes(TRANSCRIPT)
    ts = TranscriptStore("http://127.0.0.1:1", enabled=False)
    assert ts.upload("corr", f) is False
    assert ts.download("corr", tmp_path / "out") is None


def test_download_failure_is_none_not_an_exception(tmp_path):
    ts = TranscriptStore("http://127.0.0.1:1", timeout=0.2)
    assert ts.download("corr", tmp_path / "out.jsonl") is None


# ---- T1.9: the runner's restore/checkpoint decisions ------------------------

class StubStore:
    """Stands in for EventBridge: records calls, serves a canned body."""

    def __init__(self, stored: bytes | None = None):
        self.stored = stored
        self.uploads = 0
        self.downloads = 0

    def restore_for_resume(self, corr, session, config_dir, scratch):
        local = find_local(config_dir, session)
        if local is not None:
            return str(local.resolve())
        if self.stored is None:
            return None
        self.downloads += 1
        dest = pathlib.Path(scratch) / f"{session}.jsonl"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.stored)
        return str(dest.resolve())

    def checkpoint_after_turn(self, corr, session, config_dir):
        self.uploads += 1
        return True


def _er_cfg(tmp_path) -> ErCfg:
    return ErCfg(tmpdir=str(tmp_path / "rk"),
                 claude_config_dir=str(tmp_path / "claude"))


def test_a_warm_pod_resumes_from_its_own_disk_without_an_http_round_trip(tmp_path):
    cfg = _er_cfg(tmp_path)
    session = ce.session_uuid("brave-otter-4718")
    d = pathlib.Path(cfg.claude_config_dir) / "projects" / "some-cwd"
    d.mkdir(parents=True)
    local = d / f"{session}.jsonl"
    local.write_bytes(TRANSCRIPT)

    stub = StubStore(stored=b"stale-remote-copy")
    got = resolve_resume_target(cfg, stub, "brave-otter-4718", session, tmp_path / "w")
    assert got == str(local.resolve())
    assert stub.downloads == 0, "the local copy is authoritative for a warm pod"


def test_a_cold_pod_fetches_the_checkpoint_and_resumes_by_path(tmp_path):
    """The Gap B case: this pod has never seen the session."""
    cfg = _er_cfg(tmp_path)
    session = ce.session_uuid("brave-otter-4718")
    stub = StubStore(stored=TRANSCRIPT)
    got = resolve_resume_target(cfg, stub, "brave-otter-4718", session, tmp_path / "w")
    assert stub.downloads == 1
    assert got is not None and pathlib.Path(got).is_absolute()
    assert pathlib.Path(got).read_bytes() == TRANSCRIPT


def test_no_transcript_anywhere_returns_none_so_the_caller_can_warn(tmp_path):
    cfg = _er_cfg(tmp_path)
    stub = StubStore(stored=None)
    assert resolve_resume_target(cfg, stub, "c", ce.session_uuid("c"), tmp_path / "w") is None


def test_a_raising_store_never_fails_the_turn(tmp_path):
    class Boom:
        def restore_for_resume(self, *a, **k):
            raise RuntimeError("EventBridge unreachable")

    cfg = _er_cfg(tmp_path)
    assert resolve_resume_target(cfg, Boom(), "c", "u", tmp_path / "w") is None


def test_build_cmd_passes_the_transcript_path_to_resume():
    cfg = ErCfg()
    corr = "brave-otter-4718"
    event = ce.CloudEvent(attrs={"correlationid": corr,
                                 "sessionuuid": ce.session_uuid(corr),
                                 "mode": "continue"})
    cmd = build_cmd(cfg, event, {"prompt": "go on"},
                    resume_target="/data/scratch/abc.jsonl")
    assert "--resume" in cmd
    assert cmd[cmd.index("--resume") + 1] == "/data/scratch/abc.jsonl"


def test_build_cmd_falls_back_to_the_session_uuid():
    cfg = ErCfg()
    corr = "brave-otter-4718"
    session = ce.session_uuid(corr)
    event = ce.CloudEvent(attrs={"correlationid": corr, "sessionuuid": session,
                                 "mode": "continue"})
    cmd = build_cmd(cfg, event, {"prompt": "go on"})
    assert cmd[cmd.index("--resume") + 1] == session


def test_a_start_turn_uses_session_id_not_resume():
    cfg = ErCfg()
    corr = "brave-otter-4718"
    session = ce.session_uuid(corr)
    event = ce.CloudEvent(attrs={"correlationid": corr, "sessionuuid": session,
                                 "mode": "start"})
    cmd = build_cmd(cfg, event, {"prompt": "hello"})
    assert "--session-id" in cmd and "--resume" not in cmd
    assert cmd[cmd.index("--session-id") + 1] == session


def test_exactly_one_upload_per_turn(tmp_path, monkeypatch):
    """T1.9: 'a terminal event triggers exactly one upload'. Runs the real
    run_agent against a fake `claude` that emits a result frame."""
    from eventrunner.runner import run_agent

    fake_claude = tmp_path / "fake-claude"
    fake_claude.write_text(
        "#!/bin/sh\n"
        'echo \'{"type":"assistant","message":{"role":"assistant","content":'
        '[{"type":"text","text":"hi"}]}}\'\n'
        'echo \'{"type":"result","result":"hi","num_turns":1,"duration_ms":5}\'\n'
        "exit 0\n")
    fake_claude.chmod(0o755)

    cfg = ErCfg(tmpdir=str(tmp_path / "rk"), claude_bin=str(fake_claude),
                claude_config_dir=str(tmp_path / "claude"), mock_claude=False,
                include_raw=False)
    pathlib.Path(cfg.tmpdir, "eventrunner").mkdir(parents=True, exist_ok=True)

    class CaptureEmitter:
        def __init__(self):
            self.events = []
            self._n = 0

        def next_seq(self, corr, start=None):
            self._n += 1
            return self._n

        def emit(self, **kw):
            self.events.append(kw)
            return "id"

    corr = "brave-otter-4718"
    event = ce.CloudEvent(attrs={"correlationid": corr,
                                 "sessionuuid": ce.session_uuid(corr),
                                 "mode": "start", "id": "req-evt-1"},
                          data={"prompt": "hello", "max_turns": 1})
    stub = StubStore()
    em = CaptureEmitter()
    run_agent(cfg, em, event, transcripts=stub)

    assert stub.uploads == 1, f"expected exactly one checkpoint, got {stub.uploads}"
    finals = [e for e in em.events if e.get("final")]
    assert len(finals) == 1, "claude's own result frame is promoted, not duplicated"
    # §11 causation binding: every response names the request that caused it.
    assert all(e.get("causationid") == "req-evt-1" for e in em.events), em.events


def test_the_checkpoint_still_happens_when_claude_exits_nonzero(tmp_path):
    """A failed turn may still have produced a transcript worth resuming from."""
    from eventrunner.runner import run_agent

    fake = tmp_path / "failing-claude"
    fake.write_text("#!/bin/sh\necho 'boom' >&2\nexit 1\n")
    fake.chmod(0o755)
    cfg = ErCfg(tmpdir=str(tmp_path / "rk"), claude_bin=str(fake),
                claude_config_dir=str(tmp_path / "claude"), mock_claude=False)
    pathlib.Path(cfg.tmpdir, "eventrunner").mkdir(parents=True, exist_ok=True)

    class E:
        def __init__(self):
            self.events = []
            self._n = 0

        def next_seq(self, corr, start=None):
            self._n += 1
            return self._n

        def emit(self, **kw):
            self.events.append(kw)
            return "i"

    corr = "brave-otter-4718"
    event = ce.CloudEvent(attrs={"correlationid": corr,
                                 "sessionuuid": ce.session_uuid(corr),
                                 "mode": "start", "id": "req-1"},
                          data={"prompt": "x"})
    stub = StubStore()
    em = E()
    run_agent(cfg, em, event, transcripts=stub)
    assert stub.uploads == 1
    assert any(e["phase"] == "error" and e["final"] for e in em.events)


# ---- sequence continuity across pods ----------------------------------------
#
# Found by running the demo flow on the cluster: a three-turn conversation stored
# SIX events instead of nine, and turn 1's events had been replaced by turn 3's.
#
# `Emitter` keeps its per-correlation sequence counter in memory. Phase 0 had one
# long-lived runner process so it was naturally monotonic; in Phase 1 every
# scale-from-zero is a new process starting at 1, and the store's
# `PRIMARY KEY (correlationid, sequence)` with INSERT OR REPLACE then silently
# overwrites the earlier turn. It also breaks RQ-1's claim that deduplicating on
# `(correlationid, sequence)` is sufficient.

class SeqStore(StubStore):
    """StubStore plus a canned answer for last_sequence()."""

    def __init__(self, last=0, **kw):
        super().__init__(**kw)
        self.last = last
        self.seed_calls = 0

    def last_sequence(self, corr):
        return self.last

    def seed_emitter(self, emitter, corr):
        self.seed_calls += 1
        if self.last:
            emitter.seed_seq(corr, self.last)
        return self.last


class RealEmitterSeq:
    """The real Emitter's sequence logic, without Kafka."""

    def __init__(self):
        from eventrunner.emit import Emitter
        self.seqs = []
        self.events = []
        # Borrow the real methods rather than reimplementing them.
        self.next_seq = Emitter.next_seq.__get__(self)
        self.seed_seq = Emitter.seed_seq.__get__(self)
        import threading
        self._seq_lock = threading.Lock()
        self._seq_by_corr = {}

    def emit(self, **kw):
        self.seqs.append(kw["sequence"])
        self.events.append(kw)
        return "id"


def test_a_fresh_emitter_restarts_sequences_at_one():
    """The bug, demonstrated: this is what a new pod does without seeding."""
    em = RealEmitterSeq()
    assert [em.next_seq("c") for _ in range(3)] == [1, 2, 3]
    fresh = RealEmitterSeq()          # a new pod
    assert [fresh.next_seq("c") for _ in range(3)] == [1, 2, 3], \
        "a new process genuinely restarts at 1 — hence the seeding"


def test_seeding_continues_the_numbering_from_the_store():
    em = RealEmitterSeq()
    em.seed_seq("c", 6)               # EventBridge already holds 1..6
    assert [em.next_seq("c") for _ in range(3)] == [7, 8, 9]


def test_seed_seq_never_rewinds():
    em = RealEmitterSeq()
    em.seed_seq("c", 10)
    em.seed_seq("c", 3)               # a stale/lower answer must not rewind
    assert em.next_seq("c") == 11


def test_a_cold_pod_does_not_overwrite_an_earlier_turns_events(tmp_path):
    """End to end over run_agent: turn 2 on a NEW pod must not reuse turn 1's
    sequences."""
    from eventrunner.runner import run_agent

    cfg = ErCfg(tmpdir=str(tmp_path / "rk"), claude_config_dir=str(tmp_path / "claude"),
                mock_claude=True)
    pathlib.Path(cfg.tmpdir, "eventrunner").mkdir(parents=True, exist_ok=True)
    corr = "brave-otter-4718"

    def event(mode, prompt, eid):
        return ce.CloudEvent(attrs={"correlationid": corr,
                                    "sessionuuid": ce.session_uuid(corr),
                                    "mode": mode, "id": eid},
                             data={"prompt": prompt})

    # Pod A, turn 1: nothing stored yet.
    pod_a = RealEmitterSeq()
    run_agent(cfg, pod_a, event("start", "one", "r1"), transcripts=SeqStore(last=0))
    assert pod_a.seqs == [1, 2, 3]

    # Pod B is a brand-new process. EventBridge holds 3 events.
    pod_b = RealEmitterSeq()
    store_b = SeqStore(last=max(pod_a.seqs))
    run_agent(cfg, pod_b, event("continue", "two", "r2"), transcripts=store_b)
    assert store_b.seed_calls == 1, "the runner must seed before emitting"
    assert pod_b.seqs == [4, 5, 6], (
        f"a cold pod reused sequences {pod_b.seqs} — those rows would overwrite "
        f"turn 1 in the store")
    assert not set(pod_a.seqs) & set(pod_b.seqs), "sequences must not collide"


def test_the_turn_still_runs_when_the_sequence_query_fails(tmp_path):
    """A seeding failure must degrade, not fail the turn."""
    from eventrunner.runner import run_agent

    class Broken(StubStore):
        def seed_emitter(self, emitter, corr):
            raise RuntimeError("EventBridge unreachable")

    cfg = ErCfg(tmpdir=str(tmp_path / "rk"), claude_config_dir=str(tmp_path / "claude"),
                mock_claude=True)
    pathlib.Path(cfg.tmpdir, "eventrunner").mkdir(parents=True, exist_ok=True)
    em = RealEmitterSeq()
    corr = "brave-otter-4718"
    run_agent(cfg, em, ce.CloudEvent(
        attrs={"correlationid": corr, "sessionuuid": ce.session_uuid(corr),
               "mode": "start", "id": "r1"}, data={"prompt": "x"}),
        transcripts=Broken())
    assert em.seqs == [1, 2, 3], "the turn must still complete"


def test_last_sequence_reads_the_highest_stored_sequence(monkeypatch):
    import io
    import urllib.request as ur

    from eventrunner.transcript import TranscriptStore

    payload = json.dumps({"events": [{"sequence": 1}, {"sequence": 7},
                                     {"sequence": 4}]}).encode()

    class R(io.BytesIO):
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(ur, "urlopen", lambda *a, **k: R(payload))
    assert TranscriptStore("http://eb:8080").last_sequence("c") == 7


def test_last_sequence_returns_none_when_it_cannot_tell(tmp_path):
    """None must be distinguishable from a genuine 0: seeding to 0 when the real
    answer is 6 is exactly the overwrite bug."""
    from eventrunner.transcript import TranscriptStore
    assert TranscriptStore("http://127.0.0.1:1", timeout=0.2).last_sequence("c") is None
