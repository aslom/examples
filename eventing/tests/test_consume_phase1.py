"""EventRunner consumer: retry, configurable group, deferred commits, replay guard.

Gates T1.1, T1.2, T1.5, T1.6 and the §16 Gap C guard (DESIGN_PHASE1.md §20).

The Phase 0 failure these exist to prevent is the quiet one: the consumer thread
raises, the main thread keeps running, and the pod reports healthy while
consuming nothing. So the central assertion in several of these tests is not
"it worked" but "the thread is still alive".
"""
import binascii
import datetime as dt
import json
import threading
import time

from kafka.errors import KafkaConnectionError
from kafka.structs import OffsetAndMetadata, TopicPartition

from eventrunner.config import Cfg, load
from eventrunner.consume import Consumer, event_age_s
from eventrunner.offsets import OffsetLedger
from shared import ce, keyset
from shared import signing as S
from shared.heartbeat import Heartbeat

# RFC 8032 vectors 1 and 2: an approved runner and a rogue. Fixed, never generated,
# so a failure is reproducible.
SEED = binascii.unhexlify(
    "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
PUB = S.public_key(SEED)
SEED2 = binascii.unhexlify(
    "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb")


def approved_keyset(tmp_path, keys=None):
    p = tmp_path / "agents.json"
    p.write_text(json.dumps(keys or {"runner-01": PUB.hex()}))
    return keyset.load(str(p))

# ---- fakes ------------------------------------------------------------------

class Rec:
    def __init__(self, topic, partition, offset, headers, value):
        self.topic, self.partition, self.offset = topic, partition, offset
        self.headers, self.value = headers, value


def request_record(corr="brave-otter-4718", *, offset=0, partition=0,
                   topic="kev1-requests", mode="start", when=None, prompt="hi",
                   seed=None, kid=None):
    """One Kafka record carrying a request event.

    `seed` signs it before serialisation, the same order `kafka_out.publish_request`
    uses — so a signed record here travels the wire exactly as a real one does,
    rather than being signed over attributes the codec would have changed.
    """
    attrs = {}
    if when is not None:
        attrs["time"] = when
    evt = ce.new_event(type=ce.TYPE_REQUEST, source="rossoctl://eventbridge/test",
                       datacontenttype="application/json",
                       correlationid=corr, sessionuuid=ce.session_uuid(corr),
                       mode=mode, data={"prompt": prompt}, **attrs)
    S.sign_into(evt, seed, kid)
    headers, value = ce.to_kafka_binary(evt)
    return Rec(topic, partition, offset, headers, value)


class FakeConsumer:
    """Enough KafkaConsumer surface for the loop, plus a record of every commit."""

    def __init__(self, batches, *, committed=None, topic="kev1-requests", partitions=(0,)):
        self._batches = list(batches)
        self.subscribed = None
        self.commits = []
        self.closed = False
        self.paused_tps = set()
        self._tps = {TopicPartition(topic, p) for p in partitions}
        self._committed = dict(committed or {})
        self._positions = {tp: 0 for tp in self._tps}

    def subscribe(self, topics):
        self.subscribed = list(topics)

    def assignment(self):
        return set(self._tps)

    def poll(self, timeout_ms=0, **kw):
        if self._batches:
            batch = self._batches.pop(0)
            for tp, recs in batch.items():
                if recs:
                    self._positions[tp] = recs[-1].offset + 1
            return batch
        time.sleep(0.01)
        return {}

    def commit(self, offsets):
        self.commits.append({(tp.topic, tp.partition): om.offset
                             for tp, om in offsets.items()})
        for tp, om in offsets.items():
            self._committed[tp] = om

    def committed(self, tp):
        return self._committed.get(tp)

    def position(self, tp):
        return self._positions.get(tp, 0)

    def pause(self, *tps):
        self.paused_tps.update(tps)

    def resume(self, *tps):
        self.paused_tps.difference_update(tps)

    def close(self):
        self.closed = True


class RecordingRouter:
    """Captures submissions and lets the test decide when a run 'finishes'."""

    def __init__(self):
        self.submitted = []
        self.dones = []

    def submit(self, event, on_done=None):
        self.submitted.append(event)
        self.dones.append(on_done)

    def finish(self, i=0):
        cb = self.dones[i]
        if cb:
            cb()

    def finish_all(self):
        for cb in self.dones:
            if cb:
                cb()


def cfg_for(tmp_path, **over) -> Cfg:
    base = dict(kafka_bootstrap="broker:9092", request_topic="kev1-requests",
                response_topic="kev1-responses", consumer_group="kev1-eventrunner",
                heartbeat_path=str(tmp_path / "hb"), kafka_retry_initial_s=0.01,
                kafka_retry_max_s=0.02, max_concurrent=4,
                max_request_age_s=3600.0, commit_after_terminal=True,
                final_commit_wait_s=0.2)
    base.update(over)
    return Cfg(**base)


def _until(predicate, timeout: float = 5.0, interval: float = 0.01) -> bool:
    """Wait for a condition instead of guessing how long it takes."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def _until(predicate, timeout: float = 5.0, interval: float = 0.01) -> bool:
    """Wait for a condition instead of guessing how long it takes."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def drive(consumer: Consumer, fake: FakeConsumer, *, polls: int = 3,
          until=None, timeout: float = 5.0) -> None:
    """Run the consume loop, then stop it.

    Prefer `until=<predicate>`: it returns as soon as the thing being asserted has
    happened, which is faster AND immune to CPU contention. The bounded-poll form is
    kept only for assertions about something NOT happening, where waiting a little is
    the point.

    A fixed sleep for something that SHOULD happen is a wall-clock race dressed as a
    test: it passes alone and flakes under load. Two tests in this file did exactly
    that before this helper existed.
    """
    loop = threading.Thread(target=lambda: consumer._consume_loop(fake), daemon=True)
    loop.start()
    try:
        if until is not None:
            _until(until, timeout=timeout)
        else:
            time.sleep(0.05 * polls)
    finally:
        consumer.stop()
        loop.join(timeout=5)


# ---- T1.2: the consumer group must be configurable --------------------------

def test_consumer_group_defaults_to_eventrunner():
    assert Cfg().consumer_group == "eventrunner"


def test_consumer_group_is_read_from_the_env(monkeypatch, tmp_path):
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.setenv("ER_CONSUMER_GROUP", "kev1-eventrunner")
    assert load().consumer_group == "kev1-eventrunner"


def test_the_configured_group_actually_reaches_the_kafka_consumer(tmp_path):
    """A group that never reaches KafkaConsumer is the silent no-scale failure:
    KEDA watches `kev1-eventrunner` while the pod joins `eventrunner`."""
    seen = {}

    class Cap(Consumer):
        def _build_consumer(self):
            seen["group"] = self._cfg.consumer_group
            seen["bootstrap"] = self._cfg.kafka_bootstrap
            return FakeConsumer([])

    c = Cap(cfg_for(tmp_path, consumer_group="kev1-eventrunner"), RecordingRouter())
    got = c._connect_with_retry()
    assert seen["group"] == "kev1-eventrunner"
    assert got.subscribed == ["kev1-requests"]


# ---- T1.1: construction retry, and the thread must never die ---------------

def test_construction_retries_until_it_succeeds(tmp_path):
    """kafka-python 3.x removed `NoBrokersAvailable`; an unreachable broker now
    surfaces as KafkaConnectionError. The retry catches bare Exception precisely
    so a class rename cannot reintroduce the dead-thread bug."""
    attempts = {"n": 0}

    def factory():
        attempts["n"] += 1
        if attempts["n"] <= 3:
            raise KafkaConnectionError("broker still rolling")
        return FakeConsumer([])

    c = Consumer(cfg_for(tmp_path), RecordingRouter(), consumer_factory=factory)
    got = c._connect_with_retry()
    assert attempts["n"] == 4, "three failures then success"
    assert got is not None and got.subscribed == ["kev1-requests"]


def test_retry_backs_off_but_is_capped(tmp_path):
    delays = []
    real_wait = threading.Event.wait

    def factory():
        raise KafkaConnectionError("nope")

    c = Consumer(cfg_for(tmp_path, kafka_retry_initial_s=1.0, kafka_retry_max_s=4.0),
                 RecordingRouter(), consumer_factory=factory)

    def fake_wait(self, timeout=None):
        delays.append(timeout)
        return len(delays) >= 6      # stop after six attempts

    threading.Event.wait = fake_wait
    try:
        assert c._connect_with_retry() is None
    finally:
        threading.Event.wait = real_wait
    assert delays[:4] == [1.0, 2.0, 4.0, 4.0], f"expected capped backoff, got {delays}"


def test_a_failing_factory_keeps_the_heartbeat_fresh(tmp_path):
    """A pod waiting for a rolling broker is alive and must not be restarted."""
    hb = Heartbeat(tmp_path / "hb")
    calls = {"n": 0}

    def factory():
        calls["n"] += 1
        if calls["n"] <= 2:
            raise KafkaConnectionError("nope")
        return FakeConsumer([])

    c = Consumer(cfg_for(tmp_path), RecordingRouter(), heartbeat=hb,
                 consumer_factory=factory)
    c._connect_with_retry()
    assert hb.age_s() is not None, "heartbeat must be touched while retrying"


def test_the_thread_survives_an_exception_in_the_loop(tmp_path):
    """THE Phase 0 bug. The consumer thread raised and died while the main
    thread kept running; the pod stayed Running and consumed nothing forever."""
    boom = {"n": 0}

    class Exploding(FakeConsumer):
        def poll(self, timeout_ms=0, **kw):
            boom["n"] += 1
            if boom["n"] <= 2:
                raise RuntimeError("simulated broker hiccup")
            time.sleep(0.01)
            return {}

    built = {"n": 0}

    def factory():
        built["n"] += 1
        return Exploding([])

    c = Consumer(cfg_for(tmp_path), RecordingRouter(), consumer_factory=factory)
    c.start()
    time.sleep(0.4)
    alive = c.is_alive()
    c.stop()
    c.join(timeout=3)
    assert alive, "the consumer thread must survive a raising poll, not die"
    assert built["n"] >= 2, "it should have reconnected after the failure"


def test_stop_ends_the_thread_cleanly(tmp_path):
    c = Consumer(cfg_for(tmp_path), RecordingRouter(),
                 consumer_factory=lambda: FakeConsumer([]))
    c.start()
    time.sleep(0.1)
    c.stop()
    c.join(timeout=3)
    assert not c.is_alive()


# ---- T1.5: commit only after the terminal event -----------------------------

def test_no_commit_happens_before_the_run_finishes(tmp_path):
    tp = TopicPartition("kev1-requests", 0)
    fake = FakeConsumer([{tp: [request_record(offset=0)]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path), router, ledger=OffsetLedger())
    drive(c, fake, polls=2)   # timed on purpose: asserts ABSENCE
    assert len(router.submitted) == 1
    assert fake.commits == [], "lag must stay >=1 for the whole run (RQ-1)"


def test_the_commit_lands_once_the_run_reports_done(tmp_path):
    """Event-driven rather than timed.

    An earlier version slept 0.06s in a thread and drove a fixed number of polls,
    which passed in isolation and flaked under load — a wall-clock race dressed up
    as a test. Now it waits for each step to actually happen: the record to be
    submitted, then the commit to appear.
    """
    tp = TopicPartition("kev1-requests", 0)
    rec = request_record(offset=7)
    fake = FakeConsumer([{tp: [rec]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path), router, ledger=OffsetLedger())

    loop = threading.Thread(target=lambda: c._consume_loop(fake), daemon=True)
    loop.start()
    try:
        assert _until(lambda: router.submitted), "the record was never submitted"
        assert fake.commits == [], "nothing may commit before the run finishes"
        router.finish_all()
        assert _until(lambda: {("kev1-requests", 0): 8} in fake.commits), \
            f"commit never landed; got {fake.commits}"
    finally:
        c.stop()
        loop.join(timeout=5)


def test_legacy_mode_commits_immediately(tmp_path):
    """ER_COMMIT_AFTER_TERMINAL=false restores Phase 0 behaviour — kept only so
    the difference is demonstrable."""
    tp = TopicPartition("kev1-requests", 0)
    fake = FakeConsumer([{tp: [request_record(offset=0)]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    c = Consumer(cfg_for(tmp_path, commit_after_terminal=False), RecordingRouter())
    drive(c, fake, until=lambda: fake.commits)
    assert {("kev1-requests", 0): 1} in fake.commits


def test_a_malformed_event_still_advances_the_offset(tmp_path):
    """No correlationid means we can never process it; not committing would make
    it a poison pill that blocks the partition forever."""
    tp = TopicPartition("kev1-requests", 0)
    bad = Rec("kev1-requests", 0, 3, [("ce_type", b"x")], b"{}")
    fake = FakeConsumer([{tp: [bad]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path), router)
    drive(c, fake, until=lambda: fake.commits)
    assert router.submitted == []
    assert {("kev1-requests", 0): 4} in fake.commits


# ---- T1.6 / Gap C: the group always has a committed offset ------------------

def test_a_fresh_group_gets_a_starting_offset_committed(tmp_path):
    """A group with no committed offset, idled past offsets.retention, falls back
    to `earliest` and replays. Seeding means every wake inside that window
    resumes from a real position."""
    fake = FakeConsumer([{}], committed={})       # committed() answers None
    c = Consumer(cfg_for(tmp_path), RecordingRouter())
    drive(c, fake, until=lambda: fake.commits)
    assert fake.commits, "a fresh group must get a starting offset"
    assert fake.commits[0] == {("kev1-requests", 0): 0}


def test_an_existing_committed_offset_is_not_overwritten(tmp_path):
    tp = TopicPartition("kev1-requests", 0)
    fake = FakeConsumer([{}], committed={tp: OffsetAndMetadata(42, "", -1)})
    c = Consumer(cfg_for(tmp_path), RecordingRouter())
    drive(c, fake, polls=2)   # timed on purpose: asserts ABSENCE
    assert fake.commits == [], "seeding must not rewind a group that already has offsets"


def test_seeding_uses_the_first_record_offset_when_the_poll_already_fetched(tmp_path):
    """position() is past records this poll returned, so seeding from it would
    skip them."""
    tp = TopicPartition("kev1-requests", 0)
    recs = [request_record(offset=5), request_record(offset=6)]
    fake = FakeConsumer([{tp: recs}], committed={})
    c = Consumer(cfg_for(tmp_path), RecordingRouter())
    drive(c, fake, until=lambda: fake.commits)
    assert fake.commits[0] == {("kev1-requests", 0): 5}


def test_seeding_happens_once_per_partition(tmp_path):
    fake = FakeConsumer([{}, {}, {}], committed={})
    c = Consumer(cfg_for(tmp_path), RecordingRouter())
    drive(c, fake, until=lambda: fake.commits and not fake._batches)
    seeds = [x for x in fake.commits if x == {("kev1-requests", 0): 0}]
    assert len(seeds) == 1, f"seeded more than once: {fake.commits}"


# ---- §16 Gap C: the stale-request replay guard ------------------------------

def _iso(seconds_ago: float) -> str:
    t = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=seconds_ago)
    return t.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def test_event_age_parses_the_ce_time_attribute():
    rec = request_record(when=_iso(120))
    evt = ce.from_kafka_binary(rec.headers, rec.value)
    age = event_age_s(evt)
    assert age is not None and 110 < age < 130


def test_event_age_is_none_for_an_unparseable_time():
    evt = ce.CloudEvent(attrs={"time": "not-a-date", "correlationid": "x"})
    assert event_age_s(evt) is None
    assert event_age_s(ce.CloudEvent(attrs={"correlationid": "x"})) is None


def test_a_day_old_replayed_request_is_dropped_not_re_run(tmp_path):
    """The replay storm: one new task after a week idle would otherwise re-run a
    whole day of retained requests, at real API cost."""
    tp = TopicPartition("kev1-requests", 0)
    old = request_record(offset=0, when=_iso(86400))
    fake = FakeConsumer([{tp: [old]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path, max_request_age_s=3600), router)
    drive(c, fake, until=lambda: fake.commits)
    assert router.submitted == [], "a stale request must not reach the router"
    assert c.skipped_stale == 1
    assert {("kev1-requests", 0): 1} in fake.commits, "but its offset must advance"


def test_a_fresh_request_passes_the_guard(tmp_path):
    tp = TopicPartition("kev1-requests", 0)
    fresh = request_record(offset=0, when=_iso(5))
    fake = FakeConsumer([{tp: [fresh]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path, max_request_age_s=3600), router)
    drive(c, fake, until=lambda: router.submitted)
    assert len(router.submitted) == 1
    assert c.skipped_stale == 0


def test_the_guard_can_be_disabled(tmp_path):
    tp = TopicPartition("kev1-requests", 0)
    old = request_record(offset=0, when=_iso(86400))
    fake = FakeConsumer([{tp: [old]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path, max_request_age_s=0), router)
    drive(c, fake, until=lambda: router.submitted)
    assert len(router.submitted) == 1, "max_request_age_s=0 disables the guard"


# ---- backpressure + drain intake gate --------------------------------------

def test_fetching_pauses_while_too_much_work_is_in_flight(tmp_path):
    tp = TopicPartition("kev1-requests", 0)
    recs = [request_record(corr=f"corr-{i}", offset=i) for i in range(10)]
    fake = FakeConsumer([{tp: recs}], committed={tp: OffsetAndMetadata(0, "", -1)})
    c = Consumer(cfg_for(tmp_path, max_concurrent=2), RecordingRouter())
    drive(c, fake, until=lambda: fake.paused_tps)
    assert fake.paused_tps, "with 10 runs in flight and a cap of 4, fetching must pause"


def test_stop_intake_pauses_fetching_but_keeps_committing(tmp_path):
    """§8.3: during a drain the poll loop must stay alive, because it is what
    commits the offsets of the runs that are finishing."""
    tp = TopicPartition("kev1-requests", 0)
    rec = request_record(offset=0)
    fake = FakeConsumer([{tp: [rec]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path), router)

    def sequence():
        time.sleep(0.05)
        c.stop_intake()        # SIGTERM arrives
        time.sleep(0.05)
        router.finish_all()    # the in-flight run completes during the drain
        time.sleep(0.10)
        c.stop()
    threading.Thread(target=sequence, daemon=True).start()
    c._consume_loop(fake)
    assert {("kev1-requests", 0): 1} in fake.commits, \
        "a commit must still land while draining"
    assert fake.paused_tps, "intake stops by pausing the assignment"


# ---- §11: signature verification, the reject branches ------------------------
#
# `consume.py`'s verification branch had no tests at all: until this change nothing
# signed, so there was no way to reach the accept side and the reject side was
# indistinguishable from "always rejects". Each test below asserts the same three
# things the stale-request guard does, because a rejection that loses the offset is a
# poison pill that blocks the partition forever:
#   1. the request did not reach the router,
#   2. a named counter says why it was refused,
#   3. the offset still advanced.

def _signed_setup(tmp_path, *, seed, kid, ks=None, **cfg_over):
    tp = TopicPartition("kev1-requests", 0)
    rec = request_record(offset=0, seed=seed, kid=kid)
    fake = FakeConsumer([{tp: [rec]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    cfg = cfg_for(tmp_path, require_signature=True, **cfg_over)
    return fake, router, Consumer(cfg, router, keyset=ks)


def test_a_request_from_an_approved_agent_runs(tmp_path):
    """The accept side, which was unreachable before anything signed."""
    fake, router, c = _signed_setup(tmp_path, seed=SEED, kid="runner-01",
                                   ks=approved_keyset(tmp_path))
    drive(c, fake, until=lambda: router.submitted)
    assert len(router.submitted) == 1, "an approved, correctly signed request must run"
    assert c.rejected_unsigned == 0


def test_an_unsigned_request_is_rejected_when_signatures_are_required(tmp_path):
    fake, router, c = _signed_setup(tmp_path, seed=None, kid=None,
                                   ks=approved_keyset(tmp_path))
    drive(c, fake, until=lambda: fake.commits)
    assert router.submitted == [], "an unsigned request must not reach the router"
    assert c.rejected_unsigned == 1
    assert {("kev1-requests", 0): 1} in fake.commits, "but its offset must advance"


def test_a_request_signed_by_an_unapproved_key_is_rejected(tmp_path):
    """The forgery the demo turns on: a real signature from a key nobody approved.

    The attacker holds a valid Ed25519 key and can produce a structurally perfect
    signature. Being unforgeable is not the point — being *unapproved* is.
    """
    fake, router, c = _signed_setup(tmp_path, seed=SEED2, kid="runner-99",
                                   ks=approved_keyset(tmp_path))
    drive(c, fake, until=lambda: fake.commits)
    assert router.submitted == [], "an unapproved signer must not reach the router"
    assert c.rejected_unsigned == 1
    assert {("kev1-requests", 0): 1} in fake.commits


def test_a_rogue_key_claiming_an_approved_kid_is_rejected(tmp_path):
    """The nastier case: the attacker knows an approved kid but not its key. The kid
    resolves, so only the signature check stands between them and a run."""
    fake, router, c = _signed_setup(tmp_path, seed=SEED2, kid="runner-01",
                                   ks=approved_keyset(tmp_path))
    drive(c, fake, until=lambda: fake.commits)
    assert router.submitted == []
    assert c.rejected_unsigned == 1


def test_an_unnamed_token_is_rejected_when_the_approved_set_is_ambiguous(tmp_path):
    """Two approved keys and a token naming neither: accepting it would mean taking a
    signature from ANY approved agent for an event that claimed none of them."""
    ks = approved_keyset(tmp_path, {"runner-01": PUB.hex(),
                                    "runner-02": S.public_key(SEED2).hex()})
    fake, router, c = _signed_setup(tmp_path, seed=SEED, kid=None, ks=ks)
    drive(c, fake, until=lambda: fake.commits)
    assert router.submitted == []
    assert c.rejected_unsigned == 1


def test_an_unnamed_token_runs_against_a_single_key_set(tmp_path):
    """The friendly deployment: one approved key, so nothing has to name it."""
    fake, router, c = _signed_setup(tmp_path, seed=SEED, kid=None,
                                   ks=approved_keyset(tmp_path))
    drive(c, fake, until=lambda: router.submitted)
    assert len(router.submitted) == 1


def test_the_single_key_path_still_works_without_a_keyset(tmp_path):
    """Back-compat: ER_REQUIRE_SIGNATURE with only ER_VERIFY_KEY_PATH predates the
    keyset and is still the simplest useful deployment."""
    key = tmp_path / "verify.hex"
    key.write_text(PUB.hex())
    fake, router, c = _signed_setup(tmp_path, seed=SEED, kid=None, ks=None,
                                   verify_key_path=str(key))
    drive(c, fake, until=lambda: router.submitted)
    assert len(router.submitted) == 1, "a single configured key must still verify"


def test_verification_is_skipped_entirely_when_not_required(tmp_path):
    """The default path: an unsigned request runs, and no signing work happens."""
    tp = TopicPartition("kev1-requests", 0)
    fake = FakeConsumer([{tp: [request_record(offset=0)]}],
                        committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path), router)          # require_signature defaults False
    drive(c, fake, until=lambda: router.submitted)
    assert len(router.submitted) == 1
    assert c.rejected_unsigned == 0


def test_a_stale_request_is_dropped_before_its_signature_is_checked(tmp_path):
    """Ordering, pinned: a replayed day-old event is history, not an attack, and must
    not cost a ~150 ms verification each to discard."""
    tp = TopicPartition("kev1-requests", 0)
    old = request_record(offset=0, when=_iso(86400))   # stale AND unsigned
    fake = FakeConsumer([{tp: [old]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path, require_signature=True, max_request_age_s=3600),
                 router, keyset=approved_keyset(tmp_path))
    drive(c, fake, until=lambda: fake.commits)
    assert router.submitted == []
    assert c.skipped_stale == 1, "the age guard must fire..."
    assert c.rejected_unsigned == 0, "...instead of the signature check"


def test_the_consumer_thread_survives_a_rejected_request(tmp_path):
    """The failure mode this whole file exists for: a reject path that kills the
    thread leaves a pod that reports healthy and consumes nothing."""
    tp = TopicPartition("kev1-requests", 0)
    bad = request_record(corr="brave-otter-4718", offset=0, seed=SEED2, kid="runner-99")
    good = request_record(corr="calm-badger-1234", offset=1, seed=SEED, kid="runner-01")
    fake = FakeConsumer([{tp: [bad, good]}], committed={tp: OffsetAndMetadata(0, "", -1)})
    router = RecordingRouter()
    c = Consumer(cfg_for(tmp_path, require_signature=True), router,
                 keyset=approved_keyset(tmp_path))
    drive(c, fake, until=lambda: router.submitted)
    assert c.rejected_unsigned == 1, "the first record was refused"
    assert len(router.submitted) == 1, "and the loop went on to process the second"
    assert router.submitted[0]["correlationid"] == "calm-badger-1234"
