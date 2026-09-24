"""SQLite Store: insert, filter by sequence, SSE notify."""
import tempfile
import threading
import pathlib

from eventbridge.store import Store


def test_insert_and_events_for(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "test-otter-0001"
    for seq in range(1, 4):
        s.insert_response({
            "correlationid": corr, "sequence": seq, "phase": "stdout",
            "id": f"id-{seq}", "time": "2026-09-21T00:00:00Z",
            "data": {"seq": seq}, "final": "false",
        })
    all_evts = s.events_for(corr)
    assert [e["sequence"] for e in all_evts] == [1, 2, 3]

    since1 = s.events_for(corr, since_seq=1)
    assert [e["sequence"] for e in since1] == [2, 3]


def test_final_detected(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "test-otter-0002"
    s.insert_response({"correlationid": corr, "sequence": 1, "phase": "stdout",
                       "id": "a", "time": "t", "data": {}, "final": "false"})
    assert not s.final_seen(corr)
    s.insert_response({"correlationid": corr, "sequence": 2, "phase": "result",
                       "id": "b", "time": "t", "data": {}, "final": "true"})
    assert s.final_seen(corr)


def test_subscribe_notifies_on_insert(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "test-otter-0003"
    ev = s.subscribe(corr)
    def emit():
        s.insert_response({"correlationid": corr, "sequence": 1, "phase": "stdout",
                           "id": "x", "time": "t", "data": {}, "final": "false"})
    threading.Timer(0.05, emit).start()
    assert ev.wait(timeout=1.0), "subscriber was not notified"
    s.unsubscribe(corr, ev)


def test_upsert_session_increments_turns(tmp_path: pathlib.Path):
    s = Store(tmp_path)
    corr = "test-otter-0004"
    s.upsert_session(corr, "uuid-x", "/tmp/work", "first")
    s.upsert_session(corr, "uuid-x", "/tmp/work", None)
    s.upsert_session(corr, "uuid-x", "/tmp/work", None)
    session = s.get_session(corr)
    assert session and session["turns"] == 2
