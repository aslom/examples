"""OffsetLedger — commits only the contiguous completed prefix. RQ-1 / T1.5.

Phase 0 committed the moment a message was handed to the Router, so lag hit zero
while `claude` was still streaming and KEDA scaled the pod away mid-run. Moving
the commit after the terminal event makes lag mean "work not finished" — but
Kafka commits a *position*, not individual messages, so with several runs in
flight finishing out of order, committing the newest completed offset would
silently discard the older unfinished ones.
"""
from eventrunner.offsets import OffsetLedger


def test_nothing_is_committable_before_anything_completes():
    L = OffsetLedger()
    for off in (0, 1, 2):
        L.submit("kev1-requests", 0, off)
    assert L.committable() == {}
    assert L.in_flight() == 3


def test_a_completed_prefix_commits_the_next_offset():
    L = OffsetLedger()
    for off in (10, 11, 12):
        L.submit("t", 0, off)
    L.complete("t", 0, 10)
    # Kafka commits the offset of the NEXT message to read, so finishing 10 means 11.
    assert L.committable() == {("t", 0): 11}


def test_out_of_order_completion_does_not_commit_past_unfinished_work():
    """The whole point. If run at offset 11 finishes before run at offset 10,
    committing 12 would mark 10 as done and lose it on a pod restart."""
    L = OffsetLedger()
    for off in (10, 11, 12):
        L.submit("t", 0, off)
    L.complete("t", 0, 11)
    L.complete("t", 0, 12)
    assert L.committable() == {}, "offset 10 is still running"
    L.complete("t", 0, 10)
    assert L.committable() == {("t", 0): 13}, "now the whole prefix is done"


def test_take_committable_forgets_what_it_covered():
    L = OffsetLedger()
    for off in (5, 6, 7):
        L.submit("t", 0, off)
    L.complete("t", 0, 5)
    L.complete("t", 0, 6)
    assert L.take_committable() == {("t", 0): 7}
    assert L.pending("t", 0) == [7]
    assert L.in_flight() == 1
    assert L.committable() == {}


def test_committable_does_not_mutate_so_a_failed_commit_can_retry():
    L = OffsetLedger()
    L.submit("t", 0, 1)
    L.complete("t", 0, 1)
    assert L.committable() == {("t", 0): 2}
    assert L.committable() == {("t", 0): 2}, "a failed broker commit must be retryable"
    L.take_committable()
    assert L.committable() == {}


def test_partitions_are_tracked_independently():
    L = OffsetLedger()
    L.submit("t", 0, 100)
    L.submit("t", 1, 200)
    L.complete("t", 1, 200)
    assert L.committable() == {("t", 1): 201}
    assert L.in_flight() == 1


def test_topics_are_tracked_independently():
    L = OffsetLedger()
    L.submit("a", 0, 1)
    L.submit("b", 0, 1)
    L.complete("a", 0, 1)
    assert L.committable() == {("a", 0): 2}


def test_complete_is_idempotent():
    L = OffsetLedger()
    L.submit("t", 0, 1)
    L.complete("t", 0, 1)
    L.complete("t", 0, 1)
    assert L.committable() == {("t", 0): 2}
    assert L.in_flight() == 0


def test_forget_drops_revoked_partitions():
    """After a rebalance another consumer owns the partition; committing on its
    behalf would be wrong."""
    L = OffsetLedger()
    L.submit("t", 0, 1)
    L.submit("t", 1, 1)
    L.complete("t", 0, 1)
    L.forget([("t", 0)])
    assert L.committable() == {}
    assert L.pending("t", 0) == []
    assert L.pending("t", 1) == [1]


def test_in_flight_counts_only_unfinished():
    L = OffsetLedger()
    for off in range(5):
        L.submit("t", 0, off)
    assert L.in_flight() == 5
    L.complete("t", 0, 3)
    assert L.in_flight() == 4


def test_snapshot_reports_bookkeeping():
    L = OffsetLedger()
    L.submit("t", 0, 1)
    L.submit("t", 1, 1)
    L.complete("t", 0, 1)
    assert L.snapshot() == {"partitions": 2, "submitted": 2, "done": 1}


def test_concurrent_completion_is_thread_safe():
    import threading
    L = OffsetLedger()
    n = 300
    for off in range(n):
        L.submit("t", 0, off)

    def worker(lo, hi):
        for off in range(lo, hi):
            L.complete("t", 0, off)

    threads = [threading.Thread(target=worker, args=(i, i + 30)) for i in range(0, n, 30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert L.in_flight() == 0
    assert L.committable() == {("t", 0): n}
