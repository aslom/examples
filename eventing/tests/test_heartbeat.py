"""Heartbeat file — detects a live process with a dead thread. §8.4 / T1.4.

Gate: "the loop touches the file; a frozen clock makes staleness detectable."
The clock is injected precisely so this needs no sleeping.
"""

from shared.heartbeat import Heartbeat


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def test_touch_creates_the_file_and_parents(tmp_path):
    hb = Heartbeat(tmp_path / "nested" / "deeper" / "heartbeat")
    hb.touch()
    assert hb.path.exists()


def test_age_is_zero_right_after_a_touch(tmp_path):
    clock = Clock()
    hb = Heartbeat(tmp_path / "hb", clock=clock)
    hb.touch()
    assert hb.age_s() == 0.0


def test_a_frozen_clock_makes_staleness_detectable(tmp_path):
    clock = Clock()
    hb = Heartbeat(tmp_path / "hb", clock=clock)
    hb.touch()
    assert hb.is_stale(90) is False
    clock.advance(89)
    assert hb.is_stale(90) is False
    clock.advance(2)
    assert hb.age_s() == 91
    assert hb.is_stale(90) is True


def test_touching_again_clears_staleness(tmp_path):
    clock = Clock()
    hb = Heartbeat(tmp_path / "hb", clock=clock)
    hb.touch()
    clock.advance(500)
    assert hb.is_stale(90) is True
    hb.touch()
    assert hb.is_stale(90) is False


def test_a_missing_file_counts_as_stale(tmp_path):
    """EventRunner touches the heartbeat before it ever reaches Kafka, so past
    initialDelaySeconds an absent file means the process never got to its loop."""
    hb = Heartbeat(tmp_path / "never-written")
    assert hb.age_s() is None
    assert hb.is_stale(90) is True


def test_describe_is_readable_in_a_probe_failure(tmp_path):
    clock = Clock()
    hb = Heartbeat(tmp_path / "hb", clock=clock)
    assert "missing" in hb.describe(90)
    hb.touch()
    assert "ok" in hb.describe(90)
    clock.advance(200)
    assert "STALE" in hb.describe(90)
    assert "age=200.0s" in hb.describe(90)


def test_write_is_atomic_so_a_probe_never_reads_a_half_file(tmp_path):
    hb = Heartbeat(tmp_path / "hb")
    hb.touch()
    hb.touch()
    # No stray temp file left behind, and the content parses as a float.
    assert list(p.name for p in tmp_path.iterdir()) == ["hb"]
    assert float(hb.path.read_text().strip()) > 0


def test_healthcheck_cli_exit_codes(tmp_path, monkeypatch):
    from eventrunner import healthcheck
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    hbfile = tmp_path / "hb"
    monkeypatch.setenv("ER_HEARTBEAT_PATH", str(hbfile))

    assert healthcheck.main(["--max-age", "90"]) == 1, "missing heartbeat -> unhealthy"
    Heartbeat(hbfile).touch()
    assert healthcheck.main(["--max-age", "90"]) == 0, "fresh heartbeat -> healthy"
    assert healthcheck.main(["--max-age", "-1"]) == 1, "any age exceeds a negative max"
