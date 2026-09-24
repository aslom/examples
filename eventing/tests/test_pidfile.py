"""PidFile: writes on enter, removes on exit, guards against double-start."""
import os
import pathlib

import pytest

from shared.pidfile import PidFile, _pid_alive


def test_writes_and_removes(tmp_path: pathlib.Path):
    with PidFile("eb-test", directory=tmp_path) as pf:
        assert pf.path.exists()
        assert pf.path.read_text().strip() == str(os.getpid())
    assert not pf.path.exists(), "pidfile should be cleaned up on exit"


def test_stale_pidfile_is_reclaimed(tmp_path: pathlib.Path):
    # A pid that will definitely not be alive
    (tmp_path / "eb-test.pid").write_text("999999\n")
    with PidFile("eb-test", directory=tmp_path) as pf:
        assert pf.path.read_text().strip() == str(os.getpid())


def test_live_pidfile_refuses_start(tmp_path: pathlib.Path, monkeypatch):
    # Use *our own* pid — guaranteed to be alive
    (tmp_path / "eb-test.pid").write_text(f"{os.getpid()}\n")
    monkeypatch.delenv("RUN_FORCE", raising=False)
    with pytest.raises(SystemExit) as exc:
        with PidFile("eb-test", directory=tmp_path):
            pass
    assert "already running" in str(exc.value)
    # Pidfile must remain untouched so the running process's cleanup still works
    assert (tmp_path / "eb-test.pid").read_text().strip() == str(os.getpid())


def test_run_force_overrides(tmp_path: pathlib.Path, monkeypatch):
    (tmp_path / "eb-test.pid").write_text(f"{os.getpid()}\n")
    monkeypatch.setenv("RUN_FORCE", "1")
    with PidFile("eb-test", directory=tmp_path) as pf:
        assert pf.path.read_text().strip() == str(os.getpid())


def test_pid_alive_zero_and_negative_are_dead():
    assert not _pid_alive(0)
    assert not _pid_alive(-1)


def test_pid_alive_self_is_true():
    assert _pid_alive(os.getpid())
