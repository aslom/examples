"""Verify EventBridge and EventRunner exit cleanly on SIGINT.

Skipped when Kafka is not reachable or the venv interpreter cannot be spawned
under the current sandbox — the sandbox check runs once and skips both tests
uniformly.
"""
import os
import pathlib
import signal
import socket
import subprocess
import sys
import time
import urllib.request

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
VENV_PY = ROOT / ".venv" / "bin" / "python"


def _kafka_reachable() -> bool:
    try:
        with socket.create_connection(("localhost", 9092), timeout=0.5):
            return True
    except OSError:
        return False


def _can_spawn_venv() -> bool:
    try:
        r = subprocess.run([str(VENV_PY), "-c", "print(1)"], capture_output=True, timeout=5)
        return r.returncode == 0 and r.stdout.strip() == b"1"
    except (PermissionError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


_SPAWN_OK = _can_spawn_venv()
_KAFKA_OK = _kafka_reachable()
skip_reason = None
if not _SPAWN_OK:
    skip_reason = "sandbox denies subprocess spawn of the venv python"
elif not _KAFKA_OK:
    skip_reason = "kafka not reachable on localhost:9092"


pytestmark = pytest.mark.skipif(skip_reason is not None, reason=skip_reason or "")


def _wait_healthz(port: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=0.5) as r:
                if r.status == 200:
                    return True
        except Exception:
            pass
        time.sleep(0.15)
    return False


def _run_and_signal(module: str, env_extra: dict[str, str], wait_ready) -> tuple[int, float, str]:
    env = os.environ.copy()
    env.update(env_extra)
    env["PYTHONUNBUFFERED"] = "1"
    proc = subprocess.Popen(
        [str(VENV_PY), "-m", module],
        cwd=str(ROOT),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        env=env, text=True,
    )
    try:
        assert wait_ready(proc), f"{module} did not become ready"
        t0 = time.monotonic()
        proc.send_signal(signal.SIGINT)
        rc = proc.wait(timeout=10.0)
        dt = time.monotonic() - t0
        out = proc.stdout.read() if proc.stdout else ""
        return rc, dt, out
    except subprocess.TimeoutExpired:
        proc.kill(); proc.communicate()
        pytest.fail(f"{module} did not exit within 10s of SIGINT")
    finally:
        if proc.poll() is None:
            proc.kill(); proc.communicate()


def test_eventbridge_shuts_down_cleanly_on_sigint():
    port = 8091
    def ready(_): return _wait_healthz(port, timeout=8.0)
    rc, dt, out = _run_and_signal(
        "eventbridge",
        {"EB_HTTP_ADDR": f"127.0.0.1:{port}"},
        ready,
    )
    assert rc == 0, f"expected rc=0, got {rc}. output:\n{out}"
    assert dt < 8.0, f"shutdown took {dt:.2f}s, expected <8s. output:\n{out}"
    assert "shut down" in out, f"missing shutdown message in output:\n{out}"


def test_eventrunner_shuts_down_cleanly_on_sigint():
    def ready(proc):
        # EventRunner has no HTTP port; wait for the startup banner instead.
        deadline = time.monotonic() + 6.0
        assert proc.stdout is not None
        buf = ""
        while time.monotonic() < deadline:
            line = proc.stdout.readline()
            if not line:
                if proc.poll() is not None:
                    return False
                continue
            buf += line
            if "[eventrunner]" in line and "bootstrap=" in line:
                # give the KafkaConsumer thread a moment to actually join the group
                time.sleep(0.5)
                return True
        return False

    rc, dt, out = _run_and_signal(
        "eventrunner",
        {"ER_MOCK_CLAUDE": "true"},
        ready,
    )
    assert rc == 0, f"expected rc=0, got {rc}. output:\n{out}"
    assert dt < 8.0, f"shutdown took {dt:.2f}s, expected <8s. output:\n{out}"
    assert "shut down" in out, f"missing shutdown message in output:\n{out}"
