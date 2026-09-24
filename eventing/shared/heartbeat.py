"""A liveness heartbeat file, for detecting a live process with a dead thread.

DESIGN_PHASE1.md §8.4. EventRunner's failure mode in Kubernetes is not a crash:
if the Kafka consumer thread raises, the **main thread keeps running**, so the
pod stays `Running`, reports healthy, consumes nothing, and the demo hangs
silently. A process-liveness check cannot see that, and EventRunner has no HTTP
server to probe.

So the consumer loop touches a file on every poll and the Deployment runs an
`exec` probe asserting the file's mtime is recent. A wedged thread stops touching
it, the probe fails, and the kubelet restarts the pod.

Deliberately mtime-based rather than content-based: `stat` is one syscall, works
from a shell probe, and survives a partially written file.
"""
from __future__ import annotations

import os
import pathlib
import time
from typing import Callable


class Heartbeat:
    """Touch-on-progress file with an age query.

    `clock` is injectable so staleness is testable without sleeping.
    """

    def __init__(self, path: str | pathlib.Path,
                 clock: Callable[[], float] = time.time) -> None:
        self.path = pathlib.Path(path)
        self._clock = clock
        self._last_touch: float | None = None

    def touch(self) -> None:
        """Record progress. Cheap enough to call on every poll iteration.

        Writes then explicitly sets mtime from `clock`, so an injected clock also
        drives the value the probe reads — otherwise a test could not distinguish
        "we touched it" from "the filesystem did".
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        now = self._clock()
        # Content is for humans reading `kubectl exec cat`; the probe uses mtime.
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(f"{now:.3f}\n")
        os.utime(tmp, (now, now))
        os.replace(tmp, self.path)     # atomic: a probe never sees a half file
        self._last_touch = now

    def mtime(self) -> float | None:
        try:
            return self.path.stat().st_mtime
        except FileNotFoundError:
            return None

    def age_s(self) -> float | None:
        """Seconds since the last touch, or None when the file is absent."""
        m = self.mtime()
        return None if m is None else max(0.0, self._clock() - m)

    def is_stale(self, max_age_s: float) -> bool:
        """True when the file is missing or older than max_age_s.

        Missing counts as stale on purpose: EventRunner touches the heartbeat
        once at startup before it ever tries to reach Kafka, so by the time the
        probe's initialDelaySeconds has elapsed an absent file means the process
        never got as far as its own main loop.
        """
        age = self.age_s()
        return age is None or age > max_age_s

    def describe(self, max_age_s: float) -> str:
        age = self.age_s()
        if age is None:
            return f"heartbeat missing at {self.path}"
        verdict = "STALE" if age > max_age_s else "ok"
        return f"heartbeat {verdict}: age={age:.1f}s max={max_age_s:.0f}s ({self.path})"
