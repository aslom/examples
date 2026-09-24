"""Human-friendly correlation ID generator: <adj>-<animal>-<4 digits>."""
from __future__ import annotations

import pathlib
import random
import re
import threading

REGEX = re.compile(r"^[a-z]{3,10}-[a-z]{3,12}-\d{4}$")


def _load(name: str) -> list[str]:
    p = pathlib.Path(__file__).resolve().parents[1] / "shared" / "words" / f"{name}.txt"
    return [w.strip() for w in p.read_text().splitlines() if w.strip() and w.strip().isalpha()]


_ADJ = _load("adjectives")
_ANI = _load("animals")


class Minter:
    def __init__(self, seed: int | None = None) -> None:
        self._rng = random.Random(seed)
        self._seen: set[str] = set()
        self._lock = threading.Lock()

    def mint(self) -> str:
        for _ in range(2000):
            candidate = f"{self._rng.choice(_ADJ)}-{self._rng.choice(_ANI)}-{self._rng.randint(0, 9999):04d}"
            if not REGEX.match(candidate):
                continue
            with self._lock:
                if candidate not in self._seen:
                    self._seen.add(candidate)
                    return candidate
        raise RuntimeError("correlation ID space exhausted")

    def remember(self, corr: str) -> None:
        with self._lock:
            self._seen.add(corr)
