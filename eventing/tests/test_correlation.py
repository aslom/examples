"""Word-pair correlation ID generator: regex + collision guard."""
from eventbridge.correlation import Minter, REGEX


def test_regex_matches_generated_ids():
    m = Minter(seed=42)
    for _ in range(200):
        assert REGEX.match(m.mint())


def test_ids_are_unique_within_a_minter():
    m = Minter(seed=42)
    ids = {m.mint() for _ in range(500)}
    assert len(ids) == 500


def test_remember_prevents_reissue():
    m = Minter(seed=1)
    fixed = "wake-otter-1234"
    m.remember(fixed)
    for _ in range(1000):
        assert m.mint() != fixed


def test_ids_lowercase_only():
    m = Minter(seed=7)
    for _ in range(50):
        v = m.mint()
        assert v == v.lower()
