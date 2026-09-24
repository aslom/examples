"""Interface enumeration + labeling — helps the operator pick the right
address for EVENT_BRIDGE_PUBLIC_BASE_URL under Cisco TUNNELALL / dev laptops
where routing intuition is easily wrong."""
from eventbridge.netcands import (
    Candidate, _classify, _parse_ifconfig, _parse_ip_addr,
    format_startup_hint, enumerate_candidates,
)


def test_classify_by_iface_hint():
    assert _classify("192.168.1.15", "en0")       == "likely-lan"
    assert _classify("10.42.7.3",    "utun0")     == "likely-vpn"
    assert _classify("10.42.7.3",    "utun3")     == "likely-vpn"
    assert _classify("172.17.0.1",   "docker0")   == "likely-docker"
    assert _classify("172.17.0.1",   "br-abc")    == "likely-docker"
    assert _classify("127.0.0.1",    "lo0")       == "loopback"


def test_classify_10dot_without_iface_hint_is_ambiguous():
    # 10.x on a plain "en0" could be corp DHCP or a router LAN. Label it
    # so the operator sees the ambiguity rather than mislabeled as LAN.
    assert _classify("10.0.0.5", "en0") == "likely-lan-or-vpn"


def test_classify_public_ip():
    assert _classify("8.8.8.8", "en0") == "public"


def test_parse_ifconfig_macos_output():
    sample = """
lo0: flags=8049<UP,LOOPBACK,RUNNING,MULTICAST> mtu 16384
\tinet 127.0.0.1 netmask 0xff000000
en0: flags=8863<UP,BROADCAST,SMART,RUNNING> mtu 1500
\tinet 192.168.68.9 netmask 0xffffff00 broadcast 192.168.68.255
utun3: flags=8051<UP,POINTOPOINT,RUNNING,MULTICAST> mtu 1420
\tinet 10.132.4.187 --> 10.132.4.187 netmask 0xffffffff
""".strip()
    cs = _parse_ifconfig(sample)
    idx = {(c.interface, c.address): c for c in cs}
    assert ("lo0",  "127.0.0.1")    in idx
    assert ("en0",  "192.168.68.9") in idx
    assert ("utun3","10.132.4.187") in idx
    assert idx[("en0", "192.168.68.9")].label == "likely-lan"
    assert idx[("utun3","10.132.4.187")].label == "likely-vpn"


def test_parse_ip_addr_linux_output():
    sample = ("1: lo    inet 127.0.0.1/8 scope host lo\n"
              "2: eth0  inet 192.168.1.42/24 brd 192.168.1.255 scope global eth0\n"
              "3: docker0 inet 172.17.0.1/16 brd 172.17.255.255 scope global docker0\n")
    cs = _parse_ip_addr(sample)
    labels = {c.interface: c.label for c in cs}
    assert labels["lo"] == "loopback"
    assert labels["eth0"] == "likely-lan"
    assert labels["docker0"] == "likely-docker"


def test_format_startup_hint_contains_the_key_shapes():
    cands = [
        Candidate("192.168.68.9", "en0",   "likely-lan"),
        Candidate("10.132.4.187", "utun3", "likely-vpn"),
        Candidate("127.0.0.1",    "lo0",   "loopback"),
    ]
    out = format_startup_hint(cands, port=8080,
                              current="http://127.0.0.1:8080")
    # Points at the env var by its new name
    assert "EVENT_BRIDGE_PUBLIC_BASE_URL" in out
    # Every candidate URL is shown with its label
    assert "http://192.168.68.9:8080" in out
    assert "http://10.132.4.187:8080" in out
    assert "http://127.0.0.1:8080" in out
    assert "likely-lan" in out and "likely-vpn" in out and "loopback" in out
    # TUNNELALL note is present
    assert "TUNNELALL" in out
    assert "Tailscale" in out or "tailscale" in out
    assert "ngrok" in out.lower() or "cloudflared" in out.lower()


def test_enumerate_candidates_never_empty():
    """Even under a stripped-down environment we always at least return
    loopback so the operator has one usable option."""
    cs = enumerate_candidates()
    assert any(c.label == "loopback" for c in cs)
    # No obvious dupes: same (addr, iface) collapsed
    keys = [(c.address, c.interface) for c in cs]
    assert len(keys) == len(set(keys))
