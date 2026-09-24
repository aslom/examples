"""EVENT_BRIDGE_PUBLIC_BASE_URL self-test — verdict logic + report format.

The real self-test hits sockets; here we exercise the decision helper on
synthetic inputs and let the caller stub the TCP probe."""
from unittest.mock import patch

from eventbridge.selftest import (
    ProbeResult, _decide_verdict, _is_public_tunnel_host,
    format_report, probe_candidates,
)


def test_public_tunnel_hostname_recognition():
    assert _is_public_tunnel_host("abcd.ngrok-free.app")
    assert _is_public_tunnel_host("my-demo.ngrok.app")
    assert _is_public_tunnel_host("random.trycloudflare.com")
    assert _is_public_tunnel_host("laptop.tail1234.ts.net")
    assert _is_public_tunnel_host("Foo.LHR.rocks")     # case-insensitive
    assert not _is_public_tunnel_host("example.com")
    assert not _is_public_tunnel_host("192.168.68.9")


def test_verdict_public_tunnel_listening_is_recommended():
    v, notes = _decide_verdict("public-tunnel",
                                "https://abcd.ngrok-free.app", listening=True,
                                default_route_iface="en0")
    assert v == "recommended"
    assert any("public tunnel" in n for n in notes)


def test_verdict_public_tunnel_not_listening_says_start_the_tunnel():
    v, notes = _decide_verdict("public-tunnel",
                                "https://abcd.ngrok-free.app", listening=False,
                                default_route_iface="en0")
    assert v == "not-listening"
    assert any("start the tunnel" in n for n in notes)


def test_verdict_loopback_flagged_same_host_only():
    v, _ = _decide_verdict("loopback",
                            "http://127.0.0.1:8080", listening=True,
                            default_route_iface="en0")
    assert v == "reachable-same-host-only"


def test_verdict_lan_via_normal_gateway_is_recommended():
    """No VPN hijack — a phone on the same wifi can reach LAN IP."""
    v, notes = _decide_verdict("likely-lan",
                                "http://192.168.68.9:8080", listening=True,
                                default_route_iface="en0")
    assert v == "recommended"
    assert any("phones on the same wifi/LAN" in n for n in notes)


def test_verdict_lan_under_tunnelall_gets_flagged():
    """This is the whole point — LAN listening + default route via utun*
    means the phone will time out. The report has to warn the operator."""
    v, notes = _decide_verdict("likely-lan",
                                "http://192.168.68.9:8080", listening=True,
                                default_route_iface="utun4")
    assert v == "reachable-lan-only"
    assert any("TUNNELALL" in n for n in notes)
    assert any("ngrok" in n or "cloudflared" in n or "Tailscale" in n for n in notes)


def test_verdict_vpn_address_says_inside_vpn_only():
    v, notes = _decide_verdict("likely-vpn",
                                "http://10.132.4.187:8080", listening=True,
                                default_route_iface="utun4")
    assert v == "reachable-vpn-only"


def test_verdict_not_listening_dominates_label():
    v, notes = _decide_verdict("likely-lan",
                                "http://192.168.68.9:8080", listening=False,
                                default_route_iface="en0")
    assert v == "not-listening"
    assert any("EB isn't listening" in n for n in notes)
    assert any("EB_HTTP_ADDR" in n for n in notes)


def test_probe_candidates_ranks_recommended_first():
    """Feed a fake enumerate + probe pair; verify sort order."""
    from eventbridge.netcands import Candidate
    fake_cands = [
        Candidate("127.0.0.1",   "lo0",   "loopback"),
        Candidate("192.168.68.9","en0",   "likely-lan"),
        Candidate("10.132.4.187","utun4", "likely-vpn"),
    ]
    with patch("eventbridge.selftest.enumerate_candidates", return_value=fake_cands), \
         patch("eventbridge.selftest._default_route_source_ipv4", return_value="192.168.68.9"), \
         patch("eventbridge.selftest._probe_tcp", return_value=True):
        results = probe_candidates(port=8080, current=None)
    verdicts = [r.verdict for r in results]
    assert verdicts[0] == "recommended", f"top-ranked verdict must be recommended: {verdicts}"
    # The 127.0.0.1 comes AFTER a routable LAN option
    lo_pos = next(i for i, r in enumerate(results) if r.url.startswith("http://127.0.0.1"))
    lan_pos = next(i for i, r in enumerate(results) if r.url.startswith("http://192.168.68.9"))
    assert lan_pos < lo_pos


def test_probe_candidates_extra_url_gets_probed_too():
    from eventbridge.netcands import Candidate
    fake_cands = [Candidate("127.0.0.1", "lo0", "loopback")]
    with patch("eventbridge.selftest.enumerate_candidates", return_value=fake_cands), \
         patch("eventbridge.selftest._default_route_source_ipv4", return_value=None), \
         patch("eventbridge.selftest._probe_tcp", return_value=True):
        results = probe_candidates(port=8080, current=None,
                                    extra_urls=["https://my-agent.ngrok-free.app"])
    urls = [r.url for r in results]
    assert "https://my-agent.ngrok-free.app" in urls
    ngrok = next(r for r in results if r.url.endswith(".ngrok-free.app"))
    assert ngrok.verdict == "recommended"


def test_format_report_contains_key_lines():
    results = [
        ProbeResult("http://192.168.68.9:8080", "likely-lan", "en0",
                    listening=True, verdict="reachable-lan-only",
                    notes=("⚠ default route is via utun4 (VPN) — TUNNELALL",
                           "workaround: ngrok / cloudflared / Tailscale")),
        ProbeResult("http://127.0.0.1:8080", "loopback", "lo0",
                    listening=True, verdict="reachable-same-host-only",
                    notes=()),
    ]
    text = format_report(results)
    assert "candidates" in text.lower()
    assert "http://192.168.68.9:8080" in text
    assert "reachable-lan-only" in text
    assert "TUNNELALL" in text or "utun4" in text
