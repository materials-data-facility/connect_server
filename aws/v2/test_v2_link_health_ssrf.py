"""The link-health prober must never be steered at non-public addresses."""
import socket

import httpx
import pytest

from v2 import link_health as lh

PUBLIC = [(2, 1, 6, "", ("93.184.216.34", 443))]


@pytest.mark.parametrize("url,reason", [
    ("http://example.org/x", "scheme-not-https"),
    ("https://127.0.0.1/x", "non-public-ip"),
    ("https://10.0.0.8/x", "non-public-ip"),
    ("https://169.254.169.254/latest/meta-data", "non-public-ip"),
    ("https://[::1]/x", "non-public-ip"),
    ("https://[::ffff:10.0.0.1]/x", "non-public-ip"),
    ("https://localhost/x", "blocked-hostname"),
    ("https://foo.internal/x", "blocked-hostname"),
    ("https://user:pw@example.org/x", "credentials-in-url"),
])
def test_admit_url_blocks_non_public_targets(url, reason):
    assert lh.admit_url(url) == reason


def test_admit_url_allows_public_https(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: PUBLIC)
    assert lh.admit_url("https://data.materialsdatafacility.org/mdf_open/x.csv") is None


def test_hostname_resolving_to_private_ip_is_blocked(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("10.1.2.3", 443))])
    assert lh.admit_url("https://evil.example.org/x") == "resolves-to-non-public-ip"


def test_redirect_to_private_address_is_blocked(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: PUBLIC)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "public.example.org":
            return httpx.Response(302, headers={"location": "https://169.254.169.254/latest/meta-data"})
        raise AssertionError("private hop must never be requested")

    client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    check = lh.probe_url("https://public.example.org/data.csv", client=client)
    assert check["state"] == lh.CHECK_UNVERIFIABLE
    assert check["error"].startswith("blocked: non-public-ip")


def test_public_redirect_is_followed_and_counts_as_ok(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *a, **k: PUBLIC)
    hops = []

    def handler(request: httpx.Request) -> httpx.Response:
        hops.append(str(request.url))
        if request.url.host == "zenodo.example.org":
            return httpx.Response(302, headers={"location": "https://s3.example.org/bucket/data.csv"})
        return httpx.Response(200)

    client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=False)
    check = lh.probe_url("https://zenodo.example.org/record/1/files/data.csv", client=client)
    assert check["ok"] is True and check["http_status"] == 200
    assert len(hops) == 2
