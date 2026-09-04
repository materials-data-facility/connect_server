"""Data availability / link health probing (extensions proposal P1).

``data_sources`` is a bare list of Globus/HTTPS URIs and nothing has ever
checked that they resolve: a migrated 2018 dataset pointing at a decommissioned
endpoint looks identical in search to one uploaded yesterday. This module
answers the one claim MDF must never get wrong — "this data is here" — for a
single published record, cheaply and without credentials.

Design constraints that shaped the code:

- **No auth, ever.** The probe runs from the async worker with no user token and
  no Globus consent, so it can only speak anonymous HTTPS. A ``globus://`` URI
  is rewritten to its public HTTPS form when (and only when) it lives on the
  NCSA MDF collection; anything else is reported ``unverifiable`` rather than
  guessed at.
- **A false "broken" is worse than silence.** Timeouts, DNS failures and TLS
  errors are transport noise, not evidence that data is gone, so they land in
  ``unverifiable``. Only an HTTP response that actually says "no" (4xx/5xx)
  marks a URL broken.
- **Bounded work.** At most ``MAX_URLS_PER_RECORD`` URLs per record and a
  ``DEFAULT_TIMEOUT_SECONDS`` ceiling per request, so a record with 400 data
  sources cannot eat the worker's budget.
- **HEAD first, ranged GET second.** Plenty of Globus HTTPS and S3-style hosts
  answer ``HEAD`` with 403/405 while serving ``GET`` fine, so a non-2xx HEAD is
  retried once as ``GET`` with ``Range: bytes=0-0`` before it counts against
  the dataset.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from v2.transfer import NCSA_MDF_COLLECTION_UUID

logger = logging.getLogger(__name__)

# Public anonymous HTTPS face of the NCSA MDF Globus collection.
MDF_HTTPS_BASE = "https://data.materialsdatafacility.org"

# Per-request ceiling. Ten seconds is generous for a HEAD and still lets five
# URLs finish inside the async worker's budget in the worst case.
DEFAULT_TIMEOUT_SECONDS = 10.0

# Hard cap on probes per record.
MAX_URLS_PER_RECORD = 5

# Aggregate statuses. Deliberately the only four values that ever reach a
# record, a card or the admin report.
STATUS_OK = "ok"
STATUS_DEGRADED = "degraded"
STATUS_BROKEN = "broken"
STATUS_UNVERIFIABLE = "unverifiable"

STATUSES = (STATUS_OK, STATUS_DEGRADED, STATUS_BROKEN, STATUS_UNVERIFIABLE)

# Per-check verdicts.
CHECK_OK = "ok"
CHECK_BROKEN = "broken"
CHECK_UNVERIFIABLE = "unverifiable"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# URI -> anonymously fetchable URL
# ---------------------------------------------------------------------------

def public_https_url(uri: str) -> Optional[str]:
    """The anonymous HTTPS URL for a data source, or None if there isn't one.

    ``https://`` URIs are already fetchable. ``globus://`` URIs are only
    fetchable without a token when they live on the NCSA MDF collection, which
    is fronted by ``data.materialsdatafacility.org``; a Globus URI on any other
    collection would need a consent we do not have, so it has no public form.
    """
    if not uri or not isinstance(uri, str):
        return None
    candidate = uri.strip()
    if not candidate:
        return None
    if candidate.startswith("https://"):
        return candidate
    if candidate.startswith("globus://"):
        rest = candidate[len("globus://"):]
        slash = rest.find("/")
        if slash <= 0:
            return None
        collection, path = rest[:slash], rest[slash:]
        if collection.lower() != NCSA_MDF_COLLECTION_UUID:
            return None
        return f"{MDF_HTTPS_BASE}/{path.lstrip('/')}"
    # http://, ftp://, bare paths, globus endpoint names: nothing we can assert.
    return None


def collect_candidates(record: Dict[str, Any]) -> List[Tuple[str, Optional[str]]]:
    """The (uri, probe_url) pairs to check for a record, capped and deduped.

    ``download_url`` goes first: it is what ``mdf clone`` and the portal's
    download button actually use, so it is the URL whose health users feel.
    ``data_sources`` follow in declared order. ``probe_url`` is None for
    sources with no anonymous HTTPS form (see ``public_https_url``).
    """
    from v2.metadata import parse_metadata

    meta = parse_metadata(record)

    ordered: List[str] = []
    if meta.download_url:
        ordered.append(meta.download_url)
    for src in meta.data_sources or []:
        ordered.append(src)

    candidates: List[Tuple[str, Optional[str]]] = []
    seen: set = set()
    for uri in ordered:
        if not uri or not isinstance(uri, str):
            continue
        key = uri.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        candidates.append((key, public_https_url(key)))
        if len(candidates) >= MAX_URLS_PER_RECORD:
            break
    return candidates


# ---------------------------------------------------------------------------
# Probing
# ---------------------------------------------------------------------------

def _verdict_for_status(http_status: int) -> str:
    return CHECK_OK if 200 <= http_status < 400 else CHECK_BROKEN


# --- outbound admission (SSRF guard) -------------------------------------------
# data_sources are publisher-controlled. The worker must never be steered at
# private, loopback, link-local or otherwise non-public addresses, on the first
# hop or via a redirect. https only; redirects are followed manually so every
# hop is re-admitted.
MAX_REDIRECT_HOPS = 3
_BLOCKED_HOSTNAMES = {"localhost", "metadata", "metadata.google.internal", "instance-data"}


def admit_url(url: str) -> Optional[str]:
    """Return None if ``url`` may be probed, else a short reason string."""
    import ipaddress
    import socket
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(url)
    except Exception:
        return "unparseable"
    if parts.scheme != "https":
        return "scheme-not-https"
    host = (parts.hostname or "").strip().lower().rstrip(".")
    if not host:
        return "no-host"
    if host in _BLOCKED_HOSTNAMES or host.endswith(".internal") or host.endswith(".localhost"):
        return "blocked-hostname"
    if parts.username or parts.password:
        return "credentials-in-url"

    def _non_public(ip) -> bool:
        mapped = getattr(ip, "ipv4_mapped", None)
        if mapped is not None:
            return _non_public(mapped)
        return (
            ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast
            or ip.is_reserved or ip.is_unspecified
        )

    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        return "non-public-ip" if _non_public(literal) else None
    try:
        infos = socket.getaddrinfo(host, parts.port or 443, proto=socket.IPPROTO_TCP)
    except Exception:
        return None  # DNS failure is reported by the probe itself as unverifiable
    for info in infos:
        try:
            if _non_public(ipaddress.ip_address(info[4][0])):
                return "resolves-to-non-public-ip"
        except ValueError:
            continue
    return None


def _request_following(client: Any, method: str, url: str, timeout: float, headers: Optional[Dict[str, str]] = None):
    """Issue ``method`` and follow up to MAX_REDIRECT_HOPS redirects, admitting every hop.

    Raises ``PermissionError(reason)`` when a hop is not admissible.
    """
    from urllib.parse import urljoin

    current = url
    resp = None
    for _ in range(MAX_REDIRECT_HOPS + 1):
        reason = admit_url(current)
        if reason:
            raise PermissionError(reason)
        resp = client.request(method, current, timeout=timeout, headers=headers, follow_redirects=False)
        if resp.status_code in (301, 302, 303, 307, 308) and resp.headers.get("location"):
            current = urljoin(current, resp.headers["location"])
            continue
        return resp
    return resp


def probe_url(
    url: str,
    client: Any = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """Probe one URL with HEAD, falling back to a ranged GET.

    Returns a check dict: ``{url, http_status, ok, ms, state}`` (plus ``error``
    and ``method`` when relevant). Never raises — a transport failure is a
    result (``unverifiable``), not an exception, because one dead host must not
    abort the rest of the record.
    """
    import httpx

    owns_client = client is None
    if owns_client:
        client = httpx.Client(timeout=timeout, follow_redirects=False)

    started = time.monotonic()
    check: Dict[str, Any] = {
        "url": url,
        "http_status": None,
        "ok": False,
        "ms": 0,
        "state": CHECK_UNVERIFIABLE,
    }
    try:
        status: Optional[int] = None
        method = "HEAD"
        error: Optional[str] = None
        try:
            resp = _request_following(client, "HEAD", url, timeout)
            status = int(resp.status_code)
        except PermissionError as exc:
            check["ms"] = int((time.monotonic() - started) * 1000)
            check["method"] = "HEAD"
            check["state"] = CHECK_UNVERIFIABLE
            check["error"] = f"blocked: {exc}"
            return check
        except Exception as exc:  # transport-level: DNS, TLS, timeout, reset
            error = f"{type(exc).__name__}: {exc}"

        # A non-2xx/3xx HEAD is not proof of absence — many hosts refuse HEAD
        # outright. Retry once as a one-byte GET before calling it broken.
        if status is None or not 200 <= status < 400:
            method = "GET"
            try:
                resp = _request_following(client, "GET", url, timeout, headers={"Range": "bytes=0-0"})
                status = int(resp.status_code)
                error = None
            except PermissionError as exc:
                check["ms"] = int((time.monotonic() - started) * 1000)
                check["method"] = "GET"
                check["state"] = CHECK_UNVERIFIABLE
                check["error"] = f"blocked: {exc}"
                return check
            except Exception as exc:
                # Keep the HEAD error if the GET failed the same way.
                error = error or f"{type(exc).__name__}: {exc}"
                status = status if status is not None else None

        check["ms"] = int((time.monotonic() - started) * 1000)
        check["method"] = method
        if status is None:
            # Never reached the server: transport noise, not a missing file.
            check["state"] = CHECK_UNVERIFIABLE
            check["ok"] = False
            if error:
                check["error"] = error[:300]
            return check

        check["http_status"] = status
        check["state"] = _verdict_for_status(status)
        check["ok"] = check["state"] == CHECK_OK
        if not check["ok"] and error:
            check["error"] = error[:300]
        return check
    finally:
        if owns_client:
            try:
                client.close()
            except Exception:
                logger.debug("Failed to close link-health http client", exc_info=True)


def aggregate_status(checks: List[Dict[str, Any]]) -> str:
    """Roll per-URL verdicts into one record-level status.

    - no checks at all, or nothing we could reach → ``unverifiable``
    - every reachable URL answered → ``ok`` (an ``unverifiable`` sibling does
      not downgrade a dataset whose data demonstrably resolves; a Globus
      collection needing consent is honest, not bad)
    - some answered, some refused → ``degraded``
    - nothing answered and at least one refused → ``broken``
    """
    if not checks:
        return STATUS_UNVERIFIABLE
    ok = sum(1 for c in checks if c.get("state") == CHECK_OK)
    broken = sum(1 for c in checks if c.get("state") == CHECK_BROKEN)
    if broken and ok:
        return STATUS_DEGRADED
    if broken:
        return STATUS_BROKEN
    if ok:
        return STATUS_OK
    return STATUS_UNVERIFIABLE


def check_record(
    record: Dict[str, Any],
    client: Any = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """Build the ``link_health`` payload for one record.

    Shape (stored TOP-LEVEL on the record, never inside ``dataset_mdata``)::

        {"status": "ok"|"degraded"|"broken"|"unverifiable",
         "checked_at": "2026-09-04T00:00:00Z",
         "checks": [{"url", "http_status", "ok", "ms", "state", ...}]}

    ``client`` is an optional httpx-compatible client, so the caller can share
    one connection pool across a record and tests can inject a transport.
    """
    candidates = collect_candidates(record)

    checks: List[Dict[str, Any]] = []
    owns_client = client is None and any(url for _, url in candidates)
    if owns_client:
        import httpx

        client = httpx.Client(timeout=timeout, follow_redirects=False)

    try:
        for uri, url in candidates:
            if url is None:
                checks.append({
                    "url": uri,
                    "http_status": None,
                    "ok": False,
                    "ms": 0,
                    "state": CHECK_UNVERIFIABLE,
                    "error": "no anonymous HTTPS form for this data source",
                })
                continue
            check = probe_url(url, client=client, timeout=timeout)
            if check["url"] != uri:
                # Report the source as declared as well as what we fetched, so
                # a curator can tell a rewritten globus:// URI from an https one.
                check["source_uri"] = uri
            checks.append(check)
    finally:
        if owns_client:
            try:
                client.close()
            except Exception:
                logger.debug("Failed to close link-health http client", exc_info=True)

    return {
        "status": aggregate_status(checks),
        "checked_at": _utc_now(),
        "checks": checks,
    }


# ---------------------------------------------------------------------------
# Reading it back
# ---------------------------------------------------------------------------

def parse_link_health(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The full ``link_health`` block off a record, or None if absent/invalid.

    Both store backends hand back a dict, but a raw DynamoDB export or a
    hand-written fixture may still carry the JSON string, so this tolerates
    both and rejects anything without a recognized status.
    """
    raw = (record or {}).get("link_health")
    if isinstance(raw, str):
        import json

        try:
            raw = json.loads(raw)
        except Exception:
            return None
    if not isinstance(raw, dict):
        return None
    if raw.get("status") not in STATUSES:
        return None
    return raw


def public_link_health(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The status + checked_at of a record's link health, or None.

    Cards expose only these two fields: the per-URL ``checks`` list carries
    internal paths and upstream error strings that belong in the admin report,
    not on a public dataset card.
    """
    block = parse_link_health(record)
    if block is None:
        return None
    return {"status": block["status"], "checked_at": block.get("checked_at")}
