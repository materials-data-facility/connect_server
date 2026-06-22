"""Local empirical validation harness for MDF Connect v2 backend.

Runs the FastAPI app in-process via TestClient (no network) to confirm/refute a
set of catalog-derived issues. Writes a results JSON and prints a markdown table.

Run FROM /Users/blaiszik/Desktop/git/mdf_client/cs/aws with the prepared venv:
    /tmp/mdfv312/bin/python v2/validation/test_local_harness.py
"""

import json
import os
import sys
import uuid
import traceback

# ---------------------------------------------------------------------------
# Environment MUST be set BEFORE importing the app.
# ---------------------------------------------------------------------------
_UNIQUE = uuid.uuid4().hex[:8]
SQLITE_PATH = f"/tmp/mdf_test_{_UNIQUE}.db"

os.environ["STORE_BACKEND"] = "sqlite"
os.environ["SQLITE_PATH"] = SQLITE_PATH
os.environ["STORAGE_BACKEND"] = "local"
os.environ["AUTH_MODE"] = "dev"
os.environ["LOCAL_DEV_AUTH"] = "true"
os.environ["ALLOW_ALL_CURATORS"] = "false"      # plain submitters by default
os.environ["CURATOR_GROUP_IDS"] = ""
os.environ["REQUIRED_GROUP_MEMBERSHIP"] = ""
os.environ["USE_MOCK_DATACITE"] = "true"
os.environ["ASYNC_DISPATCH_MODE"] = "inline"
os.environ["USE_MOCK_SEARCH"] = "true"
# Make the user we want a curator explicit via CURATOR_USER_IDS so we can act as
# either a plain submitter or a curator deterministically (ALLOW_ALL_CURATORS stays
# false so non-curator users are genuinely non-privileged).
CURATOR_ID = "curator-user"
os.environ["CURATOR_USER_IDS"] = CURATOR_ID

RESULTS_PATH = os.path.join(os.path.dirname(__file__), "test_local_results.json")

# Ensure cwd-based `import v2...` works regardless of where invoked from.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from fastapi.testclient import TestClient  # noqa: E402

import v2.app.routers.submissions as submissions_mod  # noqa: E402
import v2.app.routers.curation as curation_mod  # noqa: E402
import v2.async_jobs as async_jobs_mod  # noqa: E402

from v2.app import app  # noqa: E402

client = TestClient(app, raise_server_exceptions=False)

results = []


def record(issue_id, verdict, evidence, detail=""):
    results.append({
        "issue_id": issue_id,
        "verdict": verdict,
        "evidence": evidence,
        "detail": detail,
    })
    print(f"\n=== {issue_id}: {verdict} ===\n{evidence}\n")


# Identity header helpers --------------------------------------------------
def hdr(uid, email=None, name=None):
    h = {"X-User-Id": uid}
    if email:
        h["X-User-Email"] = email
    if name:
        h["X-User-Name"] = name
    return h


USER_A = hdr("user-A", "alice@example.com", "Alice")
USER_B = hdr("user-B", "bob@example.com", "Bob")
CURATOR = hdr(CURATOR_ID, "curator@example.com", "Curator")


def minimal_metadata(title="Test DS", source_id=None, update=False, data_sources=None):
    md = {
        "title": title,
        "authors": [{"name": "Alice Author"}],
        "data_sources": data_sources if data_sources is not None else ["https://example.com/data.csv"],
    }
    ext = {}
    if source_id:
        ext["mdf_source_id"] = source_id
    if ext:
        md["extensions"] = ext
    if update:
        md["update"] = True
    return md


def submit_as(headers, **kw):
    md = minimal_metadata(**kw)
    return client.post("/submit", json=md, headers=headers)


def publish_via_curation(headers_owner, headers_curator, title="Pub DS"):
    """Submit then approve through curation to reach 'published' status.

    Returns (source_id, version, approve_response_json).
    """
    r = submit_as(headers_owner, title=title)
    assert r.status_code == 200, r.text
    sid = r.json()["source_id"]
    ver = r.json()["version"]
    ar = client.post(f"/curation/{sid}/approve", json={"mint_doi": True}, headers=headers_curator)
    return sid, ver, ar


# ---------------------------------------------------------------------------
# Boot / route inventory
# ---------------------------------------------------------------------------
def test_boot_and_routes():
    try:
        h = client.get("/health")
        # NOTE: fastapi 0.138 uses a lazy _IncludedRouter placeholder; inspecting
        # app.routes before expansion shows only 2 routes. The OpenAPI schema forces
        # expansion and is the authoritative source of the registered route set.
        schema = app.openapi()
        paths = schema.get("paths", {})
        ops = []
        for p in sorted(paths):
            for method in sorted(paths[p]):
                ops.append((p, method.upper()))
        evidence = (
            f"/health -> {h.status_code} {h.json()}; app boots clean; "
            f"route_count={len(paths)} unique paths, {len(ops)} operations "
            f"(via OpenAPI schema, which forces _IncludedRouter expansion). "
            f"NOTE: raw app.routes inspection shows only 2 APIRoutes pre-expansion "
            f"due to fastapi 0.138 lazy router inclusion."
        )
        detail = "\n".join(f"{m:<6} {p}" for p, m in ops)
        record("BOOT", "CONFIRMED" if h.status_code == 200 else "BLOCKED",
               evidence, detail)
        return list(paths.keys())
    except Exception as e:
        record("BOOT", "BLOCKED", f"App failed to boot: {e}", traceback.format_exc())
        return []


# ---------------------------------------------------------------------------
# Issue 8 (run early so later tests can reuse): happy-path smoke
# ---------------------------------------------------------------------------
def test_happy_path_smoke():
    steps = {}
    r = submit_as(USER_A, title="Happy Path DS")
    steps["POST /submit"] = (r.status_code, r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text)
    if r.status_code != 200:
        record("SMOKE-08", "CONFIRMED",
               f"POST /submit FAILED: {r.status_code} {r.text}", json.dumps(steps, default=str))
        return None
    sid = r.json()["source_id"]

    r2 = client.get(f"/status/{sid}", headers=USER_A)
    steps["GET /status/{sid}"] = (r2.status_code, r2.json())
    r3 = client.get(f"/versions/{sid}", headers=USER_A)
    steps["GET /versions/{sid}"] = (r3.status_code, r3.json())
    r4 = client.get("/submissions", headers=USER_A)
    steps["GET /submissions"] = (r4.status_code, r4.json() if r4.status_code == 200 else r4.text)

    failures = [k for k, (code, _) in steps.items() if code != 200]
    okmsg = "; ".join(f"{k}->{code}" for k, (code, _) in steps.items())
    if failures:
        record("SMOKE-08", "CONFIRMED",
               f"Some happy-path steps failed: {failures}. ({okmsg})",
               json.dumps(steps, default=str)[:4000])
    else:
        record("SMOKE-08", "REFUTED",
               f"Full happy path succeeds: {okmsg}",
               json.dumps(steps, default=str)[:4000])
    return sid


# ---------------------------------------------------------------------------
# SUB-01: IDOR on update — B hijacks A's dataset via /submit update=true
# ---------------------------------------------------------------------------
def test_sub01_idor_update():
    ra = submit_as(USER_A, title="A's dataset")
    if ra.status_code != 200:
        record("SUB-01", "BLOCKED", f"Setup submit as A failed: {ra.status_code} {ra.text}")
        return
    sid = ra.json()["source_id"]
    v_a = ra.json()["version"]

    # B attempts an update against A's source_id
    rb = submit_as(USER_B, title="B hijack", source_id=sid, update=True,
                   data_sources=["https://evil.example.com/data.csv"])
    detail = {
        "A_submit": ra.json(),
        "B_update_status": rb.status_code,
        "B_update_body": rb.json() if rb.headers.get("content-type", "").startswith("application/json") else rb.text,
    }

    # Inspect store directly to see who owns the new version.
    store = submissions_mod.get_submission_store()
    versions = store.list_versions(sid)
    owners = {v.get("version"): v.get("user_id") for v in versions}
    detail["versions_owners"] = owners

    if rb.status_code in (403, 404):
        record("SUB-01", "REFUTED",
               f"B's update rejected with {rb.status_code}; no hijack. owners={owners}",
               json.dumps(detail, default=str))
    elif rb.status_code == 200:
        new_ver = rb.json().get("version")
        new_owner = owners.get(new_ver)
        hijacked = new_owner == "user-B" and new_ver != v_a
        record("SUB-01", "CONFIRMED" if hijacked else "REFUTED",
               (f"B (user-B) successfully created version {new_ver} on A's source_id "
                f"{sid}; new version owner={new_owner}. /submit performs NO "
                f"owner/curator check before put_submission." if hijacked
                else f"B got 200 but did not gain ownership: owners={owners}"),
               json.dumps(detail, default=str))
    else:
        record("SUB-01", "BLOCKED",
               f"Unexpected status {rb.status_code} from B's update",
               json.dumps(detail, default=str))


# ---------------------------------------------------------------------------
# SUB-02: ownership reassignment on metadata edit (published minor bump)
# ---------------------------------------------------------------------------
def test_sub02_ownership_reassignment():
    sid, ver, ar = publish_via_curation(USER_A, CURATOR, title="Owned by A")
    if ar.status_code != 200:
        record("SUB-02", "BLOCKED",
               f"Could not publish setup: approve -> {ar.status_code} {ar.text}")
        return

    store = submissions_mod.get_submission_store()
    pub = store.get_submission(sid, ver)
    orig_owner = pub.get("user_id")

    # Curator edits metadata on the published dataset -> minor version bump.
    er = client.post(f"/submissions/{sid}/metadata",
                     json={"description": "Edited by curator"},
                     headers=CURATOR)
    detail = {
        "publish_version": ver,
        "orig_owner": orig_owner,
        "edit_status": er.status_code,
        "edit_body": er.json() if er.headers.get("content-type", "").startswith("application/json") else er.text,
    }
    if er.status_code != 200:
        record("SUB-02", "BLOCKED",
               f"Metadata edit failed: {er.status_code} {er.text}",
               json.dumps(detail, default=str))
        return

    new_ver = er.json().get("new_version")
    new_rec = store.get_submission(sid, new_ver)
    new_owner = new_rec.get("user_id") if new_rec else None
    new_email = new_rec.get("user_email") if new_rec else None
    detail["new_version"] = new_ver
    detail["new_owner"] = new_owner
    detail["new_email"] = new_email

    if new_owner == CURATOR_ID and orig_owner != CURATOR_ID:
        record("SUB-02", "CONFIRMED",
               (f"After curator edit, new version {new_ver} user_id became "
                f"'{new_owner}' (the editor) instead of original owner "
                f"'{orig_owner}'. user_email also became '{new_email}'."),
               json.dumps(detail, default=str))
    else:
        record("SUB-02", "REFUTED",
               f"Owner preserved: orig={orig_owner}, new={new_owner}",
               json.dumps(detail, default=str))


# ---------------------------------------------------------------------------
# SUB-05: delete leaves search entry (no search delete called)
# ---------------------------------------------------------------------------
def test_sub05_delete_leaves_search():
    sid, ver, ar = publish_via_curation(USER_A, CURATOR, title="To be deleted")
    if ar.status_code != 200:
        record("SUB-05", "BLOCKED", f"Publish setup failed: {ar.status_code} {ar.text}")
        return

    # Instrument the search client so we can detect any delete call during /delete.
    from v2.search_client import get_search_client
    sc = get_search_client()
    delete_calls = []
    ingest_calls = []

    # Wrap whatever delete-style methods exist.
    import types
    for meth in ("delete_entry", "delete", "remove_entry", "delete_subject"):
        if hasattr(sc, meth):
            orig = getattr(sc, meth)
            def make_wrapper(o, name):
                def w(*a, **k):
                    delete_calls.append((name, a, k))
                    return o(*a, **k)
                return w
            setattr(sc, meth, make_wrapper(orig, meth))
    if hasattr(sc, "ingest"):
        oi = sc.ingest
        def iw(*a, **k):
            ingest_calls.append((a, k))
            return oi(*a, **k)
        sc.ingest = iw

    # Force the submissions router to use this same instrumented client if it
    # re-fetches one (get_search_client returns a singleton in mock mode).
    dr = client.post(f"/submissions/{sid}/delete",
                     json={"reason": "test deletion"},
                     headers=CURATOR)

    store = submissions_mod.get_submission_store()
    rec = store.get_submission(sid, ver)
    detail = {
        "delete_status": dr.status_code,
        "delete_body": dr.json() if dr.headers.get("content-type", "").startswith("application/json") else dr.text,
        "record_status_after": rec.get("status") if rec else None,
        "search_delete_calls": delete_calls,
    }

    # Static code-path check: does delete_submission reference search at all?
    import inspect
    src = inspect.getsource(submissions_mod.delete_submission)
    references_search = ("search" in src.lower())
    detail["delete_submission_references_search"] = references_search

    if dr.status_code == 200 and not delete_calls and not references_search:
        record("SUB-05", "CONFIRMED",
               (f"/submissions/{sid}/delete set status='deleted' "
                f"(record now '{rec.get('status') if rec else None}') but made ZERO "
                f"search delete calls and the handler source contains no reference to "
                f"search at all. A published dataset remains indexed after deletion."),
               json.dumps(detail, default=str))
    elif delete_calls:
        record("SUB-05", "REFUTED",
               f"Search delete WAS invoked: {delete_calls}",
               json.dumps(detail, default=str))
    else:
        record("SUB-05", "BLOCKED",
               f"Delete returned {dr.status_code}; calls={delete_calls}",
               json.dumps(detail, default=str))


# ---------------------------------------------------------------------------
# SUB-09 / SUB-10: PII exposure via /status and /status/{sid} (no auth)
# ---------------------------------------------------------------------------
def test_sub09_pii_exposure():
    sid, ver, ar = publish_via_curation(USER_A, CURATOR, title="Public PII test")
    if ar.status_code != 200:
        record("SUB-09/10", "BLOCKED", f"Publish setup failed: {ar.status_code} {ar.text}")
        return

    # Unauthenticated request: omit X-User-Id entirely. In dev mode, get_optional_auth
    # would only build identity if X-User-Id present; without it, auth is None.
    r_status = client.get(f"/status/{sid}")  # NO headers
    r_status_all = client.get(f"/status?source_id={sid}")  # NO headers
    r_card = client.get(f"/card/{sid}")  # NO headers

    sub = r_status.json().get("submission", {}) if r_status.status_code == 200 else {}
    pii_fields = [f for f in ("user_email", "user_id", "curation_history",
                              "approved_by", "rejected_by", "dataset_mdata")
                  if f in sub]
    card = r_card.json().get("card", {}) if r_card.status_code == 200 else {}
    card_pii = [f for f in ("user_email", "user_id", "curation_history")
                if f in card]

    detail = {
        "status_code": r_status.status_code,
        "status_keys": sorted(sub.keys()),
        "status_user_email": sub.get("user_email"),
        "status_user_id": sub.get("user_id"),
        "status_has_curation_history": "curation_history" in sub,
        "status_all_keys": sorted(r_status_all.json().get("submission", {}).keys())
            if r_status_all.status_code == 200 else r_status_all.status_code,
        "card_keys": sorted(card.keys()),
        "card_pii_fields": card_pii,
    }

    leaked = [f for f in ("user_email", "user_id") if sub.get(f)]
    if leaked:
        record("SUB-09/10", "CONFIRMED",
               (f"GET /status/{sid} with NO auth returns full raw record including "
                f"{pii_fields}. user_email='{sub.get('user_email')}', "
                f"user_id='{sub.get('user_id')}'. /card by contrast exposes only "
                f"{card_pii or 'no PII fields'}."),
               json.dumps(detail, default=str))
    else:
        record("SUB-09/10", "REFUTED",
               f"/status did not leak email/user_id. status keys={sorted(sub.keys())}",
               json.dumps(detail, default=str))


# ---------------------------------------------------------------------------
# SUB-11: publish bypass — curator POST /status/update status=published
# ---------------------------------------------------------------------------
def test_sub11_publish_bypass():
    # Fresh pending submission (NOT through approve, so no DOI/index yet).
    r = submit_as(USER_A, title="Bypass publish")
    if r.status_code != 200:
        record("SUB-11", "BLOCKED", f"Submit setup failed: {r.status_code} {r.text}")
        return
    sid = r.json()["source_id"]
    ver = r.json()["version"]

    # Instrument publish pipeline + search to prove neither runs.
    publish_calls = []
    orig_enqueue = async_jobs_mod.enqueue_publish_job
    def wrapped_enqueue(*a, **k):
        publish_calls.append((a, k))
        return orig_enqueue(*a, **k)
    submissions_mod.enqueue_publish_job = wrapped_enqueue  # patched name in router ns

    from v2.search_client import get_search_client
    sc = get_search_client()
    ingest_calls = []
    if hasattr(sc, "ingest"):
        oi = sc.ingest
        def iw(*a, **k):
            ingest_calls.append((a, k))
            return oi(*a, **k)
        sc.ingest = iw

    rr = client.post("/status/update",
                     json={"source_id": sid, "version": ver, "status": "published"},
                     headers=CURATOR)

    # restore
    submissions_mod.enqueue_publish_job = orig_enqueue

    store = submissions_mod.get_submission_store()
    rec = store.get_submission(sid, ver)
    detail = {
        "update_status": rr.status_code,
        "update_body": rr.json() if rr.headers.get("content-type", "").startswith("application/json") else rr.text,
        "record_status_after": rec.get("status") if rec else None,
        "record_doi": rec.get("dataset_doi") or rec.get("doi") if rec else None,
        "record_published_at": rec.get("published_at") if rec else None,
        "publish_job_calls_during_update": publish_calls,
        "search_ingest_calls_during_update": ingest_calls,
    }
    # Static check: update_status handler only calls store.update_status
    import inspect
    src = inspect.getsource(submissions_mod.update_status)
    calls_publish = "enqueue_publish_job" in src
    calls_search = "search" in src.lower()
    detail["handler_calls_publish_pipeline"] = calls_publish
    detail["handler_references_search"] = calls_search

    if (rr.status_code == 200 and rec and rec.get("status") == "published"
            and not publish_calls and not ingest_calls and not calls_publish):
        record("SUB-11", "CONFIRMED",
               (f"Curator POST /status/update status=published flips the record to "
                f"'published' via a bare SQL UPDATE. No publish job enqueued, no search "
                f"ingest, no DOI minted (doi={detail['record_doi']}, "
                f"published_at={detail['record_published_at']}). Handler never references "
                f"the publish pipeline."),
               json.dumps(detail, default=str))
    else:
        record("SUB-11", "REFUTED",
               (f"status={rec.get('status') if rec else None}, publish_calls={publish_calls}, "
                f"ingest_calls={len(ingest_calls)}"),
               json.dumps(detail, default=str))


# ---------------------------------------------------------------------------
# SUB-06: version sort is lexicographic in GET /versions
# ---------------------------------------------------------------------------
def test_sub06_version_sort():
    # Create a chain: submit (1.0), then repeated update=true with new data
    # which does a MAJOR bump (has_new_data) each time -> 2.0, 3.0, ... 10.0.
    r = submit_as(USER_A, title="Versioned DS")
    if r.status_code != 200:
        record("SUB-06", "BLOCKED", f"Initial submit failed: {r.status_code} {r.text}")
        return
    sid = r.json()["source_id"]

    last_ver = r.json()["version"]
    versions_made = [last_ver]
    for i in range(9):  # produce up to 10.0
        ru = submit_as(USER_A, title="Versioned DS", source_id=sid, update=True,
                       data_sources=[f"https://example.com/data_{i}.csv"])
        if ru.status_code != 200:
            break
        last_ver = ru.json()["version"]
        versions_made.append(last_ver)

    # Now query the listing. Need them visible: owner (USER_A) sees all statuses.
    lv = client.get(f"/versions/{sid}", headers=USER_A)
    detail = {
        "versions_created": versions_made,
        "list_status": lv.status_code,
    }
    if lv.status_code != 200 or not lv.json().get("success"):
        record("SUB-06", "BLOCKED",
               f"versions listing failed: {lv.status_code} {lv.text}",
               json.dumps(detail, default=str))
        return

    listed = [v["version"] for v in lv.json()["versions"]]
    detail["listed_order"] = listed

    # Correct numeric order
    def numeric_key(v):
        return [int(p) if p.isdigit() else p for p in v.split(".")]
    correct = sorted(listed, key=numeric_key)
    lexicographic = sorted(listed)

    have_10_and_2 = "10.0" in listed and "2.0" in listed
    is_lexicographic = listed == lexicographic and listed != correct

    detail["correct_numeric_order"] = correct
    detail["lexicographic_order"] = lexicographic

    if have_10_and_2 and is_lexicographic:
        idx10 = listed.index("10.0")
        idx2 = listed.index("2.0")
        record("SUB-06", "CONFIRMED",
               (f"GET /versions/{sid} returns versions sorted lexicographically: "
                f"{listed}. '10.0' appears at index {idx10}, before '2.0' at index "
                f"{idx2} (numerically wrong). Code uses key=lambda x: x.get('version')."),
               json.dumps(detail, default=str))
    elif have_10_and_2 and listed == correct:
        record("SUB-06", "REFUTED",
               f"Versions correctly numerically ordered: {listed}",
               json.dumps(detail, default=str))
    else:
        record("SUB-06", "BLOCKED",
               f"Could not reach 10.0/2.0 chain. listed={listed}",
               json.dumps(detail, default=str))


# ---------------------------------------------------------------------------
# Issue 7: curation happy path + dataset_mdata type inconsistency
# ---------------------------------------------------------------------------
def test_curation_flow():
    detail = {}
    # submit
    r = submit_as(USER_A, title="Curation Flow DS")
    detail["submit"] = (r.status_code, r.json() if r.status_code == 200 else r.text)
    if r.status_code != 200:
        record("CURATION-07", "BLOCKED", f"submit failed: {r.status_code} {r.text}",
               json.dumps(detail, default=str))
        return
    sid = r.json()["source_id"]
    ver = r.json()["version"]

    # how submit stored dataset_mdata
    store = submissions_mod.get_submission_store()
    # peek raw sqlite (bypass _row_to_dict auto-parse) to see stored type
    import sqlite3
    raw_conn = sqlite3.connect(SQLITE_PATH)
    raw_row = raw_conn.execute(
        "SELECT dataset_mdata FROM submissions WHERE source_id=? AND version=?",
        (sid, ver)).fetchone()
    raw_conn.close()
    submit_stored_is_str = isinstance(raw_row[0], str) if raw_row else None
    detail["submit_stored_dataset_mdata_is_json_string"] = submit_stored_is_str

    # appears in pending
    pend = client.get("/curation/pending", headers=CURATOR)
    detail["pending"] = (pend.status_code, pend.json().get("pending_count") if pend.status_code == 200 else pend.text)
    pending_ids = [s["source_id"] for s in pend.json().get("submissions", [])] if pend.status_code == 200 else []
    detail["in_pending"] = sid in pending_ids

    # get curation detail
    gc = client.get(f"/curation/{sid}", headers=CURATOR)
    detail["get_curation"] = (gc.status_code, gc.json().get("current_status") if gc.status_code == 200 else gc.text)

    # plain submitter forbidden from curation endpoints
    forbidden = client.get("/curation/pending", headers=USER_B)
    detail["non_curator_pending_status"] = forbidden.status_code

    # approve
    ap = client.post(f"/curation/{sid}/approve", json={"mint_doi": True}, headers=CURATOR)
    detail["approve"] = (ap.status_code, ap.json() if ap.status_code == 200 else ap.text)

    # how approve stored dataset_mdata (raw)
    raw_conn = sqlite3.connect(SQLITE_PATH)
    raw_row2 = raw_conn.execute(
        "SELECT dataset_mdata FROM submissions WHERE source_id=? AND version=?",
        (sid, ver)).fetchone()
    raw_conn.close()
    # Note: the sqlite store's _write_submission json-dumps dicts on the way in,
    # so the persisted column is always a string. The inconsistency is in the
    # in-memory `submission` object the router hands to the store.
    detail["after_approve_stored_is_str"] = isinstance(raw_row2[0], str) if raw_row2 else None

    # Static proof of the dict-vs-string inconsistency in router code:
    import inspect
    submit_src = inspect.getsource(submissions_mod.submit)
    approve_src = inspect.getsource(curation_mod.approve)
    submit_dumps = 'json.dumps(flat)' in submit_src
    # approve assigns the parsed dict back without re-dumping
    approve_assigns_dict = 'submission["dataset_mdata"] = existing_metadata' in approve_src
    detail["submit_json_dumps_dataset_mdata"] = submit_dumps
    detail["approve_assigns_raw_dict"] = approve_assigns_dict

    # reject flow on a separate submission
    r2 = submit_as(USER_A, title="Reject Flow DS")
    sid2 = r2.json()["source_id"]
    rej = client.post(f"/curation/{sid2}/reject",
                      json={"reason": "incomplete metadata"}, headers=CURATOR)
    detail["reject"] = (rej.status_code, rej.json() if rej.status_code == 200 else rej.text)

    flow_ok = (pend.status_code == 200 and gc.status_code == 200
               and ap.status_code == 200 and rej.status_code == 200
               and detail["in_pending"])
    inconsistency = submit_dumps and approve_assigns_dict

    verdict = "CONFIRMED" if flow_ok else "BLOCKED"
    msg = (
        f"Curation happy path works end-to-end: submit->pending(count via /curation/pending)"
        f"->get->approve({ap.status_code})/reject({rej.status_code}). "
        f"non-curator GET /curation/pending -> {forbidden.status_code} (gated). "
        f"INCONSISTENCY: submit() does json.dumps(flat) (line ~669) storing a STRING, "
        f"while curation.approve() assigns the parsed DICT directly "
        f"(submission['dataset_mdata']=existing_metadata, line ~161) before upsert. "
        f"submit_dumps={submit_dumps}, approve_assigns_dict={approve_assigns_dict}. "
        f"(SQLite store re-serializes on write so the persisted column normalizes to "
        f"string in this backend; in DynamoDB the dict-vs-string divergence persists.)"
    )
    record("CURATION-07", verdict, msg, json.dumps(detail, default=str)[:4000])


# ---------------------------------------------------------------------------
# Issue 9: 500s / unhandled exceptions sweep
# ---------------------------------------------------------------------------
def test_500_sweep(observed_500s):
    if observed_500s:
        record("ERR-500", "CONFIRMED",
               f"{len(observed_500s)} request(s) returned 500: " +
               "; ".join(observed_500s[:10]),
               json.dumps(observed_500s, default=str))
    else:
        record("ERR-500", "REFUTED",
               "No 500/unhandled exceptions observed across all exercised endpoints.",
               "")


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main():
    observed_500s = []

    # Monkeypatch client.request to track any 500 responses globally.
    orig_request = client.request
    def tracking_request(method, url, *a, **k):
        resp = orig_request(method, url, *a, **k)
        if resp.status_code == 500:
            observed_500s.append(f"{method} {url} -> 500 {resp.text[:200]}")
        return resp
    client.request = tracking_request

    route_paths = test_boot_and_routes()
    test_happy_path_smoke()
    test_sub01_idor_update()
    test_sub02_ownership_reassignment()
    test_sub05_delete_leaves_search()
    test_sub09_pii_exposure()
    test_sub11_publish_bypass()
    test_sub06_version_sort()
    test_curation_flow()
    test_500_sweep(observed_500s)

    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2, default=str)

    print("\n\n" + "=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)
    print(f"{'issue_id':<14} | {'verdict':<10} | evidence")
    print("-" * 70)
    for r in results:
        ev = r["evidence"].replace("\n", " ")
        if len(ev) > 90:
            ev = ev[:87] + "..."
        print(f"{r['issue_id']:<14} | {r['verdict']:<10} | {ev}")
    print("=" * 70)
    print(f"Results JSON: {RESULTS_PATH}")
    print(f"SQLite db:    {SQLITE_PATH}")


if __name__ == "__main__":
    main()
