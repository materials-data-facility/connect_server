"""Post-fix verification for the fixes NOT covered by test_local_harness.py.

Covers: SYNC-02 (resource_type), INFRA-06 (source_name), INFRA-04 (datacite
fail-loud), SRCH-03/SRCH-02 (embed/semantic require auth), SRCH-01 (fallback ACL
filter), CUR-03 (publish gated on search ingest), and the dataset_mdata type
consistency fix. Verdict semantics here are POSITIVE: PASS = fix works.

Run FROM /Users/blaiszik/Desktop/git/mdf_client/cs/aws with the prepared venv:
    /tmp/mdfv312/bin/python v2/validation/test_postfix_extra.py
"""
import json
import os
import sys
import uuid

_U = uuid.uuid4().hex[:8]
SQLITE_PATH = f"/tmp/mdf_pfx_{_U}.db"
os.environ.update({
    "STORE_BACKEND": "sqlite", "SQLITE_PATH": SQLITE_PATH, "STORAGE_BACKEND": "local",
    "AUTH_MODE": "dev", "LOCAL_DEV_AUTH": "true", "ALLOW_ALL_CURATORS": "false",
    "CURATOR_GROUP_IDS": "", "REQUIRED_GROUP_MEMBERSHIP": "", "USE_MOCK_DATACITE": "true",
    "ASYNC_DISPATCH_MODE": "inline", "USE_MOCK_SEARCH": "true", "CURATOR_USER_IDS": "curator-user",
})
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from fastapi.testclient import TestClient  # noqa: E402
from v2.app import app  # noqa: E402
import v2.app.routers.submissions as submissions_mod  # noqa: E402

client = TestClient(app, raise_server_exceptions=False)
results = []


def check(name, passed, evidence):
    results.append({"check": name, "verdict": "PASS" if passed else "FAIL", "evidence": evidence})
    print(f"[{'PASS' if passed else 'FAIL'}] {name}: {evidence}")


CUR = {"X-User-Id": "curator-user", "X-User-Email": "curator@example.com"}
A = {"X-User-Id": "user-A", "X-User-Email": "a@example.com"}


def submit(headers, title, acl=None, sid=None, update=False, data_sources=None):
    md = {"title": title, "authors": [{"name": "Au"}],
          "data_sources": data_sources if data_sources is not None else ["https://example.com/d.csv"]}
    if acl is not None:
        md["acl"] = acl
    ext = {}
    if sid:
        ext["mdf_source_id"] = sid
    if ext:
        md["extensions"] = ext
    if update:
        md["update"] = True
    return client.post("/submit", json=md, headers=headers)


# --- SYNC-02 + INFRA-06: build_gmeta_entry -------------------------------
from v2.search_client import GlobusSearchClient  # noqa: E402
_bare = GlobusSearchClient.__new__(GlobusSearchClient)

sub_migrated = {"source_id": "mdf-abc123def456", "version": "1.0", "organization": "MDF Open",
                "dataset_mdata": json.dumps({"title": "T", "authors": [{"name": "A"}],
                    "acl": ["public"], "extensions": {"mdf_source_name": "pub_42_smith"}})}
e1 = _bare.build_gmeta_entry(sub_migrated)
mdf1 = e1["content"]["mdf"]
check("SYNC-02 resource_type written", mdf1.get("resource_type") == "dataset",
      f"mdf.resource_type={mdf1.get('resource_type')!r}")
check("INFRA-06 source_name preserved from v1 extensions", mdf1.get("source_name") == "pub_42_smith",
      f"mdf.source_name={mdf1.get('source_name')!r} (was corrupted to 'mdf' by rsplit before)")

sub_native = {"source_id": "mdf-deadbeefcafe", "version": "1.0",
              "dataset_mdata": json.dumps({"title": "T2", "authors": [{"name": "A"}], "acl": ["public"]})}
mdf2 = _bare.build_gmeta_entry(sub_native)["content"]["mdf"]
check("INFRA-06 native source_name not corrupted", mdf2.get("source_name") == "mdf-deadbeefcafe",
      f"mdf.source_name={mdf2.get('source_name')!r} (full stable id, not 'mdf')")

# --- INFRA-04: datacite fails loud when mock disabled + no creds ----------
from v2 import datacite  # noqa: E402
_saved = {k: os.environ.get(k) for k in ("USE_MOCK_DATACITE", "DATACITE_USERNAME", "DATACITE_PASSWORD")}
os.environ["USE_MOCK_DATACITE"] = "false"
os.environ.pop("DATACITE_USERNAME", None)
os.environ.pop("DATACITE_PASSWORD", None)
raised = False
try:
    datacite.get_datacite_client()
except RuntimeError as ex:
    raised = True
    msg = str(ex)
check("INFRA-04 datacite raises (no silent mock) when USE_MOCK_DATACITE=false + no creds", raised,
      f"raised RuntimeError={raised}")
# auto-mode still mocks
os.environ["USE_MOCK_DATACITE"] = ""
auto_client = datacite.get_datacite_client()
check("INFRA-04 auto/unset still falls back to mock (dev unaffected)",
      type(auto_client).__name__ == "MockDataCiteClient", f"auto -> {type(auto_client).__name__}")
for k, v in _saved.items():
    if v is None:
        os.environ.pop(k, None)
    else:
        os.environ[k] = v

# --- SRCH-03 / SRCH-02: embed + semantic require auth (prod mode) ---------
os.environ["AUTH_MODE"] = "production"
os.environ.pop("LOCAL_DEV_AUTH", None)
r_embed = client.post("/embed", json={"text": "hello"})        # no Authorization
r_sem = client.get("/search/semantic", params={"q": "hello"})  # no Authorization
r_kw = client.get("/search", params={"q": "hello"})            # public keyword search must still work
check("SRCH-03 POST /embed requires auth", r_embed.status_code == 401, f"/embed unauth -> {r_embed.status_code}")
check("SRCH-02 GET /search/semantic requires auth", r_sem.status_code == 401, f"/search/semantic unauth -> {r_sem.status_code}")
check("Public keyword /search still anonymous", r_kw.status_code == 200, f"/search unauth -> {r_kw.status_code}")
os.environ["AUTH_MODE"] = "dev"
os.environ["LOCAL_DEV_AUTH"] = "true"

# --- SRCH-01: Dynamo fallback excludes non-public datasets ----------------
from v2.search import _is_public_dataset, search_datasets  # noqa: E402
check("SRCH-01 _is_public_dataset(public)", _is_public_dataset({"dataset_mdata": json.dumps({"acl": ["public"]})}) is True, "acl=[public] -> public")
check("SRCH-01 _is_public_dataset(restricted)", _is_public_dataset({"dataset_mdata": json.dumps({"acl": ["urn:globus:auth:identity:x"]})}) is False, "acl=[identity] -> not public")
check("SRCH-01 _is_public_dataset(no acl defaults public)", _is_public_dataset({"dataset_mdata": json.dumps({})}) is True, "no acl -> public")

# Integration: publish one public + one restricted, clear mock index so the
# DynamoDB fallback path runs, then confirm only the public one is returned.
rp = submit(A, "ZEBRAQUERY public dataset", acl=["public"])
sp = rp.json()["source_id"]
client.post(f"/curation/{sp}/approve", json={"mint_doi": True}, headers=CUR)
rr = submit(A, "ZEBRAQUERY restricted dataset", acl=["urn:globus:auth:identity:secret-user"])
sr = rr.json()["source_id"]
client.post(f"/curation/{sr}/approve", json={"mint_doi": True}, headers=CUR)
from v2.search_client import get_search_client  # noqa: E402
mock = get_search_client()
if hasattr(mock, "_entries"):
    mock._entries.clear()  # force fallback to local Dynamo/sqlite scan
fb = search_datasets("ZEBRAQUERY", limit=20)
ids = {r.get("source_id") for r in fb.get("results", [])}
check("SRCH-01 fallback returns public dataset", sp in ids, f"public {sp} in results={sp in ids}")
check("SRCH-01 fallback EXCLUDES restricted dataset", sr not in ids, f"restricted {sr} excluded={sr not in ids}")

# --- CUR-03: publish gated on search ingest success -----------------------
rc = submit(A, "Gate publish on ingest")
sc_sid = rc.json()["source_id"]
sc_ver = rc.json()["version"]
mock2 = get_search_client()
_orig_ingest = mock2.ingest
mock2.ingest = lambda *a, **k: {"success": False, "error": "simulated ingest failure"}
ap = client.post(f"/curation/{sc_sid}/approve", json={"mint_doi": True}, headers=CUR)
mock2.ingest = _orig_ingest
store = submissions_mod.get_submission_store()
rec = store.get_submission(sc_sid, sc_ver)
final_status = rec.get("status") if rec else None
check("CUR-03 publish aborted (status != published) when search ingest fails",
      final_status != "published",
      f"approve->{ap.status_code}; final status={final_status!r} (expected NOT 'published')")

# --- dataset_mdata type consistency (CURATION-07 latent) -------------------
import inspect  # noqa: E402
import v2.app.routers.curation as curation_mod  # noqa: E402
approve_src = inspect.getsource(curation_mod.approve)
consistent = 'json.dumps' in approve_src and 'submission["dataset_mdata"] = existing_metadata' not in approve_src
check("CURATION-07 approve stores dataset_mdata as JSON string (Dynamo-consistent)",
      consistent, "approve now json.dumps dataset_mdata" if consistent else "approve still assigns raw dict")

# ---- summary -------------------------------------------------------------
RESULTS_PATH = os.path.join(os.path.dirname(__file__), "test_postfix_extra_results.json")
with open(RESULTS_PATH, "w") as f:
    json.dump(results, f, indent=2, default=str)
npass = sum(1 for r in results if r["verdict"] == "PASS")
print("\n" + "=" * 64)
print(f"POST-FIX EXTRA: {npass}/{len(results)} checks PASS")
for r in results:
    if r["verdict"] != "PASS":
        print(f"  FAIL: {r['check']} -- {r['evidence']}")
print("=" * 64)
print(f"Results JSON: {RESULTS_PATH}")
