#!/usr/bin/env python3
"""Apply Phase-2 test verdicts to issues_register.csv and recompute
feature_validation.csv statuses to the '2-tested' phase.

Verdict sources:
  LIVE   = black-box probe against deployed staging (live_probe.py)
  LOCAL  = in-process TestClient harness (test_local_results.json)
  STATIC = authoritative CloudFormation/source confirmation (test_static_results.json)
"""
import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))

# feature_id -> (verdict, vector, evidence note). Verdict: CONFIRMED / REFUTED / PARTIAL.
# These are the issues we empirically/authoritatively adjudicated.
VERDICTS = {
    # --- security, confirmed against LIVE deployed staging ---
    "SUB-09": ("CONFIRMED", "LIVE+LOCAL", "GET /status/{sid} unauth -> HTTP200 leaks user_id (+user_email/curation_history on native records)"),
    "SUB-10": ("CONFIRMED", "LIVE+LOCAL", "GET /status (query) unauth -> full raw record incl PII/curation_history"),
    "SRCH-03": ("CONFIRMED", "LIVE", "POST /embed unauth -> HTTP200, 1536-dim embedding on MDF OpenAI key"),
    "SRCH-02": ("CONFIRMED", "LIVE", "GET /search/semantic unauth -> HTTP200 (cost amplification)"),
    "CARD-01": ("CONFIRMED", "LIVE", "GET /card unauth exposes download_url"),
    # --- security/functional, confirmed LOCAL (dev-mode in-process) ---
    "SUB-01": ("CONFIRMED", "LOCAL", "user B hijacks A's dataset: /submit update=true + A's mdf_source_id -> 200, new version owned by B"),
    "SUB-02": ("CONFIRMED", "LOCAL", "curator edit of published ds -> new_record.user_id=curator (ownership theft)"),
    "SUB-05": ("CONFIRMED", "LOCAL", "/delete sets status=deleted, ZERO search-delete calls; stays indexed"),
    "SUB-11": ("CONFIRMED", "LOCAL", "/status/update status=published flips status w/o DOI/index/publish job"),
    "SUB-06": ("CONFIRMED", "LOCAL", "GET /versions lexicographic sort: 10.0 before 2.0 (display-only)"),
    "SRCH-01": ("CONFIRMED", "STATIC", "Dynamo fallback search returns all status==published with no acl filter"),
    # --- sync-parity, confirmed LIVE+STATIC ---
    "SYNC-02": ("CONFIRMED", "LIVE+STATIC", "v2 /search docs omit resource_type; v1 query mdf.resource_type:dataset -> 0 v2 datasets"),
    "INFRA-06": ("CONFIRMED", "STATIC", "source_name=source_id.rsplit('-',1)[0] corrupts v1 family grouping for mdf-<uuid> ids"),
    # --- infra/security, STATIC authoritative ---
    "AUTH-02": ("CONFIRMED", "STATIC", "AllowAllCurators=true in samconfig staging:50 AND prod:64 -> every user is curator"),
    "IAC-01": ("CONFIRMED", "STATIC", "GLOBUS_CLIENT_SECRET/DATACITE_PASSWORD/OPENAI_API_KEY plaintext Lambda env (template:223,229,254)"),
    "IAC-02": ("CONFIRMED", "STATIC", "DataCite staging password committed: samconfig.toml:50"),
    "IAC-06": ("PARTIAL", "STATIC", "No PITR/SSE on tables CONFIRMED; tables DO have DeletionPolicy:Retain (catalog claim corrected)"),
    "INFRA-04": ("CONFIRMED", "STATIC", "get_datacite_client silently returns MockDataCiteClient when creds missing even if USE_MOCK_DATACITE=false"),
    "CUR-03": ("CONFIRMED", "STATIC", "_process_publish_submission sets status=published even if DOI/index failed (warn-only)"),
    "FILE-05": ("CONFIRMED", "STATIC", "Globus list_files reads empty per-cold-start in-memory cache; broken in prod"),
    "STOR-04": ("CONFIRMED", "STATIC", "list/size/delete depend on in-memory _metadata_cache lost on cold start"),
}

# Map dimension match: only mark the issue row whose dimension/feature is the tested one.
reg_path = os.path.join(HERE, "issues_register.csv")
rows = list(csv.DictReader(open(reg_path)))
applied = 0
for r in rows:
    fid = r["feature_id"]
    sev = r["severity"]
    if fid in VERDICTS:
        v, vec, note = VERDICTS[fid]
        r["test_result"] = f"{v} [{vec}] {note}"
        applied += 1
    elif sev == "high":
        r["test_result"] = "STATIC-REVIEW (catalog read of code/IaC; not runtime-exercised)"
    elif sev == "med":
        r["test_result"] = "STATIC-REVIEW (code-read; pending runtime confirmation)"
    else:
        r["test_result"] = "NOTED (low; static catalog observation)"

with open(reg_path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=rows[0].keys())
    w.writeheader(); w.writerows(rows)

# Recompute feature_validation statuses -> phase 2-tested.
feat_path = os.path.join(HERE, "feature_validation.csv")
feats = list(csv.DictReader(open(feat_path)))
# build per-feature worst confirmed verdict per dimension from register
from collections import defaultdict
conf = defaultdict(lambda: defaultdict(list))  # fid -> dim -> [(sev, verdict)]
for r in rows:
    verdict = "CONFIRMED" if r["test_result"].startswith("CONFIRMED") else \
              "PARTIAL" if r["test_result"].startswith("PARTIAL") else "STATIC"
    conf[r["feature_id"]][r["dimension"]].append((r["severity"], verdict))

def status_for(fid, dim):
    items = conf.get(fid, {}).get(dim, [])
    if not items:
        return "PASS (no issue found)"
    has_conf_high = any(s == "high" and v == "CONFIRMED" for s, v in items)
    has_conf_med = any(s == "med" and v == "CONFIRMED" for s, v in items)
    has_high = any(s == "high" for s, _ in items)
    has_med = any(s == "med" for s, _ in items)
    if has_conf_high:
        return "FAIL (confirmed high)"
    if has_high:
        return "FAIL? (high, static)"
    if has_conf_med:
        return "AT-RISK (confirmed med)"
    if has_med:
        return "AT-RISK (med, static)"
    return "MINOR (low only)"

dim_col = {"security": "security_status", "functionality": "functionality_status",
           "deployability": "deployability_status", "sync_parity": "sync_parity_status"}
for f in feats:
    for dim, col in dim_col.items():
        f[col] = status_for(f["feature_id"], dim)
    f["phase"] = "2-tested"

with open(feat_path, "w", newline="") as fo:
    w = csv.DictWriter(fo, fieldnames=feats[0].keys())
    w.writeheader(); w.writerows(feats)

print(f"Applied verdicts to {applied} priority issues; {len(rows)} register rows updated; {len(feats)} features re-statused -> 2-tested")
from collections import Counter
c = Counter(f["security_status"].split(" ")[0] for f in feats)
print("security_status dist:", dict(c))
