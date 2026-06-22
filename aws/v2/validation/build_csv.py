#!/usr/bin/env python3
"""Merge the three feature catalogs into the canonical validation CSV + an issues register.

Canonical CSV (feature_validation.csv): one row per feature, with provisional status
across the four dimensions (security, functionality, deployability, sync_parity),
derived from the catalog issues. Statuses evolve through the test/fix/re-test phases.

Issues register (issues_register.csv): one row per discovered issue, used to drive
the test -> fix -> re-test loop.
"""
import csv
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = ["catalog_submissions.json", "catalog_files_search.json", "catalog_infra_security_sync.json"]

DIM_MAP = {
    "security": "security",
    "functionality": "functionality",
    "func": "functionality",
    "deployability": "deployability",
    "deploy": "deployability",
    "sync": "sync_parity",
    "sync_parity": "sync_parity",
    "sync-parity": "sync_parity",
}

# provisional status from worst issue severity in a dimension (catalog phase, pre-test)
def provisional(severity_set):
    if "high" in severity_set:
        return "FAIL?(catalog-high)"
    if "med" in severity_set or "medium" in severity_set:
        return "AT-RISK(catalog-med)"
    if "low" in severity_set:
        return "MINOR(catalog-low)"
    return "UNTESTED"


def as_list(v):
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def join(v, sep=" | "):
    if isinstance(v, list):
        return sep.join(str(x) for x in v)
    return str(v) if v is not None else ""


features = []
issues = []

for fn in SRC:
    path = os.path.join(HERE, fn)
    with open(path) as f:
        data = json.load(f)
    for rec in data:
        fid = rec.get("feature_id") or rec.get("id") or "?"
        # collect issues per dimension
        dim_sev = {"security": set(), "functionality": set(), "deployability": set(), "sync_parity": set()}
        rec_issues = as_list(rec.get("issues"))
        issue_summ = []
        for iss in rec_issues:
            if not isinstance(iss, dict):
                continue
            dim = DIM_MAP.get(str(iss.get("dimension", "")).lower().strip(), "functionality")
            sev = str(iss.get("severity", "")).lower().strip()
            dim_sev[dim].add(sev)
            issues.append({
                "feature_id": fid,
                "area": rec.get("area", ""),
                "feature_name": rec.get("name", ""),
                "severity": sev,
                "dimension": dim,
                "description": iss.get("desc") or iss.get("description", ""),
                "evidence": iss.get("evidence", ""),
                "test_result": "",        # filled in Phase 2
                "fix_status": "OPEN",      # OPEN / FIXED / WONTFIX / NOT-A-BUG
                "fix_note": "",
            })
            issue_summ.append(f"[{sev}/{dim.split('_')[0]}] {iss.get('desc') or iss.get('description','')}")

        features.append({
            "feature_id": fid,
            "area": rec.get("area", ""),
            "name": rec.get("name", ""),
            "files": join(rec.get("files")),
            "user_story": rec.get("user_story", ""),
            "expected_behavior": rec.get("expected_behavior", ""),
            "inputs": join(rec.get("inputs")),
            "outputs": join(rec.get("outputs")),
            "side_effects": join(rec.get("side_effects")),
            "error_paths": join(rec.get("error_paths")),
            "security_posture": join(rec.get("security")),
            "deployability_posture": join(rec.get("deployability")),
            "sync_parity_posture": join(rec.get("sync_parity")),
            "security_status": provisional(dim_sev["security"]),
            "functionality_status": provisional(dim_sev["functionality"]),
            "deployability_status": provisional(dim_sev["deployability"]),
            "sync_parity_status": provisional(dim_sev["sync_parity"]),
            "phase": "1-cataloged",
            "issues_summary": " ;; ".join(issue_summ),
        })

# stable sort by feature_id area then id
features.sort(key=lambda r: (r["area"], r["feature_id"]))

feat_cols = ["feature_id", "area", "name", "files", "user_story", "expected_behavior",
             "inputs", "outputs", "side_effects", "error_paths",
             "security_posture", "deployability_posture", "sync_parity_posture",
             "security_status", "functionality_status", "deployability_status", "sync_parity_status",
             "phase", "issues_summary"]

with open(os.path.join(HERE, "feature_validation.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=feat_cols)
    w.writeheader()
    for r in features:
        w.writerow(r)

# issues register sorted by severity then dimension
sev_rank = {"high": 0, "med": 1, "medium": 1, "low": 2, "": 3}
issues.sort(key=lambda r: (sev_rank.get(r["severity"], 3), r["dimension"], r["feature_id"]))
iss_cols = ["feature_id", "area", "feature_name", "severity", "dimension", "description",
            "evidence", "test_result", "fix_status", "fix_note"]
with open(os.path.join(HERE, "issues_register.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=iss_cols)
    w.writeheader()
    for r in issues:
        w.writerow(r)

# print stats
from collections import Counter
sev_c = Counter(i["severity"] for i in issues)
dim_c = Counter(i["dimension"] for i in issues)
area_c = Counter(f["area"] for f in features)
print(f"Features: {len(features)}  | Issues: {len(issues)}")
print("By severity:", dict(sev_c))
print("By dimension:", dict(dim_c))
print("By area:", dict(area_c))
print("Wrote feature_validation.csv and issues_register.csv")
