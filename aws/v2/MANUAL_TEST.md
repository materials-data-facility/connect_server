# Manual Test Plan — v2 Backend with Production Data

**Prerequisites:**
1. Deploy updated code to staging: `cd cs/aws && ./deploy.sh quick staging`
2. 904 production datasets are already ingested (DynamoDB + Globus Search)

**Staging API:** `https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging`

Set a shorthand:
```bash
API=https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging
```

---

## 1. Health Check

```bash
curl -s $API/health | python3 -m json.tool
```

Expected: `{"status": "ok", ...}`

---

## 2. Search (Globus Search Index)

**Full-text search — keyword:**
```bash
curl -s "$API/search?q=perovskite&limit=5" | python3 -m json.tool
```
Expected: Multiple results from Foundry and MDF Open (there are 20+ perovskite datasets).

**Search — author name:**
```bash
curl -s "$API/search?q=Wolverton&limit=5" | python3 -m json.tool
```

**Search — narrow term:**
```bash
curl -s "$API/search?q=electroadhesives" | python3 -m json.tool
```
Expected: `levine_abo2179_database_v2.1` appears.

---

## 3. Dataset Card

**Rich dataset (DOI, multiple authors, related works):**
```bash
curl -s $API/card/levine_abo2179_database_v2.1 | python3 -m json.tool
```
Expected: Title, 5 authors, DOI `10.18126/jx14-t0v8`, related work `10.1126/scirobotics.abo2179`, download_url, keywords.

**Foundry ML dataset:**
```bash
curl -s $API/card/semiconductor_defectlevels_v1.1 | python3 -m json.tool
```

**Minimal dataset (no DOI, no org):**
```bash
curl -s $API/card/jarvis_v1.1 | python3 -m json.tool
```

---

## 4. Citations

**All formats:**
```bash
curl -s "$API/citation/levine_abo2179_database_v2.1?format=all" | python3 -m json.tool
```
Expected: `bibtex`, `ris`, `apa`, `datacite` keys in response.

**BibTeX only:**
```bash
curl -s "$API/citation/levine_abo2179_database_v2.1?format=bibtex" | python3 -m json.tool
```

---

## 5. Status & Versions

**Public status (no auth — published datasets visible):**
```bash
curl -s "$API/status?source_id=levine_abo2179_database_v2.1" | python3 -m json.tool
```
Expected: `status: "published"`, source_id, version, metadata.

**Multi-version dataset:**
```bash
curl -s "$API/versions/Dataset_hea_hardness" | python3 -m json.tool
```
Expected: Two versions (1.0 and 1.1), one with `latest: false`.

**Version diff:**
```bash
curl -s "$API/versions/Dataset_hea_hardness/diff?from=1.0&to=1.1" | python3 -m json.tool
```
Expected: Shows metadata differences between versions.

---

## 6. Dataset Stats (Access Tracking)

**Before — check current counts:**
```bash
curl -s $API/stats/levine_abo2179_database_v2.1 | python3 -m json.tool
```
Expected: `view_count` and `download_count` (initially 0 or low).

**Generate views — hit card and citation:**
```bash
curl -s $API/card/levine_abo2179_database_v2.1 > /dev/null
curl -s $API/card/levine_abo2179_database_v2.1 > /dev/null
curl -s $API/citation/levine_abo2179_database_v2.1 > /dev/null
```

**After — counts should increment:**
```bash
curl -s $API/stats/levine_abo2179_database_v2.1 | python3 -m json.tool
```
Expected: `view_count` increased by 3.

---

## 7. Admin Stats (requires curator auth)

```bash
curl -s $API/admin/stats \
  -H "X-User-Id: test-curator" \
  -H "X-User-Name: Curator" | python3 -m json.tool
```
Expected: `total_submissions: 922`, counts by status (mostly `published`), `access_totals` with aggregate view/download counts.

---

## 8. Submission Lifecycle (dev auth)

**Submit a new dataset:**
```bash
curl -s -X POST $API/submit \
  -H "Content-Type: application/json" \
  -H "X-User-Id: manual-test-user" \
  -H "X-User-Email: test@example.com" \
  -d '{
    "title": "Manual Test Dataset",
    "authors": [{"name": "Test User"}],
    "description": "Testing the v2 backend with production data",
    "keywords": ["test", "manual"],
    "data_sources": ["https://example.com/data.csv"]
  }' | python3 -m json.tool
```
Save the returned `source_id`.

**Check status:**
```bash
curl -s "$API/status/<SOURCE_ID>" \
  -H "X-User-Id: manual-test-user" | python3 -m json.tool
```
Expected: `status: "pending_curation"`.

**Edit metadata:**
```bash
curl -s -X POST "$API/submissions/<SOURCE_ID>/metadata" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: manual-test-user" \
  -d '{"title": "Manual Test Dataset (Edited)"}' | python3 -m json.tool
```

**Approve (as curator):**
```bash
curl -s -X POST "$API/curation/<SOURCE_ID>/approve" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: test-curator" \
  -H "X-User-Name: Curator" \
  -d '{"notes": "Looks good"}' | python3 -m json.tool
```

**Verify it shows up in search:**
```bash
curl -s "$API/search?q=Manual+Test+Dataset" | python3 -m json.tool
```

**Soft-delete (as curator):**
```bash
curl -s -X POST "$API/submissions/<SOURCE_ID>/delete" \
  -H "Content-Type: application/json" \
  -H "X-User-Id: test-curator" \
  -H "X-User-Name: Curator" \
  -d '{"reason": "manual test cleanup"}' | python3 -m json.tool
```

---

## 9. CLI Smoke Tests

These require `mdf` CLI configured to point at staging.

```bash
# Search
mdf search "perovskite" --service staging --limit 5

# Show a dataset
mdf show levine_abo2179_database_v2.1 --service staging

# Citation
mdf cite levine_abo2179_database_v2.1 --format bibtex --service staging

# Versions
mdf versions Dataset_hea_hardness --service staging

# Version diff
mdf diff Dataset_hea_hardness --from 1.0 --to 1.1 --service staging

# Stats
mdf stats --service staging --dev-user test-curator

# Preview
mdf preview levine_abo2179_database_v2.1 --service staging

# List user submissions
mdf list --service staging
```

---

## 10. Spot-Check Data Integrity

Verify a few records have correct field mapping from v1 data:

**Check DOI round-trip:**
```bash
curl -s $API/card/abx3_perovs_alloys_v1.1 | python3 -c "
import sys, json
card = json.load(sys.stdin)
c = card.get('card', {})
print('DOI:', c.get('doi'))
print('Download:', c.get('download_url'))
print('Org:', c.get('organization'))
print('Authors:', len(c.get('authors', [])))
"
```
Expected: DOI=`10.18126/3jn1-o5nk`, download_url contains `data.materialsdatafacility.org`, org=`MDF Open`.

**Check ML metadata preserved:**
```bash
curl -s "$API/status?source_id=semiconductor_defectlevels_v1.1" \
  -H "X-User-Id: test" | python3 -c "
import sys, json
data = json.load(sys.stdin)
mdata = data.get('submission', {}).get('dataset_mdata', {})
if isinstance(mdata, str):
    mdata = json.loads(mdata)
ml = mdata.get('ml', {})
print('ML data_format:', ml.get('data_format'))
print('ML keys:', len(ml.get('keys', [])))
print('ML splits:', len(ml.get('splits', [])))
"
```
Expected: `data_format`, keys, and splits populated from original Foundry schema.

---

## Quick Pass/Fail Checklist

| # | Test | Pass? |
|---|------|-------|
| 1 | Health returns ok | |
| 2 | Search returns perovskite results | |
| 3 | Card shows full metadata for DOI dataset | |
| 4 | Card works for minimal dataset (no DOI/org) | |
| 5 | Citation returns bibtex/ris/apa | |
| 6 | Status returns published record | |
| 7 | Versions lists 2 versions for Dataset_hea_hardness | |
| 8 | Version diff shows changes | |
| 9 | Stats shows view_count | |
| 10 | View count increments after card hits | |
| 11 | Admin stats shows 922+ submissions | |
| 12 | Submit + approve + search lifecycle works | |
| 13 | Soft-delete works | |
| 14 | DOI and download_url preserved from v1 | |
| 15 | ML metadata preserved for Foundry datasets | |
