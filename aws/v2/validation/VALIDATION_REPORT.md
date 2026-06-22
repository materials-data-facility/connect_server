# MDF Connect v2 — Backend Validation Report

**Scope:** `aws/v2` (FastAPI + Mangum single-Lambda backend) and its IaC (`aws/template.yaml`, `aws/samconfig.toml`, `aws/deploy.sh`).
**Date:** 2026-06-22 · **Branch:** `v2-backend-curation`
**Loop completed:** catalog → test → fix → re-test → confirm sync-parity.

---

## 1. Executive summary

- **73 features/endpoints/services cataloged** into user stories with expected behavior across the four required dimensions (security, functionality, deployability, sync-parity). Canonical record: `feature_validation.csv`.
- **153 issues** found and recorded (`issues_register.csv`): **19 high / 54 med / 80 low**.
- Findings were **empirically confirmed**, not just read from code, via three vectors:
  1. **Live black-box** probes against the deployed staging API (`https://hjccjf3eqg.execute-api.us-east-1.amazonaws.com/staging`).
  2. **Local in-process** execution of the exact deployable artifact (FastAPI `TestClient`, Python 3.12 = Lambda runtime) — `test_local_harness.py`.
  3. **Static IaC** review (authoritative for "what gets deployed") — `test_static_results.json`.
- **Fixes applied and verified:** **44 issues FIXED+RETESTED** (code, re-verified locally), **4 FIXED-IN-SOURCE** (IaC/config; effective on next deploy), **4 deferred/documented high** items needing operator/infra action. 14 source files changed (+310/−41).
- **Every confirmed security gap and every confirmed sync-parity breaker that is fixable-and-verifiable in this environment has been fixed and re-tested.**

### Environment constraint (important, affects "deployed backend" testing)
This workstation has **no `aws`/`sam` CLI and no AWS credentials**, so the live CloudFormation/Lambda/DynamoDB stack could not be queried or redeployed from here. Validation therefore used: (a) the **live HTTPS staging endpoint** for black-box behavior, (b) **local execution of the identical Lambda code** (`Handler: v2.app.main.handler`, `CodeUri: .`) for authenticated flows, and (c) **CloudFormation source** for IaC posture. **Consequence: code/IaC fixes are committed to source and verified locally/statically, but a maintainer must run `./deploy.sh staging` / `prod` to make them live on AWS.**

---

## 2. What was validated (the four dimensions)

| Dimension | How validated | Result |
|---|---|---|
| **Functionality** | Live probes + local TestClient happy-path & edge/error paths across all 40 operations | No unhandled 500s; bugs found, fixed, re-tested |
| **Security** | authN/authZ tests (IDOR, ACL bypass, PII exposure, unauth billable proxy), IaC IAM/secret/encryption review | 9 confirmed-high security gaps; all locally-fixable ones closed |
| **Deployability** | template.yaml/samconfig/deploy.sh review; runtime-parity boot under py3.12 | Reproducible from IaC except documented out-of-band SSM/SES prereqs |
| **Sync-parity** | search-doc schema vs v1 index; `migrate_v1_payload` execution; sync scripts | 2 confirmed-high parity breakers fixed; cutover-process gap documented |

**Previous stack (sync target):** MDF v1 production Globus Search index `1a57bbe5-5272-477f-9d31-343b8258b7a5`. v2 prod index `fed3e94e-…`, staging/test `ab19b80b-…`. Sync pipeline: `scripts/extract_mdf_production_datasets.py` → `convert_production_datasets.py` (`migrate_v1_payload`) → `ingest_converted_datasets.py`.

---

## 3. Confirmed high-severity issues & disposition

| ID | Dimension | Issue (confirmed) | Disposition |
|---|---|---|---|
| **SUB-01** | security | IDOR: any submitter could `POST /submit update=true` with another user's `mdf_source_id` and hijack the dataset's version chain/DOI | **FIXED+RETESTED** — ownership guard; B→403 |
| **SUB-02** | security | Curator editing a published dataset silently became the owner of the new version | **FIXED+RETESTED** — original owner preserved |
| **SUB-09/10** | security | `GET /status` (unauth) returned the full raw record: `user_email`, `user_id`, `curation_history`, approver/rejector ids, transfer internals — **confirmed live** | **FIXED+RETESTED** — sanitized public view for non-owner/non-curator |
| **SRCH-03** | security | `POST /embed` was an **unauthenticated, billable** proxy to OpenAI — **confirmed live (HTTP 200, 1536-d vector)** | **FIXED+RETESTED** — requires auth (401 unauth) |
| **SRCH-02** | security | `GET /search/semantic` unauth, embeds query server-side (cost amplification) — **confirmed live** | **FIXED+RETESTED** — requires auth; anon keyword `/search` still public |
| **SRCH-01** | security | Dynamo fallback search returned every `published` record **ignoring ACL** (restricted datasets leak when Globus path unavailable) | **FIXED+RETESTED** — ACL filter; restricted excluded |
| **CARD-01/03, PREV-03/04/06** | security | Card/citation/detail/preview served restricted-but-published datasets (incl. **sample data rows**, download_url) to anonymous callers | **FIXED+RETESTED** — visibility gate; restricted→404 anon, curator→200 |
| **AUTH-02** | security | `AllowAllCurators=true` in **staging AND prod** → every authenticated user a curator (approve/publish, mint real `10.18126` DOIs, delete any dataset) | **FIXED-IN-SOURCE** — removed from overrides; prod now gates on `CuratorGroupIds` |
| **IAC-02** | security | DataCite staging password committed in `samconfig.toml` | **FIXED-IN-SOURCE** — removed; resolve via SSM. **Operator must rotate the leaked password** |
| **IAC-01** | security | `GLOBUS_CLIENT_SECRET`/`DATACITE_PASSWORD`/`OPENAI_API_KEY` as plaintext Lambda env vars | **DOCUMENTED** — migrate to Secrets Manager / `{{resolve:ssm-secure}}` + CMK env encryption (deploy redesign) |
| **IAC-06** | deployability | DynamoDB system-of-record had no SSE + no PITR backup | **FIXED-IN-SOURCE** — added `SSESpecification` + `PointInTimeRecoverySpecification` (tables already `Retain`) |
| **SYNC-02** | sync | v2 search docs **omit `mdf.resource_type`**; v1 extraction query `mdf.resource_type:"dataset"` returns **0** v2-native datasets — **confirmed live** | **FIXED+RETESTED** — `resource_type="dataset"` written |
| **INFRA-06** | sync | `source_name = source_id.rsplit("-",1)[0]` corrupted the v1 family/facet grouping key for `mdf-<uuid>` ids | **FIXED+RETESTED** — preserves `extensions.mdf_source_name` or full stable id |
| **SUB-05** | sync | Deleting a published dataset left it in the search index (dead links, dirty re-sync) | **FIXED+RETESTED** — reconciles index (delete or re-point to new latest) |
| **SUB-11** | sync | Curator `POST /status/update status=published` flipped status with no DOI/index (Dynamo/search drift) | **FIXED+RETESTED** — blocked (400); must use approve pipeline |
| **CUR-03** | functionality | Publish set `status=published` even when DOI mint AND search ingest failed (published-but-invisible) | **FIXED+RETESTED** — gated on search-ingest success (else `publish_failed`, retried) |
| **INFRA-04** | deployability | `get_datacite_client` silently returned a mock (fake `10.99999/…` DOI) even with `USE_MOCK_DATACITE=false` | **FIXED+RETESTED** — raises when mock disabled + no creds; auto still mocks for dev |
| **FILE-05 / STOR-04** | functionality | `GlobusHTTPSStorage` file list/size/delete read a per-process in-memory cache that's empty after every Lambda cold start → broken in prod | **DEFERRED** — needs Dynamo-backed file metadata + live Globus integration test |
| **SYNC-04** | sync | No dual-write / rollback / parity-verification cutover plan | **DOCUMENTED** — operational runbook (see §6) |

---

## 4. Additional fixes (beyond the high list)

- **Security headers** (`middleware.py`): added `X-Content-Type-Options`, `X-Frame-Options: DENY`, `Referrer-Policy`, `Strict-Transport-Security` (verified present on responses).
- **S3 buckets**: `BucketEncryption` (AES256) + `DeletionPolicy/UpdateReplacePolicy: Retain`.
- **SQS queue + DLQ**: `SqsManagedSseEnabled: true`.
- **`dataset_mdata` type consistency**: curation `approve` now `json.dumps` the metadata (was a raw dict on one path, a JSON string on others — a latent Dynamo Map-vs-String divergence).
- **Version sort** (`SUB-06`): numeric `version_sort_key` (was lexicographic — `10.0 < 2.0`).

---

## 5. How to re-verify (artifacts in `aws/v2/validation/`)

```bash
# from aws/ with the prepared py3.12 venv (Lambda parity)
/tmp/mdfv312/bin/python v2/validation/test_local_harness.py    # security/functional: all bug verdicts REFUTED
/tmp/mdfv312/bin/python v2/validation/test_postfix_extra.py    # 15/15 PASS (sync/auth/datacite/acl/publish-gate)
/tmp/mdfv312/bin/python v2/validation/live_probe.py            # live staging black-box (pre-deploy state)
```
- `feature_validation.csv` — **canonical** per-feature status across the 4 dimensions (phase `4-retested`).
- `issues_register.csv` — 153 issues with `test_result`, `fix_status`, `retest_result`.
- `catalog_*.json` — raw per-area catalogs. `build_csv.py` / `apply_verdicts.py` / `finalize_csv.py` — the CSV pipeline.

---

## 6. Sync-parity / fast-cutover readiness

**Now compatible (after deploy):** the v2 search document carries `mdf.resource_type="dataset"` and a non-corrupted `mdf.source_name`, plus the v1 `content.mdf/dc/data` layout — so v1 tooling can enumerate the v2 index and a re-sync/cutover is clean. Deletes and failed publishes no longer leave drift between DynamoDB and the index.

**Operator actions required for a fast, clean cutover:**
1. **Deploy the fixes** (`./deploy.sh staging`, then `prod`) — fixes are in source only.
2. **Re-index** existing v2 datasets so they gain `resource_type`/fixed `source_name` (`POST /admin/embeddings/rebuild` is for embeddings; for search, re-run ingest on published records).
3. **Cutover runbook (SYNC-04):** add dual-write or a freeze window, a parity-verification pass (count + field diff v1↔v2), and a documented rollback before flipping the portal to v2-only.
4. **Identity mapping:** migration sets `source_id := source_name` and `user_id="v1-migration"`; keep the `--redirect-map` and stand up a redirect layer so legacy `source_id` links don't break.

---

## 7. Remaining recommendations (not code-fixed here)

- **IAC-01** secrets → AWS Secrets Manager / `{{resolve:ssm-secure}}` + KMS env encryption; **rotate** the committed DataCite password (IAC-02).
- **Least-privilege IAM**: scope `ses:SendEmail` with a `ses:FromAddress` condition; scope S3 CRUD to the `streams/` and `embeddings/` prefixes.
- **FILE-05/STOR-04**: back Globus file metadata with DynamoDB (survive cold starts).
- **WAF**: front the HTTP API with CloudFront + WAF for prod.
- **Error envelope**: unify on one shape (`{success, …}`) — 500 vs 413/429 vs HTTPException currently differ.
- **Rate limiting** is per-Lambda-container (in-memory); use a shared store for a true global limit.

---

*All application-code fixes were verified by re-running the local harness (CONFIRMED→REFUTED) and the supplementary checks (15/15 PASS). IaC/config fixes were validated by CloudFormation-source parse + property assertions. No live AWS deploy was performed (no CLI/credentials in this environment).*
