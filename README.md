Who is working on topics 11/12G


# MDF Connect

The Materials Data Facility Connect service is the backend for submitting, curating, and publishing datasets to MDF Search. For the Python client, see [connect_client](https://github.com/materials-data-facility/connect_client).

## v2 Backend

The v2 backend lives in `aws/v2/` and is a complete rewrite: a single FastAPI application deployed to AWS Lambda via Mangum + SAM. It replaces the old per-endpoint Lambda functions and Terraform deployment.

**Stack**: Python 3.12, FastAPI, Pydantic v2, DynamoDB, Globus HTTPS storage, DataCite DOI minting, Globus Search, AWS SAM.

### What it does

- **Dataset submission**: submit → pending_curation → approved (DOI minted) → published (indexed to Globus Search)
- **Versioning**: update existing datasets with automatic version incrementing, version history via `GET /versions/{source_id}`
- **Datasets by reference**: datasets are submitted and published by reference to existing Globus data sources (`data_sources`); the in-band file streaming/upload feature (create stream → upload files to Globus HTTPS → snapshot → close with DOI) is **disabled for the initial v2 release** — its router is unmounted (see `aws/v2/app/__init__.py`) and can be re-enabled later
- **Curation**: pending list, approve/reject, curator guards
- **Discovery**: search, dataset cards, citations (BibTeX/APA/RIS), file preview (profile-based, from `data_sources`)
- **Auth**: Globus token validation (prod) or `X-User-Id` headers (dev)

### API endpoints

| Group | Endpoints |
|-------|-----------|
| **Submissions** | `POST /submit`, `GET /versions/{id}`, `GET /status/{id}`, `POST /status/update`, `GET /submissions` |
| **Streams / Files** *(disabled for initial v2 release)* | ~~`POST /stream/create`, `POST ../append`, `GET /stream/{id}`, `POST ../close`, `POST ../snapshot`~~, ~~`POST ../upload`, `POST ../upload-url`, `POST ../upload-confirm`, `POST ../download-url`, `GET ../files`~~ — routers unmounted; datasets are published by reference to Globus data sources instead |
| **Curation** | `GET /curation/pending`, `GET /curation/{id}`, `POST ../approve`, `POST ../reject` |
| **Search** | `GET /search` |
| **Cards** | `GET /card/{id}`, `GET /citation/{id}` |
| **Preview** | `GET /preview/{id}`, `GET ../files`, `GET ../files/{path}`, `GET ../sample` |
| **Health** | `GET /health` |

### Deployment architecture

```
Client (mdf_agent CLI / SDK)              AWS us-east-1
─────────────────────────────             ────────────────
Bearer: auth.globus.org token ──────────> API Gateway (HttpApi)
X-Globus-Token: data token                    │
                                              ▼
                                    ┌─── ApiFunction (Lambda) ───┐
                                    │  FastAPI + Mangum           │
                                    │  Auth, Submit, Stream,      │
                                    │  Search, Curation, Cards    │
                                    └──────┬─────────┬────────────┘
                                           │         │
                                    ┌──────┴───┐  ┌──┴──────────────┐
                                    │ DynamoDB  │  │  SQS            │
                                    │ submissions│  │  async-jobs     │
                                    │ streams   │  └──┬──────────────┘
                                    └───────────┘     │
                                                      ▼
                                    ┌─── AsyncWorkerFunction (Lambda) ──┐
                                    │  DOI minting (DataCite)            │
                                    │  Globus Search ingest              │
                                    │  Dataset profiling                 │
                                    └────────────────────────────────────┘
```

### Internal service architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  FastAPI Application  (v2/app/__init__.py)                                  │
│                                                                             │
│  Middleware: CORS, request logging                                          │
│                                                                             │
│  ┌─── Routers (v2/app/routers/) ─────────────────────────────────────────┐  │
│  │                                                                       │  │
│  │  submissions.py          streams.py           files.py                │  │
│  │  ├ POST /submit          (disabled for v2 —    (disabled for v2 —      │  │
│  │  ├ GET  /versions/{id}    router unmounted,      router unmounted,     │  │
│  │  ├ GET  /status/{id}      see app/__init__.py)   see app/__init__.py) │  │
│  │  ├ POST /status/update                                                │  │
│  │  └ GET  /submissions                                                  │  │
│  │                                                                       │  │
│  │  curation.py             search.py    cards.py       preview.py       │  │
│  │  ├ GET  /curation/pending├ GET /search├ GET /card/{id}├ GET dataset.. │  │
│  │  ├ GET  /curation/{id}   │            └ GET /cite/{id}├ GET ../files  │  │
│  │  ├ POST ../approve       │                            ├ GET ../files/{path}
│  │  └ POST ../reject        │                            └ GET ../sample │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│         │                │                │                                  │
│         ▼                ▼                ▼                                  │
│  ┌── Auth ──────┐ ┌─ Dependencies ─┐ ┌── Helpers ──────────────────────┐   │
│  │  (auth.py)   │ │  (deps.py)     │ │  metadata.py   DatasetMetadata  │   │
│  │              │ │                │ │  citation.py   BibTeX/APA/RIS   │   │
│  │  dev mode:   │ │  Singletons:   │ │  search.py     full-text search │   │
│  │   X-User-Id  │ │  submission    │ │  dataset_card.py  card builder  │   │
│  │              │ │  store         │ │  preview.py    file previews    │   │
│  │  production: │ │  stream store  │ │  profiler.py   dataset profiles │   │
│  │   Globus     │ │  storage       │ │  curation.py   DOI + approve    │   │
│  │   userinfo() │ │  backend       │ │  datacite.py   DOI minting      │   │
│  └──────────────┘ └──────┬─────────┘ └─────────────────────────────────┘   │
│                          │                                                  │
└──────────────────────────┼──────────────────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
┌─── Store Layer ──┐ ┌─ Stream Store ─┐ ┌── Storage Layer ──────────────────┐
│  (store.py)      │ │(stream_store.py)│ │  (storage/)                      │
│                  │ │                │ │                                   │
│  SubmissionStore │ │  StreamStore   │ │  StorageBackend                   │
│  (abstract)      │ │  (abstract)    │ │  (abstract)                      │
│    │             │ │    │           │ │    │                              │
│    ├─ Dynamo     │ │    ├─ Dynamo   │ │    ├─ GlobusHTTPSStorage (prod)  │
│    │  SubmStore  │ │    │  StrmStore│ │    │  PUT/GET to data.mdf.org    │
│    │             │ │    │           │ │    │                              │
│    └─ Sqlite     │ │    └─ Sqlite   │ │    ├─ S3Storage                  │
│       SubmStore  │ │       StrmStore│ │    │  AWS S3 bucket              │
│                  │ │                │ │    │                              │
│  Operations:     │ │  Operations:   │ │    └─ LocalStorage (dev)         │
│  put, get, list  │ │  create, get   │ │       Local filesystem           │
│  upsert, update  │ │  append, close │ │                                  │
│  list_by_user    │ │  update_meta   │ │  Operations:                     │
│  list_by_org     │ │  list_all      │ │  store_file, get_file            │
│  list_by_status  │ │                │ │  get_upload_url (presigned)      │
│  update_profile  │ │                │ │  get_download_url                │
│  scan_transfers  │ │                │ │  list_files                      │
└──────────────────┘ └────────────────┘ └──────────────────────────────────┘

          ┌──────────────────────────────────────────────┐
          │  Async Job Dispatch  (async_jobs.py)          │
          │                                              │
          │  Job types:                                  │
          │  ├ profile_submission  (scan data files)     │
          │  ├ mint_submission_doi (DataCite API)        │
          │  ├ mint_stream_doi    (DataCite API)         │
          │  ├ publish_submission (search index + DOI)   │
          │  ├ transfer_data      (Globus Transfer)      │
          │  └ cleanup_transfers  (ACL cleanup)          │
          │                                              │
          │  Dispatchers:                                │
          │  ├ InlineJobDispatcher   (dev: sync)         │
          │  ├ SQSJobDispatcher      (prod: async)       │
          │  └ SqliteJobDispatcher   (test: queued)      │
          └──────────────────────────────────────────────┘
```

### Data flow: dataset submission to publication

```
Researcher                    CLI/SDK                    Backend                    External
──────────                    ───────                    ───────                    ────────
    │                            │                          │                          │
    ├─ mdf publish ─────────────>│                          │                          │
    │                            ├─ upload files (HTTPS PUT)│─────────────────────────>│ Globus
    │                            │                          │                          │ Storage
    │                            ├─ POST /submit ──────────>│                          │
    │                            │                          ├─ validate metadata       │
    │                            │                          ├─ generate source_id      │
    │                            │                          ├─ version (1.0 or +0.1)   │
    │                            │                          ├─ store (pending_curation) │
    │                            │                          ├─ enqueue profile job ────>│ Async
    │                            │<── {source_id, version} ─┤                          │ Worker
    │                            │                          │                          │
    │                            │                          │       Profile job runs:   │
    │                            │                          │       scan files, build   │
    │                            │                          │       schema + stats      │
    │                            │                          │                          │
Curator                          │                          │                          │
──────                           │                          │                          │
    ├─ mdf approve ─────────────>│                          │                          │
    │                            ├─ POST /curation/{id}/approve ─>│                    │
    │                            │                          ├─ update status: approved  │
    │                            │                          ├─ enqueue publish job ────>│ Async
    │                            │<── {success, doi} ───────┤                          │ Worker
    │                            │                          │                          │
    │                            │                          │       Publish job runs:   │
    │                            │                          │       mint DOI (DataCite) │
    │                            │                          │       index (Globus Search)│
    │                            │                          │       status → published  │
```

## Prerequisites

```bash
# AWS SAM CLI
brew install aws-sam-cli   # macOS
# or: pip install aws-sam-cli

# AWS credentials
aws configure

# Verify
aws sts get-caller-identity
```

## Environments

| Environment | Stack | Auth | DataCite | Search | Curators |
|-------------|-------|------|----------|--------|----------|
| **dev** | `mdf-connect-v2-dev` | `X-User-Id` headers | Mock | Mock | All users |
| **staging** | `mdf-connect-v2-staging` | Globus tokens | Test API (creds from SSM) | Test index | All users |
| **prod** | `mdf-connect-v2-prod` | Globus tokens | Real API (`api.datacite.org`, prefix `10.18126`) | Real index | MDF curators group (`CuratorGroupIds`; `CuratorUserIds` for break-glass) |

DataCite credentials for staging and prod live only in SSM (`/mdf/{env}/datacite-*`); `deploy.sh` refuses to deploy without them while `UseMockDatacite=false`. All environments are fully separate CloudFormation stacks with their own DynamoDB tables, Lambda functions, API Gateway, and SQS queues.

## Deploying

### Dev (no external dependencies)

```bash
cd aws
sam build && ./deploy.sh dev
```

This creates a self-contained stack. No Globus credentials needed — auth uses `X-User-Id` headers, storage is local, DataCite is mocked.

### Staging

Requires Globus credentials stored in AWS SSM Parameter Store:

```bash
# One-time: store Globus credentials (already done for staging)
aws ssm put-parameter --name /mdf/globus-client-id \
  --value "YOUR_CLIENT_ID" --type String --region us-east-1
aws ssm put-parameter --name /mdf/globus-client-secret \
  --value "YOUR_CLIENT_SECRET" --type SecureString --region us-east-1
```

DataCite and Search credentials are in `samconfig.toml` for staging. Then:

```bash
cd aws
sam build && ./deploy.sh staging
```

### Production

Prod runs against **real** services: DataCite (`api.datacite.org`, prefix `10.18126`),
the production Globus Search index, and group-gated curation
(`AllowAllCurators=false`, `CuratorGroupIds`). Before the first deploy, store the real
DataCite repository credentials in SSM (namespaced by environment) — `deploy.sh` will
refuse to deploy without them:

```bash
aws ssm put-parameter --name /mdf/prod/datacite-username \
  --value "REPOSITORY_ID" --type String --region us-east-1
aws ssm put-parameter --name /mdf/prod/datacite-password \
  --value "REPOSITORY_PASSWORD" --type SecureString --region us-east-1

# Optional — otherwise the samconfig.toml [prod] values are used:
aws ssm put-parameter --name /mdf/prod/datacite-api-url \
  --value "https://api.datacite.org" --type String --region us-east-1
aws ssm put-parameter --name /mdf/prod/datacite-prefix \
  --value "10.18126" --type String --region us-east-1
```

Then deploy (Globus client id/secret must also be in SSM — see below):

```bash
cd aws
sam build && ./deploy.sh prod
```

#### Curator access

Curators are members of the Globus group in `CuratorGroupIds`
(default `3ce2c53e-3752-11e8-891c-0e00fd09bf20`). To grant a specific person curator
rights without group membership — e.g. a break-glass admin if the groups-token path
fails — add their Globus user id (`sub`) to `CuratorUserIds` in the `[prod]`
`parameter_overrides` (comma-separated).

### Custom domain (`api.materialsdatafacility.org`)

The CLI and SDK point at `https://api.materialsdatafacility.org` in prod (and
`https://api-staging.materialsdatafacility.org` for staging). The stack creates that
hostname only when both parameters below are set — otherwise nothing changes and the
`https://{apiId}.execute-api.us-east-1.amazonaws.com/{stage}` URL is the only entry
point. Adding the domain is **additive**: the execute-api URL keeps working afterwards.

| Parameter | Example | Notes |
|-----------|---------|-------|
| `ApiCustomDomainName` | `api.materialsdatafacility.org` | The public hostname |
| `ApiCustomDomainCertArn` | `arn:aws:acm:us-east-1:123456789012:certificate/…` | ACM cert for that hostname, **in this stack's region** |

**1. Request the ACM certificate.** HTTP API (API Gateway v2) custom domains are
`REGIONAL` only, so the certificate must live in the *same region as the stack*
(`us-east-1` here). The `us-east-1`-only rule people remember applies to
edge-optimized REST API domains and CloudFront, not to this one — for MDF both happen
to be `us-east-1`, so one cert in `us-east-1` satisfies either reading.

```bash
aws acm request-certificate \
  --domain-name api.materialsdatafacility.org \
  --validation-method DNS \
  --region us-east-1

# Print the CNAME that proves ownership, then create it in DNS:
aws acm describe-certificate --region us-east-1 \
  --certificate-arn arn:aws:acm:us-east-1:ACCOUNT:certificate/UUID \
  --query 'Certificate.DomainValidationOptions[].ResourceRecord'
```

Wait for `Status: ISSUED` — CloudFormation fails to create the domain with a
`PENDING_VALIDATION` certificate.

**2. Set the two parameters** in `aws/samconfig.toml` under `[prod.deploy.parameters]`
(commented placeholders are already there) and deploy:

```bash
cd aws
sam build && ./deploy.sh prod
```

**3. Create the DNS record.** The stack deliberately does **not** create Route53
records — the `materialsdatafacility.org` zone may not live in this AWS account. Read
the DNS target from the stack outputs (`./deploy.sh prod` prints it too):

```bash
aws cloudformation describe-stacks --stack-name mdf-connect-v2-prod \
  --region us-east-1 \
  --query 'Stacks[0].Outputs[?starts_with(OutputKey, `ApiCustomDomain`)]' --output table
```

| Output | Use |
|--------|-----|
| `ApiCustomDomainRegionalDomainName` | `d-xxxx.execute-api.us-east-1.amazonaws.com` — the CNAME / alias target |
| `ApiCustomDomainRegionalHostedZoneId` | Only needed for a Route53 alias (`AliasTarget.HostedZoneId`) |
| `ApiCustomDomainUrl` | `https://api.materialsdatafacility.org` |

- **External DNS provider:** create a `CNAME` from `api.materialsdatafacility.org` to
  the `ApiCustomDomainRegionalDomainName` value.
- **Route53 in this account:** create an `A`/`AAAA` alias record pointing at that same
  regional domain name with the regional hosted zone id.

**4. Verify** once DNS propagates:

```bash
curl https://api.materialsdatafacility.org/health
```

Paths on the custom domain drop the stage prefix: the mapping has no base path, so
`https://api.materialsdatafacility.org/health` corresponds to
`https://{apiId}.execute-api.us-east-1.amazonaws.com/prod/health`.

### Alarms and monitoring

Set `AlarmEmail` (in the environment's `parameter_overrides`) to create an SNS topic
plus CloudWatch alarms. Leave it blank and no monitoring resources are created.
**AWS emails a subscription confirmation link once — it must be clicked, or the topic
delivers nothing.** All alarms treat missing data as `notBreaching` (an idle stack is
not an incident) and notify on both ALARM and OK.

| Alarm | Condition |
|-------|-----------|
| `api-errors` | `ApiFunction` Lambda `Errors` > 0 for 2 consecutive 5-min periods |
| `async-worker-errors` | `AsyncWorkerFunction` `Errors` > 0 for 2 consecutive 5-min periods |
| `async-dlq-not-empty` | Any visible message in the async-jobs DLQ (jobs dropped after 3 attempts) |
| `async-queue-backlog` | Oldest queued async job older than 900 s (worker stalled or throttled) |
| `{submissions,streams}-{read,write}-throttles` | DynamoDB `ReadThrottleEvents` / `WriteThrottleEvents` > 0 |
| `{submissions,streams}-system-errors` | DynamoDB `SystemErrors` > 0 (service-side faults) |

```bash
# Alarm state at a glance
aws cloudwatch describe-alarms --region us-east-1 \
  --alarm-name-prefix mdf-connect-v2-prod \
  --query 'MetricAlarms[].[AlarmName,StateValue]' --output table
```

### Quick deploy (code only, skips CloudFormation)

For Lambda code changes that don't touch infrastructure:

```bash
cd aws
./deploy.sh quick staging   # or: quick prod
```

### Local development

```bash
cd aws
./deploy.sh local
# Server starts at http://127.0.0.1:8080
# Uses SQLite, local storage, mock DataCite, dev auth
```

## After deploying

```bash
# Get the API URL
./deploy.sh status staging

# Tail Lambda logs
./deploy.sh logs staging

# Health check
curl https://YOUR_API_URL/health

# Full teardown (removes stack, keeps DynamoDB tables)
./deploy.sh teardown dev
```

## SSM Parameters

DataCite parameters are namespaced by environment (`/mdf/{env}/...`); the Globus app
credentials are shared across environments.

| Parameter | Required for | Description |
|-----------|-------------|-------------|
| `/mdf/globus-client-id` | staging, prod | Globus confidential app client ID |
| `/mdf/globus-client-secret` | staging, prod | Globus confidential app client secret |
| `/mdf/{env}/datacite-username` | staging, prod (**required** when `UseMockDatacite=false`) | DataCite repository ID |
| `/mdf/{env}/datacite-password` | staging, prod (**required** when `UseMockDatacite=false`) | DataCite repository password |
| `/mdf/{env}/datacite-api-url` | optional | Overrides samconfig (`https://api.datacite.org` for real DOIs) |
| `/mdf/{env}/datacite-prefix` | optional | DOI prefix (e.g., `10.18126`) |

## Running tests

Backend tests run in CI (GitHub Actions) on every pull request to `master` or `mdf-agent`. No AWS credentials are needed — all tests use SQLite, local storage, and mock services.

```bash
cd aws

# All v2 tests
python -m pytest v2/test_v2_*.py -v

# Individual suites
python -m pytest v2/test_v2_publish_pipeline.py -v   # Full publish pipeline
python -m pytest v2/test_v2_hardening.py -v           # Security hardening
python -m pytest v2/test_v2_integration.py -v         # Integration tests
python -m pytest v2/test_v2_versioning.py -v          # Dataset versioning
python -m pytest v2/test_v2_async_jobs.py -v          # Async job dispatch
```

## Environment variables

For local development, set these in your shell or a `.env` file (requires `pip install python-dotenv`). The `.env` file is loaded automatically when running `python -m v2.app.main`.

| Variable | Default | Description |
|----------|---------|-------------|
| **Core** | | |
| `STORE_BACKEND` | `dynamo` | `sqlite` (dev) or `dynamo` (prod) |
| `SQLITE_PATH` | `/tmp/mdf_connect_v2.db` | Database path when using SQLite |
| `AUTH_MODE` | `dev` | `dev` (X-User-Id headers) or `production` (Globus tokens) |
| `ASYNC_DISPATCH_MODE` | `inline` | `inline` (sync), `sqs` (prod), or `sqlite` (test) |
| `LOG_LEVEL` | `INFO` | Logging level |
| **Storage** | | |
| `STORAGE_BACKEND` | `local` | `local`, `s3`, or `globus` |
| `FILE_STORE_PATH` | `/tmp/mdf_files` | Filesystem path when `STORAGE_BACKEND=local` |
| `S3_BUCKET` | — | S3 bucket when `STORAGE_BACKEND=s3` |
| `GLOBUS_ENDPOINT_ID` | NCSA UUID | Globus endpoint when `STORAGE_BACKEND=globus` |
| `GLOBUS_HTTPS_SERVER` | `data.materialsdatafacility.org` | HTTPS hostname for Globus storage |
| **Auth** | | |
| `LOCAL_USER_ID` | `local-user` | Dev-mode user identity |
| `LOCAL_USER_EMAIL` | `local@example.com` | Dev-mode user email |
| `GLOBUS_CLIENT_ID` | — | Globus confidential app client ID (prod) |
| `GLOBUS_CLIENT_SECRET` | — | Globus confidential app client secret (prod) |
| `ALLOW_ALL_CURATORS` | `false` | `true` lets all users curate (dev only) |
| `CURATOR_USER_IDS` | — | Comma-separated Globus user IDs |
| `CURATOR_GROUP_IDS` | — | Comma-separated Globus group UUIDs |
| **DataCite** | | |
| `USE_MOCK_DATACITE` | `false` | `true` for mock DOI minting (dev/test) |
| `DATACITE_USERNAME` | — | DataCite repository ID |
| `DATACITE_PASSWORD` | — | DataCite repository password |
| `DATACITE_PREFIX` | `10.23677` | DOI prefix |
| `DATACITE_TEST_MODE` | `true` | Use DataCite test API |
| **Search** | | |
| `USE_MOCK_SEARCH` | `false` | `true` for mock search (dev/test) |
| `SEARCH_INDEX_UUID` | — | Production Globus Search index |
| `TEST_SEARCH_INDEX_UUID` | — | Test Globus Search index |
| **Limits** | | |
| `MAX_SUBMIT_METADATA_BYTES` | `262144` | Max metadata payload size |
| `MAX_SUBMIT_DATA_SOURCES` | `2000` | Max data sources per submission |
| `MAX_SUBMIT_AUTHORS` | `1000` | Max authors per submission |
| `MAX_STREAM_APPEND_COUNT` | `10000` | Max records per stream append |
| `CORS_ALLOWED_ORIGINS` | `*` | CORS allowed origins |

## Key configuration files

| File | Purpose |
|------|---------|
| `aws/template.yaml` | SAM/CloudFormation template — Lambda, API Gateway, DynamoDB, SQS, S3 |
| `aws/samconfig.toml` | Per-environment deploy config (dev, staging, prod) |
| `aws/deploy.sh` | Deploy script — `dev`, `staging`, `prod`, `quick`, `local`, `teardown`, `logs`, `status` |
| `aws/requirements.txt` | Python dependencies bundled into Lambda |

## v1 (legacy)

The v1 system (`aws/submit.py`, `aws/status.py`, `aws/automate_manager.py`, `infra/`) uses per-endpoint Lambda functions deployed via Terraform and GitHub Actions, orchestrated by Globus Automate Flows. It remains operational on the `prod` branch. The v2 backend runs on completely separate infrastructure (different stack name, tables, API Gateway) and can be deployed in parallel.

## Support

This work was performed under financial assistance award 70NANB14H012 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the [Center for Hierarchical Material Design (CHiMaD)](http://chimad.northwestern.edu). This work was performed under the following financial assistance award 70NANB19H005 from U.S. Department of Commerce, National Institute of Standards and Technology as part of the Center for Hierarchical Materials Design (CHiMaD). This work was also supported by the National Science Foundation as part of the [Midwest Big Data Hub](http://midwestbigdatahub.org) under NSF Award Number: 1636950 "BD Spokes: SPOKE: MIDWEST: Collaborative: Integrative Materials Design (IMaD): Leverage, Innovate, and Disseminate".
