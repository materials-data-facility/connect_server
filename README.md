# MDF Connect

The Materials Data Facility Connect service is the backend for submitting, curating, and publishing datasets to MDF Search. For the Python client, see [connect_client](https://github.com/materials-data-facility/connect_client).

## v2 Backend

The v2 backend lives in `aws/v2/` and is a complete rewrite: a single FastAPI application deployed to AWS Lambda via Mangum + SAM. It replaces the old per-endpoint Lambda functions and Terraform deployment.

**Stack**: Python 3.12, FastAPI, Pydantic v2, DynamoDB, Globus HTTPS storage, DataCite DOI minting, Globus Search, AWS SAM.

### What it does

- **Dataset submission**: submit → pending_curation → approved (DOI minted) → published (indexed to Globus Search)
- **Streaming**: create stream → upload files to Globus HTTPS → snapshot to dataset → close with DOI
- **Curation**: pending list, approve/reject, curator guards
- **Discovery**: search, dataset cards, citations (BibTeX/APA/RIS), file preview
- **Auth**: Globus token validation (prod) or `X-User-Id` headers (dev)

### Architecture

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
| **staging** | `mdf-connect-v2-staging` | Globus tokens | Test API (`Globus.TEST`) | Test index | All users |
| **prod** | `mdf-connect-v2-prod` | Globus tokens | Test API (switch to real later) | Test index (switch to real later) | All users (switch to group-based later) |

All environments are fully separate CloudFormation stacks with their own DynamoDB tables, Lambda functions, API Gateway, and SQS queues.

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

Same SSM prerequisites as staging. The prod config in `samconfig.toml` currently uses **test credentials** (DataCite test API, test search index) so the stack can be deployed and validated before switching to real credentials.

```bash
cd aws
sam build && ./deploy.sh prod
```

#### Switching prod to real credentials

When ready to go live, update `samconfig.toml` `[prod]` section:

```toml
[prod.deploy.parameters]
parameter_overrides = "Environment=prod AuthMode=production AllowAllCurators=false DataCiteUsername=REAL_USERNAME DataCitePassword=REAL_PASSWORD DataCiteApiUrl=https://api.datacite.org DataCitePrefix=10.18126 UseMockDatacite=false SearchIndexUUID=REAL_INDEX_UUID TestSearchIndexUUID=TEST_INDEX_UUID"
```

Or store DataCite credentials in SSM (deploy.sh will pick them up automatically):

```bash
aws ssm put-parameter --name /mdf/datacite-username \
  --value "REAL_USERNAME" --type String --region us-east-1
aws ssm put-parameter --name /mdf/datacite-password \
  --value "REAL_PASSWORD" --type SecureString --region us-east-1
aws ssm put-parameter --name /mdf/datacite-api-url \
  --value "https://api.datacite.org" --type String --region us-east-1
aws ssm put-parameter --name /mdf/datacite-prefix \
  --value "10.18126" --type String --region us-east-1
```

Then redeploy: `sam build && ./deploy.sh prod`

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

| Parameter | Required for | Description |
|-----------|-------------|-------------|
| `/mdf/globus-client-id` | staging, prod | Globus confidential app client ID |
| `/mdf/globus-client-secret` | staging, prod | Globus confidential app client secret |
| `/mdf/datacite-username` | prod (optional) | DataCite repository ID — overrides samconfig |
| `/mdf/datacite-password` | prod (optional) | DataCite repository password |
| `/mdf/datacite-api-url` | prod (optional) | `https://api.datacite.org` for real DOIs |
| `/mdf/datacite-prefix` | prod (optional) | DOI prefix (e.g., `10.18126`) |

## Running tests

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
