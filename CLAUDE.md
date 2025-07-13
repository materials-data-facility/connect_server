# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

MDF Connect is a serverless ETL service for the Materials Data Facility that processes dataset submissions through a complex workflow involving AWS Lambda functions and Globus Automate flows. The service validates, transfers, curates, and indexes datasets into MDF Search.

## Key Commands

### Testing
```bash
# Run tests (from aws/tests directory)
cd aws/tests
pip install -r requirements-test.txt
PYTHONPATH=.. python -m pytest --ignore schemas

# Run tests with specific coverage (from root)
pip install -r aws/tests/requirements-test.txt
PYTHONPATH=aws/ python -m pytest aws/tests
```

### Development Setup
```bash
# Install main dependencies
cd aws
pip install -r requirements.txt

# Install test dependencies
pip install -r tests/requirements-test.txt

# Security check
pip install safety==2.3.5
pip freeze | safety check
```

### Deployment Commands
```bash
# Deploy Globus Automate flow from command line
cd automate
export $(cat ../secrets.env) PYTHONPATH=../aws && python deploy_mdf_flow.py dev 1.0.0-rc.10

# Create new flow
python create_new_flow.py
```

## Architecture

### Core Components

**AWS Lambda Functions** (`aws/` directory):
- `submit.py`: Handles dataset submission requests, validates against JSON schema
- `status.py`: Returns submission status from DynamoDB
- `submissions.py`: Queries and returns submission lists
- `auth.py`: Handles Globus authentication

**Globus Automate Flow** (`automate/minimus_mdf_flow.py`):
1. Email admin notification of submission
2. Check for data file updates vs metadata-only
3. Initiate Globus transfer if data files present
4. Optional curation step (organization-dependent)
5. DOI minting (organization-dependent)
6. Dataset indexing in MDF Search
7. User completion notification

**Data Management**:
- `dynamo_manager.py`: DynamoDB operations for submission tracking
- `organization.py`: Organization configuration and validation
- `source_id_manager.py`: Manages dataset source identifiers

### Key Architectural Patterns

- **Schema Validation**: All submissions validated against JSON schemas from separate data-schemas repo (automate branch)
- **Organization-based Configuration**: Different orgs have different curation/DOI requirements
- **Globus Integration**: Heavy use of Globus Auth, Transfer, and Automate services
- **Environment Separation**: Dev/prod environments with separate flows and Lambda functions

## Development Workflow

1. Create feature branch from `dev`
2. Test changes locally using pytest commands above
3. Create PR to `dev` branch
4. Auto-deployment to dev environment via GitHub Actions
5. Test in dev environment
6. Create PR from `dev` to `prod` (main branch)
7. Auto-deployment to prod environment

## Deployment Details

### GitHub Actions
- **CI Pipeline**: `.github/workflows/deploy-lambda.yml` handles testing and deployment
- **Flow Deployment**: `.github/workflows/deploy_flow.yml` deploys Globus Automate flows
- **Triggers**: Push to `dev` or `prod` branches

### Infrastructure
- **Terraform**: Infrastructure defined in `infra/` directory
- **Docker**: Lambda functions containerized and pushed to ECR
- **Multi-environment**: Separate dev/prod deployments with different flow IDs

### Critical Files
- Flow configurations: `automate/mdf_*_flow_info.json` files contain flow IDs
- Schemas: Pulled from external data-schemas repository during build
- Organization data: Managed in data-schemas repo automate branch

## New Self-Service Metadata Update System

### New API Endpoints

**Frontend-Friendly REST API for metadata updates:**

```bash
# List user's datasets with pagination
GET /datasets?limit=20&cursor={token}&status=active

# Get dataset metadata with edit permissions
GET /datasets/{source_id}/metadata?version={optional}

# Update specific metadata fields (authors, title, description, tags only)
PATCH /datasets/{source_id}/metadata

# Get version history with optional diffs
GET /datasets/{source_id}/versions?include_diff=true&limit=50
```

### Performance Improvements

**DynamoDB Optimizations:**
- Added Global Secondary Indexes for efficient queries:
  - `user-updated-index`: Query datasets by user_id + updated_at
  - `organization-source-index`: Query by organization + source_id  
  - `status-updated-index`: Query by status + updated_at
- Replaced table scans with indexed queries (10-100x performance improvement)
- Efficient pagination using DynamoDB's native pagination

### Field-Level Validation

**User-Updatable Fields:**
- `dc.titles`: Dataset titles
- `dc.creators`: Authors/creators
- `dc.descriptions`: Descriptions  
- `dc.subjects`: Tags/subjects

**Restricted Fields (admin-only):**
- DOI fields (`datacite.doi`)
- Data location fields (`services`)
- System metadata (`mdf.source_id`, version info)

### Key Components

- **`metadata_validator.py`**: Field-level validation and permissions
- **`list_datasets.py`**: Efficient user dataset listing with GSI queries
- **`get_metadata.py`**: Retrieve dataset metadata with permission checks
- **`update_metadata.py`**: Process metadata updates with validation
- **`get_versions.py`**: Version history with optional change diffs

### Workflow Integration

- Metadata updates trigger `update_metadata_only` workflow
- Automatic versioning with change tracking
- API-triggered updates bypass file transfers
- Enhanced email notifications distinguish API vs manual updates

### Testing Notes

- Tests use pytest-bdd for behavior-driven testing
- Mock AWS services with boto3 mocking
- Schema validation tests require schemas from external repo
- Integration tests in `tests/deploy_suite_files/`
- New metadata update tests in `tests/test_metadata_updates.py`