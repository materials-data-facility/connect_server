# MDF Connect v2 - Deployment Guide

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         API Gateway                                  │
│                    api.materialsdatafacility.org                    │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
   │   Submit    │    │   Stream    │    │   Search    │
   │   Card      │    │   Upload    │    │   Citation  │
   │   Status    │    │   Close     │    │             │
   └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
          │                  │                   │
          ▼                  ▼                   ▼
   ┌─────────────────────────────────────────────────────┐
   │                     DynamoDB                         │
   │     submissions          │         streams           │
   └─────────────────────────────────────────────────────┘
                              │
                              ▼
   ┌─────────────────────────────────────────────────────┐
   │                        S3                            │
   │              Stream file storage                     │
   └─────────────────────────────────────────────────────┘
```

## Prerequisites

```bash
# Install AWS SAM CLI
brew install aws-sam-cli  # macOS
# or: pip install aws-sam-cli

# Configure AWS credentials
aws configure
```

## Quick Start

```bash
# Local development
make local              # Start server on http://127.0.0.1:8080
make demo               # Run demo script

# Deploy to AWS
make deploy-dev         # Deploy to dev (fast, no confirmation)
make deploy-prod        # Deploy to production (requires confirmation)
```

## Deployment Environments

| Environment | Stack Name | Usage |
|-------------|------------|-------|
| `dev` | mdf-connect-v2-dev | Development, testing |
| `staging` | mdf-connect-v2-staging | Pre-production validation |
| `prod` | mdf-connect-v2-prod | Production |

## First-Time Setup

1. **Create S3 buckets for SAM deployments:**
   ```bash
   aws s3 mb s3://mdf-sam-deployments-dev --region us-east-1
   aws s3 mb s3://mdf-sam-deployments-prod --region us-east-1
   ```

2. **Store Globus credentials in SSM Parameter Store:**
   ```bash
   aws ssm put-parameter \
     --name /mdf/globus-client-id \
     --value "YOUR_GLOBUS_CLIENT_ID" \
     --type SecureString
   ```

3. **Initial deployment:**
   ```bash
   sam build
   sam deploy --guided  # Interactive setup
   ```

## Making Changes

The beauty of serverless: **code changes are just deploys**.

```bash
# 1. Edit code locally
vim v2/submit.py

# 2. Test locally
make local
curl http://localhost:8080/submit ...

# 3. Deploy
make deploy-dev

# 4. Verify in AWS
make logs-dev
```

## Cost Estimation

For ~10 datasets/month + streaming:

| Service | Estimated Cost |
|---------|---------------|
| Lambda | ~$0 (free tier: 1M requests) |
| API Gateway | ~$0 (free tier: 1M calls) |
| DynamoDB | ~$0 (free tier: 25GB, 25 WCU/RCU) |
| S3 | ~$0.02/GB stored |
| **Total** | **< $5/month** |

## Monitoring

```bash
# Tail logs
make logs-dev

# Check stack status
make status-dev

# CloudWatch metrics
FUNC_NAME=$(aws cloudformation describe-stack-resources \
  --stack-name mdf-connect-v2-dev \
  --query 'StackResources[?ResourceType==`AWS::Lambda::Function`].PhysicalResourceId' \
  --output text)
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value="$FUNC_NAME" \
  --start-time $(date -v-1d +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date +%Y-%m-%dT%H:%M:%S) \
  --period 3600 \
  --statistics Sum
```

## Rollback

```bash
# Automatic rollback on failure (default)
# Manual rollback to previous version:
aws cloudformation rollback-stack --stack-name mdf-connect-v2-dev
```

## Adding New Endpoints

1. **Add router endpoint** in `v2/app/routers/*.py`:
   ```python
   from fastapi import APIRouter

   router = APIRouter()

   @router.get("/my-endpoint")
   async def my_endpoint():
       return {"success": True}
   ```

2. **Register the router** in `v2/app/__init__.py`:
   ```python
   from v2.app.routers import my_router
   app.include_router(my_router.router)
   ```

3. **Deploy**:
   ```bash
   make deploy-dev
   ```

## Troubleshooting

### Lambda timeout
Increase in `template.yaml`:
```yaml
Timeout: 60  # seconds
```

### DynamoDB throttling
Already using PAY_PER_REQUEST (auto-scaling). If issues persist, check CloudWatch metrics.

### API Gateway 502
Check Lambda logs:
```bash
sam logs --stack-name mdf-connect-v2-dev --tail
```

## Security Notes

- All endpoints except `/card`, `/citation`, `/search`, `/status` require Globus Auth
- DynamoDB tables have `DeletionPolicy: Retain` (won't delete on stack removal)
- S3 bucket is encrypted at rest and blocks public access
- Secrets stored in SSM Parameter Store (encrypted)
