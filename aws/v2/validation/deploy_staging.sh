#!/bin/bash
# Non-interactive staging deploy. Resolves Globus creds from SSM at runtime
# (never hard-coded here) and masks them out of all output.
set -uo pipefail
cd /Users/blaiszik/Desktop/git/mdf_client/cs/aws
export AWS_PROFILE=mdf-deploy AWS_DEFAULT_REGION=us-east-1
AWS=/opt/homebrew/bin/aws
SAM=/opt/homebrew/bin/sam

GID=$("$AWS" ssm get-parameter --name /mdf/globus-client-id --query Parameter.Value --output text)
GSEC=$("$AWS" ssm get-parameter --name /mdf/globus-client-secret --with-decryption --query Parameter.Value --output text)

ALL="Environment=staging AuthMode=production UseMockDatacite=true \
SearchIndexUUID=ab19b80b-0887-4337-b9f8-b8cc7feb1fdc \
TestSearchIndexUUID=ab19b80b-0887-4337-b9f8-b8cc7feb1fdc \
GlobusClientId=${GID} GlobusClientSecret=${GSEC}"

"$SAM" deploy --config-env staging \
  --s3-bucket mdf-sam-deployments-staging-557062710055 \
  --no-confirm-changeset --no-fail-on-empty-changeset \
  --parameter-overrides "$ALL" 2>&1 \
  | sed "s|${GSEC}|***SECRET-REDACTED***|g; s|${GID}|***CLIENT-ID***|g"
rc=${PIPESTATUS[0]}
echo "STAGING_DEPLOY_EXIT=${rc}"
exit "$rc"
