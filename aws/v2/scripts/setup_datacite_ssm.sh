#!/bin/bash
# Create DataCite SSM parameters for production.
#
# The deploy script (deploy.sh) already reads these automatically during
# staging/prod deployments and passes them as CloudFormation parameter overrides.
#
# For staging: not needed — test credentials are in samconfig.toml.
# For prod:    run this script once with real DataCite production credentials.
#
# Usage:
#   ./setup_datacite_ssm.sh
#
# You will be prompted for each value. The password is stored as SecureString.

set -e

REGION="us-east-1"

echo "=== MDF DataCite SSM Parameter Setup ==="
echo ""
echo "These parameters are read by deploy.sh during staging/prod deployments."
echo "Region: $REGION"
echo ""

read -p "DataCite repository ID (e.g., MDF.MDF): " DC_USER
read -s -p "DataCite password: " DC_PASS
echo ""
read -p "DataCite API URL [https://api.datacite.org]: " DC_URL
DC_URL=${DC_URL:-https://api.datacite.org}
read -p "DataCite DOI prefix (e.g., 10.18126): " DC_PREFIX

echo ""
echo "Creating SSM parameters..."

aws ssm put-parameter \
    --name "/mdf/datacite-username" \
    --type "String" \
    --value "$DC_USER" \
    --region "$REGION" \
    --overwrite

aws ssm put-parameter \
    --name "/mdf/datacite-password" \
    --type "SecureString" \
    --value "$DC_PASS" \
    --region "$REGION" \
    --overwrite

aws ssm put-parameter \
    --name "/mdf/datacite-api-url" \
    --type "String" \
    --value "$DC_URL" \
    --region "$REGION" \
    --overwrite

aws ssm put-parameter \
    --name "/mdf/datacite-prefix" \
    --type "String" \
    --value "$DC_PREFIX" \
    --region "$REGION" \
    --overwrite

echo ""
echo "Done. Verify with:"
echo "  aws ssm get-parameter --name /mdf/datacite-username --region $REGION --query Parameter.Value --output text"
echo "  aws ssm get-parameter --name /mdf/datacite-prefix --region $REGION --query Parameter.Value --output text"
echo ""
echo "deploy.sh will automatically pick these up on next staging/prod deployment."
