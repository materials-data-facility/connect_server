#!/bin/bash
# Create DataCite SSM parameters for a specific environment.
#
# The deploy script (deploy.sh) reads these automatically during
# staging/prod deployments and passes them as CloudFormation parameter overrides.
# Parameters are namespaced by environment: /mdf/{env}/datacite-*
#
# For staging: not needed — test credentials are in samconfig.toml.
# For prod:    run this script once with real DataCite production credentials.
#
# Usage:
#   ./setup_datacite_ssm.sh prod       # Set up production credentials
#   ./setup_datacite_ssm.sh staging    # Set up staging credentials (optional)
#
# You will be prompted for each value. The password is stored as SecureString.

set -e

REGION="us-east-1"
ENV="${1:-prod}"

if [[ "$ENV" != "prod" && "$ENV" != "staging" ]]; then
    echo "Usage: $0 [prod|staging]"
    exit 1
fi

echo "=== MDF DataCite SSM Parameter Setup ($ENV) ==="
echo ""
echo "Parameters will be stored under /mdf/$ENV/datacite-*"
echo "Region: $REGION"
echo ""

read -p "DataCite repository ID (e.g., MDF.MDF): " DC_USER
read -s -p "DataCite password: " DC_PASS
echo ""
read -p "DataCite API URL [https://api.datacite.org]: " DC_URL
DC_URL=${DC_URL:-https://api.datacite.org}
read -p "DataCite DOI prefix (e.g., 10.18126): " DC_PREFIX

echo ""
echo "Creating SSM parameters under /mdf/$ENV/..."

aws ssm put-parameter \
    --name "/mdf/$ENV/datacite-username" \
    --type "String" \
    --value "$DC_USER" \
    --region "$REGION" \
    --overwrite

aws ssm put-parameter \
    --name "/mdf/$ENV/datacite-password" \
    --type "SecureString" \
    --value "$DC_PASS" \
    --region "$REGION" \
    --overwrite

aws ssm put-parameter \
    --name "/mdf/$ENV/datacite-api-url" \
    --type "String" \
    --value "$DC_URL" \
    --region "$REGION" \
    --overwrite

aws ssm put-parameter \
    --name "/mdf/$ENV/datacite-prefix" \
    --type "String" \
    --value "$DC_PREFIX" \
    --region "$REGION" \
    --overwrite

echo ""
echo "Done. Verify with:"
echo "  aws ssm get-parameter --name /mdf/$ENV/datacite-username --region $REGION --query Parameter.Value --output text"
echo "  aws ssm get-parameter --name /mdf/$ENV/datacite-prefix --region $REGION --query Parameter.Value --output text"
echo ""
echo "deploy.sh will automatically pick these up on next $ENV deployment."
