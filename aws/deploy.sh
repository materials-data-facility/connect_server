#!/bin/bash
# MDF v2 Backend Deployment Script
#
# Usage:
#   ./deploy.sh dev          # Deploy to dev (self-contained, no external deps)
#   ./deploy.sh prod         # Deploy to production (requires Globus SSM params)
#   ./deploy.sh local        # Run local server
#   ./deploy.sh teardown dev # Completely remove a dev deployment
#
# What "dev" creates (scoped entirely to this stack):
#   - CloudFormation stack:  mdf-connect-v2-dev
#   - S3 bucket:             mdf-sam-deployments-dev  (deployment artifacts)
#   - DynamoDB tables:       mdf-submissions-dev, mdf-streams-dev
#   - Lambda function:       ApiFunction (inside the stack)
#   - HTTP API Gateway:      (inside the stack)
#
# First-time setup:
#   pip install aws-sam-cli
#   aws configure  # Set up AWS credentials

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

REGION="us-east-1"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()  { echo -e "${GREEN}[MDF]${NC} $1"; }
warn() { echo -e "${YELLOW}[MDF]${NC} $1"; }
info() { echo -e "${CYAN}[MDF]${NC} $1"; }
error(){ echo -e "${RED}[MDF]${NC} $1"; exit 1; }

# ---------------------------------------------------------------------------
# Dependency checks
# ---------------------------------------------------------------------------
check_deps() {
    command -v sam &>/dev/null || error "AWS SAM CLI not found. Install: pip install aws-sam-cli"
    command -v aws &>/dev/null || error "AWS CLI not found. Install: pip install awscli"
}

check_aws_identity() {
    log "Checking AWS credentials..."
    local identity
    identity=$(aws sts get-caller-identity --output json 2>/dev/null) \
        || error "AWS credentials not configured. Run: aws configure"
    local account=$(echo "$identity" | python3 -c "import sys,json; print(json.load(sys.stdin)['Account'])")
    local arn=$(echo "$identity" | python3 -c "import sys,json; print(json.load(sys.stdin)['Arn'])")
    info "Account: $account"
    info "Identity: $arn"
}

# ---------------------------------------------------------------------------
# S3 bucket for SAM deployment artifacts
# ---------------------------------------------------------------------------
ensure_s3_bucket() {
    local bucket=$1
    if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        log "S3 bucket $bucket exists"
    else
        log "Creating S3 bucket $bucket..."
        if [[ "$REGION" == "us-east-1" ]]; then
            aws s3api create-bucket --bucket "$bucket" --region "$REGION"
        else
            aws s3api create-bucket --bucket "$bucket" --region "$REGION" \
                --create-bucket-configuration LocationConstraint="$REGION"
        fi
        # Block public access
        aws s3api put-public-access-block --bucket "$bucket" \
            --public-access-block-configuration \
            "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
        log "Created S3 bucket $bucket (public access blocked)"
    fi
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
build() {
    log "Building SAM application..."
    sam build
}

# ---------------------------------------------------------------------------
# Deploy to dev (fully self-contained)
# ---------------------------------------------------------------------------
deploy_dev() {
    local stack_name="mdf-connect-v2-dev"
    local s3_bucket="mdf-sam-deployments-dev"

    echo ""
    info "═══════════════════════════════════════════════════"
    info "  MDF Connect v2 — Dev Deployment"
    info "═══════════════════════════════════════════════════"
    info "  Stack:    $stack_name"
    info "  Region:   $REGION"
    info "  Auth:     dev (X-User-Id headers, no Globus)"
    info "  Storage:  local (no Globus transfers)"
    info "  DataCite: mock (no real DOIs)"
    info "  Tables:   mdf-submissions-dev, mdf-streams-dev"
    info "═══════════════════════════════════════════════════"
    echo ""

    check_aws_identity
    ensure_s3_bucket "$s3_bucket"
    build

    log "Deploying stack $stack_name..."
    sam deploy --config-env dev --no-fail-on-empty-changeset

    echo ""
    log "Deployment complete!"
    echo ""

    # Print the API URL
    local api_url
    api_url=$(aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' \
        --output text 2>/dev/null || echo "")

    if [[ -n "$api_url" && "$api_url" != "None" ]]; then
        info "API URL: $api_url"
        echo ""
        echo "  Test it:"
        echo "    curl $api_url/health"
        echo ""
        echo "  Submit a dataset:"
        echo "    curl -X POST $api_url/submit \\"
        echo "      -H 'Content-Type: application/json' \\"
        echo "      -H 'X-User-Id: test-user' \\"
        echo "      -d '{\"title\": \"Test Dataset\", \"authors\": [{\"name\": \"Test\"}], \"data_sources\": [\"https://example.com/data.csv\"]}'"
        echo ""
        echo "  Tear down when done:"
        echo "    ./deploy.sh teardown dev"
    fi
}

# ---------------------------------------------------------------------------
# Deploy to staging/prod (requires Globus credentials)
# ---------------------------------------------------------------------------
deploy_prod() {
    local env=$1
    local stack_name="mdf-connect-v2-$env"

    echo ""
    info "═══════════════════════════════════════════════════"
    local env_label
    env_label=$(echo "$env" | awk '{print toupper(substr($0,1,1)) substr($0,2)}')
    info "  MDF Connect v2 — $env_label Deployment"
    info "═══════════════════════════════════════════════════"
    echo ""

    check_aws_identity

    # Resolve Globus credentials from SSM
    log "Resolving Globus credentials from SSM..."
    local globus_id globus_secret
    globus_id=$(aws ssm get-parameter \
        --name "/mdf/globus-client-id" \
        --region "$REGION" \
        --query 'Parameter.Value' --output text 2>/dev/null) \
        || error "SSM parameter /mdf/globus-client-id not found. Create it first."
    globus_secret=$(aws ssm get-parameter \
        --name "/mdf/globus-client-secret" \
        --region "$REGION" \
        --with-decryption \
        --query 'Parameter.Value' --output text 2>/dev/null) \
        || error "SSM parameter /mdf/globus-client-secret not found. Create it first."
    log "Globus credentials resolved from SSM"

    # Resolve DataCite credentials from SSM (optional — falls back to env/defaults)
    # Parameters are namespaced by environment: /mdf/{env}/datacite-*
    log "Resolving DataCite credentials from SSM (/mdf/$env/datacite-*)..."
    local datacite_user datacite_pass datacite_url datacite_prefix
    datacite_user=$(aws ssm get-parameter \
        --name "/mdf/$env/datacite-username" \
        --region "$REGION" \
        --query 'Parameter.Value' --output text 2>/dev/null || echo "")
    datacite_pass=$(aws ssm get-parameter \
        --name "/mdf/$env/datacite-password" \
        --region "$REGION" \
        --with-decryption \
        --query 'Parameter.Value' --output text 2>/dev/null || echo "")
    datacite_url=$(aws ssm get-parameter \
        --name "/mdf/$env/datacite-api-url" \
        --region "$REGION" \
        --query 'Parameter.Value' --output text 2>/dev/null || echo "")
    datacite_prefix=$(aws ssm get-parameter \
        --name "/mdf/$env/datacite-prefix" \
        --region "$REGION" \
        --query 'Parameter.Value' --output text 2>/dev/null || echo "")
    if [[ -n "$datacite_user" ]]; then
        log "DataCite credentials resolved from SSM"
    else
        warn "DataCite SSM parameters not found — using defaults from samconfig.toml"
    fi

    ensure_s3_bucket "mdf-sam-deployments-$env"
    build

    # Read base parameter_overrides from samconfig.toml and append credentials.
    # CLI --parameter-overrides fully replaces samconfig values, so we must
    # pass the complete set here.
    local base_params
    base_params=$(python3 -c "
try:
    import tomllib
except ImportError:
    import tomli as tomllib
import sys
with open('samconfig.toml', 'rb') as f:
    cfg = tomllib.load(f)
print(cfg.get('${env}', {}).get('deploy', {}).get('parameters', {}).get('parameter_overrides', ''))
" 2>/dev/null || echo "Environment=$env AuthMode=production")

    local all_params="$base_params GlobusClientId=$globus_id GlobusClientSecret=$globus_secret"

    # Append DataCite params if resolved from SSM
    [[ -n "$datacite_user" ]] && all_params="$all_params DataCiteUsername=$datacite_user"
    [[ -n "$datacite_pass" ]] && all_params="$all_params DataCitePassword=$datacite_pass"
    [[ -n "$datacite_url" ]] && all_params="$all_params DataCiteApiUrl=$datacite_url"
    [[ -n "$datacite_prefix" ]] && all_params="$all_params DataCitePrefix=$datacite_prefix"

    log "Deploying stack $stack_name..."
    sam deploy \
        --config-env "$env" \
        --no-fail-on-empty-changeset \
        --parameter-overrides "$all_params"

    log "Deployment complete!"
}

# ---------------------------------------------------------------------------
# Teardown — completely remove a deployment
# ---------------------------------------------------------------------------
teardown() {
    local env=$1
    [[ -z "$env" ]] && error "Environment required: ./deploy.sh teardown dev"

    local stack_name="mdf-connect-v2-$env"
    local s3_bucket="mdf-sam-deployments-$env"

    echo ""
    warn "═══════════════════════════════════════════════════"
    warn "  TEARDOWN: $stack_name"
    warn "═══════════════════════════════════════════════════"
    warn "  This will delete:"
    warn "    - CloudFormation stack: $stack_name"
    warn "    - Lambda function and API Gateway"
    warn "    - S3 bucket: $s3_bucket"
    warn ""
    warn "  DynamoDB tables have DeletionPolicy=Retain and"
    warn "  will NOT be auto-deleted. Delete manually if needed:"
    warn "    aws dynamodb delete-table --table-name mdf-submissions-$env"
    warn "    aws dynamodb delete-table --table-name mdf-streams-$env"
    warn "═══════════════════════════════════════════════════"
    echo ""

    read -p "Type '$env' to confirm teardown: " confirm
    [[ "$confirm" != "$env" ]] && error "Aborted."

    log "Deleting CloudFormation stack $stack_name..."
    sam delete \
        --stack-name "$stack_name" \
        --region "$REGION" \
        --no-prompts \
        2>/dev/null || warn "Stack deletion may have partial failures (retained resources)"

    log "Emptying S3 bucket $s3_bucket..."
    aws s3 rm "s3://$s3_bucket" --recursive 2>/dev/null || true
    log "Deleting S3 bucket $s3_bucket..."
    aws s3api delete-bucket --bucket "$s3_bucket" --region "$REGION" 2>/dev/null || true

    echo ""
    log "Teardown complete."
    info "Retained DynamoDB tables (delete manually if desired):"
    info "  aws dynamodb delete-table --table-name mdf-submissions-$env --region $REGION"
    info "  aws dynamodb delete-table --table-name mdf-streams-$env --region $REGION"
}

# ---------------------------------------------------------------------------
# Quick deploy (code-only, no infrastructure changes)
# ---------------------------------------------------------------------------
quick_deploy() {
    local env=$1
    [[ -z "$env" ]] && error "Environment required: ./deploy.sh quick dev"

    local stack_name="mdf-connect-v2-$env"

    log "Quick deploying to $env (Lambda code only)..."

    # Package the code
    cd "$SCRIPT_DIR"
    zip -r /tmp/mdf-lambda.zip v2/ requirements.txt \
        -x "**/__pycache__/*" "**/*.pyc" "**/.pytest_cache/*"

    # Get Lambda function name from stack
    local func
    func=$(aws cloudformation describe-stack-resources \
        --stack-name "$stack_name" \
        --region "$REGION" \
        --query 'StackResources[?ResourceType==`AWS::Lambda::Function`].PhysicalResourceId' \
        --output text 2>/dev/null) \
        || error "Stack $stack_name not found. Deploy first with: ./deploy.sh $env"

    for f in $func; do
        log "Updating Lambda $f..."
        aws lambda update-function-code \
            --function-name "$f" \
            --region "$REGION" \
            --zip-file fileb:///tmp/mdf-lambda.zip \
            --no-cli-pager > /dev/null
    done

    rm /tmp/mdf-lambda.zip
    log "Quick deploy complete!"
}

# ---------------------------------------------------------------------------
# Local development server
# ---------------------------------------------------------------------------
local_server() {
    log "Starting local development server on http://127.0.0.1:8080"

    export STORE_BACKEND=sqlite
    export SQLITE_PATH=/tmp/mdf_connect_v2.db
    export STORAGE_BACKEND=local
    export USE_MOCK_DATACITE=true
    export AUTH_MODE=dev
    export LOCAL_DEV_AUTH=true
    export ALLOW_ALL_CURATORS=true
    export CURATOR_GROUP_IDS=
    export REQUIRED_GROUP_MEMBERSHIP=

    python3 -m v2.app.main
}

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
status() {
    local env=${1:-dev}
    local stack_name="mdf-connect-v2-$env"

    aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$REGION" \
        --query 'Stacks[0].{Status:StackStatus,Created:CreationTime,Updated:LastUpdatedTime,Outputs:Outputs[*].{Key:OutputKey,Value:OutputValue}}' \
        --output table 2>/dev/null \
        || warn "Stack $stack_name not found"
}

# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------
logs() {
    local env=${1:-dev}
    log "Tailing logs for mdf-connect-v2-$env..."
    sam logs --stack-name "mdf-connect-v2-$env" --region "$REGION" --tail
}

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
help() {
    echo "MDF Connect v2 — Deployment"
    echo ""
    echo "Usage: ./deploy.sh <command> [environment]"
    echo ""
    echo "Commands:"
    echo "  dev              Deploy to dev (self-contained, no Globus needed)"
    echo "  staging          Deploy to staging (requires Globus SSM params)"
    echo "  prod             Deploy to production (requires Globus SSM params)"
    echo "  quick <env>      Quick deploy (code only, skips CloudFormation)"
    echo "  local            Run local development server"
    echo "  status [env]     Show stack status (default: dev)"
    echo "  logs [env]       Tail Lambda logs (default: dev)"
    echo "  teardown <env>   Completely remove a deployment"
    echo "  build            Build SAM application"
    echo "  help             Show this help"
    echo ""
    echo "Dev deployment creates these AWS resources:"
    echo "  Stack:    mdf-connect-v2-dev"
    echo "  Tables:   mdf-submissions-dev, mdf-streams-dev"
    echo "  Bucket:   mdf-sam-deployments-dev"
    echo ""
    echo "Examples:"
    echo "  ./deploy.sh dev              # First deploy to AWS"
    echo "  ./deploy.sh quick dev        # Push code changes only"
    echo "  ./deploy.sh status dev       # Check deployment status"
    echo "  ./deploy.sh logs dev         # Watch Lambda logs"
    echo "  ./deploy.sh teardown dev     # Remove everything"
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
case "${1:-help}" in
    local)
        local_server
        ;;
    dev)
        check_deps
        deploy_dev
        ;;
    staging)
        check_deps
        deploy_prod staging
        ;;
    prod)
        check_deps
        deploy_prod prod
        ;;
    quick)
        check_deps
        quick_deploy "$2"
        ;;
    build)
        check_deps
        build
        ;;
    status)
        status "$2"
        ;;
    logs)
        logs "$2"
        ;;
    teardown)
        check_deps
        teardown "$2"
        ;;
    help|--help|-h)
        help
        ;;
    *)
        error "Unknown command: $1. Run './deploy.sh help' for usage."
        ;;
esac
