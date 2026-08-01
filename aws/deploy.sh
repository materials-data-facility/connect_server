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
#
# AWS credentials / profile:
#   No --profile flag is hard-coded anywhere in this script. Every `aws` and
#   `sam` invocation picks up the ambient credential chain, so all of these work
#   without editing anything:
#     AWS_PROFILE=mdf-prod ./deploy.sh prod        # named profile
#     ./deploy.sh prod                             # default profile
#     (GitHub Actions OIDC)  ./deploy.sh prod      # env-var credentials
#
# Non-interactive / CI:
#   Set MDF_NON_INTERACTIVE=1 (GitHub Actions' own CI=true also counts, as does
#   a non-TTY stdin) to remove every blocking prompt. See NON_INTERACTIVE below.
#   .github/workflows/deploy-v2.yml is the supported CI entry point.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

REGION="us-east-1"

# ---------------------------------------------------------------------------
# Non-interactive mode.
# GitHub Actions exports CI=true; .github/workflows/deploy-v2.yml additionally
# sets MDF_NON_INTERACTIVE=1. A piped/redirected stdin also counts. Anything
# that would otherwise block on a TTY prompt must honour this flag.
# ---------------------------------------------------------------------------
NON_INTERACTIVE=0
if [[ -n "${MDF_NON_INTERACTIVE:-}" || -n "${CI:-}" || ! -t 0 ]]; then
    NON_INTERACTIVE=1
fi

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
    prune_build
}

# ---------------------------------------------------------------------------
# Prune the built Lambda artifacts using .samignore
#
# Both functions use `CodeUri: .`, and SAM CLI has NO native file-exclusion
# mechanism for zip-packaged Python functions (no .samignore support; the
# python_pip builder's EXCLUDED_FILES tuple is hard-coded; `Metadata:
# BuildProperties` exclusion is esbuild-only; `sam build --exclude` skips
# resources, not files). So an untouched `sam build` copies the whole aws/ tree
# into each artifact: v1 modules, aws/tests/, every test_v2_*.py, docs,
# Dockerfile, deploy.sh, samconfig.toml, template.yaml.
#
# `sam deploy` zips .aws-sam/build/<Function>/ at package time, so deleting
# files from that directory after `sam build` and before `sam deploy` really
# does change what is uploaded. See aws/.samignore for the pattern syntax.
# ---------------------------------------------------------------------------
prune_build() {
    local ignore_file="$SCRIPT_DIR/.samignore"
    local build_dir="$SCRIPT_DIR/.aws-sam/build"

    [[ -d "$build_dir" ]] || return 0
    if [[ ! -f "$ignore_file" ]]; then
        warn ".samignore not found — shipping the full CodeUri tree"
        return 0
    fi

    log "Pruning build artifacts with .samignore..."
    python3 - "$ignore_file" "$build_dir" <<'PY'
import fnmatch
import os
import shutil
import sys

ignore_file, build_dir = sys.argv[1], sys.argv[2]

patterns = []
with open(ignore_file, encoding="utf-8") as fh:
    for raw in fh:
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # Refuse patterns that would nuke an entire artifact.
        if line.strip("/*") == "":
            print("  ! refusing catch-all pattern: %r" % line)
            continue
        patterns.append(line)

if not patterns:
    print("  no patterns; nothing to prune")
    sys.exit(0)


def matched_by(rel, is_dir):
    """rel is an artifact-root-relative POSIX path."""
    name = rel.rsplit("/", 1)[-1]
    for pattern in patterns:
        dir_only = pattern.endswith("/")
        pat = pattern[:-1] if dir_only else pattern
        if dir_only and not is_dir:
            continue
        if "/" in pat:
            if fnmatch.fnmatch(rel, pat):
                return pattern
        elif "/" not in rel and fnmatch.fnmatch(name, pat):
            # Bare patterns are top-level only, so "*.md" can never strip
            # READMEs out of installed dependencies.
            return pattern
    return None


def tree_size(path):
    total = files = 0
    for root, _dirs, names in os.walk(path):
        for n in names:
            p = os.path.join(root, n)
            files += 1
            try:
                total += os.path.getsize(p)
            except OSError:
                pass
    return total, files


artifacts = sorted(
    d for d in os.listdir(build_dir) if os.path.isdir(os.path.join(build_dir, d))
)
if not artifacts:
    print("  no function artifacts under %s" % build_dir)
    sys.exit(0)

for artifact in artifacts:
    root_dir = os.path.realpath(os.path.join(build_dir, artifact))
    removed_files = 0
    removed_bytes = 0
    hits = []

    for dirpath, dirnames, filenames in os.walk(root_dir, topdown=True):
        rel_dir = os.path.relpath(dirpath, root_dir).replace(os.sep, "/")
        rel_dir = "" if rel_dir == "." else rel_dir

        keep_dirs = []
        for d in dirnames:
            rel = "%s/%s" % (rel_dir, d) if rel_dir else d
            pattern = matched_by(rel, True)
            if pattern is None:
                keep_dirs.append(d)
                continue
            target = os.path.realpath(os.path.join(dirpath, d))
            if not (target == root_dir or target.startswith(root_dir + os.sep)):
                keep_dirs.append(d)
                continue
            size, count = tree_size(target)
            shutil.rmtree(target, ignore_errors=True)
            removed_files += count
            removed_bytes += size
            hits.append(rel + "/")
        dirnames[:] = keep_dirs  # do not descend into removed trees

        for f in filenames:
            rel = "%s/%s" % (rel_dir, f) if rel_dir else f
            if matched_by(rel, False) is None:
                continue
            target = os.path.realpath(os.path.join(dirpath, f))
            if not target.startswith(root_dir + os.sep):
                continue
            try:
                removed_bytes += os.path.getsize(target)
                os.remove(target)
                removed_files += 1
                hits.append(rel)
            except OSError as exc:
                print("  ! could not remove %s: %s" % (rel, exc))

    print(
        "  %s: removed %d file(s), %.1f KiB"
        % (artifact, removed_files, removed_bytes / 1024.0)
    )
    for h in sorted(hits)[:40]:
        print("      - %s" % h)
    if len(hits) > 40:
        print("      ... and %d more" % (len(hits) - 40))
PY
}

# ---------------------------------------------------------------------------
# Deploy to dev (fully self-contained)
# ---------------------------------------------------------------------------
deploy_dev() {
    local stack_name="mdf-connect-v2-dev"
    # Bucket names are global across ALL AWS accounts; the bare name is owned
    # by another account, so ours are suffixed with the account id (must match
    # s3_bucket in samconfig.toml).
    local account
    account=$(aws sts get-caller-identity --query Account --output text)
    local s3_bucket="mdf-sam-deployments-dev-$account"

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

    # Read base parameter_overrides from samconfig.toml up front so required
    # secrets can be validated against the resolved configuration before we
    # spend time building. CLI --parameter-overrides fully replaces samconfig
    # values, so we must pass the complete set when deploying.
    local base_params
    base_params=$(python3 -c "
try:
    import tomllib
except ImportError:
    import tomli as tomllib
with open('samconfig.toml', 'rb') as f:
    cfg = tomllib.load(f)
print(cfg.get('${env}', {}).get('deploy', {}).get('parameters', {}).get('parameter_overrides', ''))
" 2>/dev/null || echo "Environment=$env AuthMode=production")

    # Resolve DataCite credentials from SSM (namespaced /mdf/{env}/datacite-*).
    # Secrets are NEVER committed to samconfig.toml — they live in SSM only.
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

    # When DOIs are minted for real (UseMockDatacite=false — staging and prod),
    # DataCite credentials are mandatory. Refuse to deploy with the template's
    # "not-configured" placeholder, which would silently break DOI minting at
    # publish time instead of failing fast here.
    if echo "$base_params" | grep -q "UseMockDatacite=false"; then
        if [[ -z "$datacite_user" || -z "$datacite_pass" ]]; then
            error "DataCite credentials required for '$env' (UseMockDatacite=false) but missing from SSM.
  Store them first (test repository creds for staging, real creds for prod):
    aws ssm put-parameter --name /mdf/$env/datacite-username --value 'REPOSITORY_ID' --type String --region $REGION
    aws ssm put-parameter --name /mdf/$env/datacite-password --value 'REPOSITORY_PASSWORD' --type SecureString --region $REGION
  Optional overrides (otherwise samconfig.toml values are used):
    aws ssm put-parameter --name /mdf/$env/datacite-api-url --value 'https://api.datacite.org' --type String --region $REGION
    aws ssm put-parameter --name /mdf/$env/datacite-prefix --value '10.18126' --type String --region $REGION"
        fi
        log "DataCite credentials resolved from SSM"
    elif [[ -n "$datacite_user" ]]; then
        log "DataCite credentials resolved from SSM"
    else
        warn "DataCite SSM parameters not found — relying on mock/default DataCite behavior"
    fi

    # Account-suffixed: the bare name is owned by another AWS account (bucket
    # names are global). Must match s3_bucket in samconfig.toml.
    ensure_s3_bucket "mdf-sam-deployments-$env-$(aws sts get-caller-identity --query Account --output text)"
    build

    local all_params="$base_params GlobusClientId=$globus_id GlobusClientSecret=$globus_secret"

    # Append DataCite params if resolved from SSM
    [[ -n "$datacite_user" ]] && all_params="$all_params DataCiteUsername=$datacite_user"
    [[ -n "$datacite_pass" ]] && all_params="$all_params DataCitePassword=$datacite_pass"
    [[ -n "$datacite_url" ]] && all_params="$all_params DataCiteApiUrl=$datacite_url"
    [[ -n "$datacite_prefix" ]] && all_params="$all_params DataCitePrefix=$datacite_prefix"

    # In CI there is no TTY to approve a changeset on. The typed-confirmation
    # gate in .github/workflows/deploy-v2.yml (plus GitHub environment
    # protection rules) is the approval step there.
    local confirm_flag=()
    if [[ "$NON_INTERACTIVE" == "1" ]]; then
        confirm_flag=(--no-confirm-changeset)
    fi

    log "Deploying stack $stack_name..."
    sam deploy \
        --config-env "$env" \
        --no-fail-on-empty-changeset \
        "${confirm_flag[@]}" \
        --parameter-overrides "$all_params"

    log "Deployment complete!"
    print_custom_domain_dns "$stack_name"
}

# ---------------------------------------------------------------------------
# Custom domain DNS hint — printed only when the stack created a custom domain
# (ApiCustomDomainCertArn set). DNS is managed outside this account, so the
# record must be created by hand; see README "Custom domain".
# ---------------------------------------------------------------------------
print_custom_domain_dns() {
    local stack_name=$1
    local outputs domain regional
    outputs=$(aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs' --output json 2>/dev/null || echo "[]")
    domain=$(echo "$outputs" | python3 -c "
import sys, json
o = {x['OutputKey']: x['OutputValue'] for x in (json.load(sys.stdin) or [])}
print(o.get('ApiCustomDomainUrl', ''))
" 2>/dev/null || echo "")
    [[ -z "$domain" ]] && return 0
    regional=$(echo "$outputs" | python3 -c "
import sys, json
o = {x['OutputKey']: x['OutputValue'] for x in (json.load(sys.stdin) or [])}
print(o.get('ApiCustomDomainRegionalDomainName', ''))
" 2>/dev/null || echo "")
    echo ""
    info "Custom domain: $domain"
    info "  DNS target (CNAME / Route53 alias): $regional"
    info "  The domain only answers once that record exists and has propagated."
}

# ---------------------------------------------------------------------------
# Teardown — completely remove a deployment
# ---------------------------------------------------------------------------
teardown() {
    local env=$1
    [[ -z "$env" ]] && error "Environment required: ./deploy.sh teardown dev"

    local stack_name="mdf-connect-v2-$env"
    local s3_bucket
    s3_bucket="mdf-sam-deployments-$env-$(aws sts get-caller-identity --query Account --output text)"

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

    if [[ "$NON_INTERACTIVE" == "1" ]]; then
        # No TTY to type into. Require the same value out-of-band so an
        # automated caller still has to name the environment explicitly.
        [[ "${MDF_CONFIRM_TEARDOWN:-}" == "$env" ]] \
            || error "Non-interactive teardown requires MDF_CONFIRM_TEARDOWN=$env"
        warn "Non-interactive teardown confirmed via MDF_CONFIRM_TEARDOWN=$env"
    else
        read -p "Type '$env' to confirm teardown: " confirm
        [[ "$confirm" != "$env" ]] && error "Aborted."
    fi

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
