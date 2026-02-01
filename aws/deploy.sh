#!/bin/bash
# MDF v2 Backend Deployment Script
#
# Usage:
#   ./deploy.sh dev      # Deploy to dev environment
#   ./deploy.sh prod     # Deploy to production
#   ./deploy.sh local    # Run local server
#
# First-time setup:
#   pip install aws-sam-cli
#   aws configure  # Set up AWS credentials

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log() { echo -e "${GREEN}[MDF]${NC} $1"; }
warn() { echo -e "${YELLOW}[MDF]${NC} $1"; }
error() { echo -e "${RED}[MDF]${NC} $1"; exit 1; }

# Check dependencies
check_deps() {
    if ! command -v sam &> /dev/null; then
        error "AWS SAM CLI not found. Install with: pip install aws-sam-cli"
    fi
    if ! command -v aws &> /dev/null; then
        error "AWS CLI not found. Install with: pip install awscli"
    fi
}

# Build the SAM application
build() {
    log "Building SAM application..."
    sam build --use-container 2>/dev/null || sam build
}

# Deploy to an environment
deploy() {
    local env=$1

    if [[ -z "$env" ]]; then
        error "Environment required: dev, staging, or prod"
    fi

    log "Deploying to $env environment..."

    # Build first
    build

    # Deploy with environment-specific config
    sam deploy \
        --config-env "$env" \
        --no-confirm-changeset \
        --no-fail-on-empty-changeset

    log "Deployment complete!"

    # Get the API URL
    local stack_name="mdf-connect-v2-$env"
    local api_url=$(aws cloudformation describe-stacks \
        --stack-name "$stack_name" \
        --query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' \
        --output text 2>/dev/null || echo "")

    if [[ -n "$api_url" ]]; then
        log "API URL: $api_url"
        echo ""
        echo "Test with:"
        echo "  curl ${api_url}search?q=test"
    fi
}

# Quick deploy without rebuild (for code-only changes)
quick_deploy() {
    local env=$1

    if [[ -z "$env" ]]; then
        error "Environment required: dev, staging, or prod"
    fi

    log "Quick deploying to $env (skipping full build)..."

    # Just update the Lambda code directly
    local stack_name="mdf-connect-v2-$env"

    # Package the code
    cd v2
    zip -r ../lambda.zip . -x "__pycache__/*" "*.pyc" ".pytest_cache/*"
    cd ..

    # Get Lambda function names from stack
    local functions=$(aws cloudformation describe-stack-resources \
        --stack-name "$stack_name" \
        --query 'StackResources[?ResourceType==`AWS::Lambda::Function`].PhysicalResourceId' \
        --output text 2>/dev/null || echo "")

    for func in $functions; do
        log "Updating $func..."
        aws lambda update-function-code \
            --function-name "$func" \
            --zip-file fileb://lambda.zip \
            --no-cli-pager > /dev/null
    done

    rm lambda.zip
    log "Quick deploy complete!"
}

# Run local development server
local_server() {
    log "Starting local development server..."

    export STORE_BACKEND=sqlite
    export SQLITE_PATH=/tmp/mdf_connect_v2.db
    export STORAGE_BACKEND=local
    export USE_MOCK_DATACITE=true

    # Check for Globus token
    if [[ -f ~/.mdf/v2_https_tokens.json ]]; then
        warn "Globus token found. Set STORAGE_BACKEND=globus to use Globus storage."
    fi

    python3 v2/local_server.py
}

# Run local with Globus backend
local_globus() {
    log "Starting local server with Globus backend..."

    if [[ ! -f ~/.mdf/v2_https_tokens.json ]]; then
        warn "No Globus token found. Run: python test_globus_upload.py"
    fi

    export STORE_BACKEND=sqlite
    export SQLITE_PATH=/tmp/mdf_connect_v2.db
    export STORAGE_BACKEND=globus
    export USE_MOCK_DATACITE=true

    python3 v2/local_server.py
}

# Run tests
test() {
    log "Running tests..."
    python3 -m pytest tests/ -v
}

# Show help
help() {
    echo "MDF v2 Backend Deployment"
    echo ""
    echo "Usage: ./deploy.sh <command>"
    echo ""
    echo "Commands:"
    echo "  local       Run local development server (SQLite + local storage)"
    echo "  local-globus Run local server with Globus storage backend"
    echo "  dev         Deploy to dev environment"
    echo "  staging     Deploy to staging environment"
    echo "  prod        Deploy to production environment"
    echo "  quick <env> Quick deploy (code only, no infrastructure changes)"
    echo "  build       Build SAM application"
    echo "  test        Run tests"
    echo "  demo        Run the demo script"
    echo ""
    echo "API Endpoints:"
    echo "  Streaming:"
    echo "    POST /stream/create         Create a new data stream"
    echo "    POST /stream/:id/upload     Upload files to stream"
    echo "    POST /stream/:id/close      Close stream (optionally mint DOI)"
    echo "    GET  /stream/:id            Get stream status"
    echo "    GET  /stream/:id/preview    Preview stream files"
    echo ""
    echo "  Curation:"
    echo "    GET  /curation/pending      List submissions awaiting curation"
    echo "    GET  /curation/:id          Get submission details for review"
    echo "    POST /curation/:id/approve  Approve and publish (mints DOI)"
    echo "    POST /curation/:id/reject   Reject with reason"
    echo ""
    echo "Examples:"
    echo "  ./deploy.sh local          # Start local server"
    echo "  ./deploy.sh dev            # Deploy to dev"
    echo "  ./deploy.sh quick dev      # Quick update to dev"
    echo ""
}

# Run demo
demo() {
    log "Running MDF v2 demo..."
    python3 demo_mdf_v2.py
}

# Main entry point
case "${1:-help}" in
    local)
        local_server
        ;;
    local-globus)
        local_globus
        ;;
    dev|staging|prod)
        check_deps
        deploy "$1"
        ;;
    quick)
        check_deps
        quick_deploy "$2"
        ;;
    build)
        check_deps
        build
        ;;
    test)
        test
        ;;
    demo)
        demo
        ;;
    help|--help|-h)
        help
        ;;
    *)
        error "Unknown command: $1. Run './deploy.sh help' for usage."
        ;;
esac
