# MDF Connect v2 - Development & Deployment
#
# Quick Start:
#   make local      # Start local development server
#   make test       # Run tests
#   make deploy-dev # Deploy to AWS dev environment

.PHONY: help local test lint build deploy-dev deploy-prod clean

# Default target
help:
	@echo "MDF Connect v2 Backend"
	@echo ""
	@echo "Development:"
	@echo "  make local         Start local development server (port 8080)"
	@echo "  make demo          Run the demo script"
	@echo "  make test          Run unit tests"
	@echo "  make lint          Run linters"
	@echo ""
	@echo "Deployment:"
	@echo "  make build         Build SAM application"
	@echo "  make deploy-dev    Deploy to dev environment"
	@echo "  make deploy-staging Deploy to staging environment"
	@echo "  make deploy-prod   Deploy to production"
	@echo ""
	@echo "Utilities:"
	@echo "  make logs-dev      Tail logs from dev"
	@echo "  make clean         Clean build artifacts"

# ===========================================================================
# Development
# ===========================================================================

local:
	@echo "Starting local server on http://127.0.0.1:8080"
	@STORE_BACKEND=sqlite AUTH_MODE=dev python -m v2.app.main

demo:
	@python v2/demo_full_workflow.py

test:
	@pytest tests/ -v --tb=short

lint:
	@ruff check v2/
	@mypy v2/ --ignore-missing-imports

# ===========================================================================
# Deployment
# ===========================================================================

build:
	@sam build

validate:
	@sam validate --lint

deploy-dev: build
	@sam deploy --config-env dev --no-confirm-changeset
	@echo ""
	@echo "Dev deployment complete! API URL:"
	@aws cloudformation describe-stacks \
		--stack-name mdf-connect-v2-dev \
		--query 'Stacks[0].Outputs[?OutputKey==`ApiUrl`].OutputValue' \
		--output text

deploy-staging: build
	@sam deploy --config-env staging
	@echo ""
	@echo "Staging deployment complete!"

deploy-prod: build
	@echo "Deploying to PRODUCTION. This will prompt for confirmation."
	@sam deploy --config-env prod

# ===========================================================================
# Monitoring
# ===========================================================================

logs-dev:
	@sam logs --stack-name mdf-connect-v2-dev --tail

logs-prod:
	@sam logs --stack-name mdf-connect-v2-prod --tail

status-dev:
	@aws cloudformation describe-stacks \
		--stack-name mdf-connect-v2-dev \
		--query 'Stacks[0].{Status:StackStatus,Outputs:Outputs}' \
		--output table

# ===========================================================================
# Cleanup
# ===========================================================================

clean:
	@rm -rf .aws-sam/
	@find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned build artifacts"

# ===========================================================================
# Layer Management
# ===========================================================================

layer-deps:
	@mkdir -p layers/dependencies/python
	@pip install -r requirements.txt -t layers/dependencies/python/
	@echo "Dependencies installed to layers/dependencies/"
