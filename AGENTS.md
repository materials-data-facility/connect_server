# Repository Guidelines

## Project Structure & Module Organization
Service logic resides in `aws/`, where each Lambda-backed endpoint has a Python module (`submit.py`, `status.py`, `submissions.py`) and shared helpers live in `utils.py`. Automated flow definitions and deployment helpers sit in `automate/` (notably `minimus_mdf_flow.py` and `deploy_mdf_flow.py`). Operational scripts for tokens, submissions, and schema sync live in `scripts/`, while infrastructure templates and IAM policies are grouped in `infra/`. Test suites and BDD feature files are colocated in `aws/tests/` with payload fixtures in `aws/tests/schemas/`, and high-level background material remains in `docs/`.

## Build, Test, and Development Commands
Target Python 3.7.10 to mirror production. Recommended setup:
- `python3 -m venv .venv && source .venv/bin/activate` — create an isolated environment.
- `pip install -r aws/requirements.txt` — Lambda runtime dependencies.
- `pip install -r aws/tests/requirements-test.txt` — pytest, pytest-bdd, and boto mocks.
- `PYTHONPATH=aws python -m pytest aws/tests --ignore=aws/tests/schemas` — run the suite locally.
For flow updates, install `automate/requirements.txt` before invoking `python automate/deploy_mdf_flow.py --env dev` to stage the definition.

## Coding Style & Naming Conventions
Follow PEP 8 with four-space indentation and concise module docstrings describing each handler’s contract. Keep functions and variables in `snake_case`, reserve `CamelCase` for classes, and uppercase constants. Mirror API routes with entry-point names, isolate AWS or Globus clients behind manager classes, and prefer explicit imports to ease packaging for Lambda layers.

## Testing Guidelines
Pair changes with unit tests in `test_*.py` and behavior coverage in the relevant `*.feature` files when workflows shift. Use `pytest -k <pattern>` for focused runs but complete a full `pytest` pass before requesting review. Maintain deterministic fixtures in `conftest.py`, mock network calls, and update JSON schemas when payload contracts change.

## Commit & Pull Request Guidelines
Branch from `dev`, keep commits single-purpose, and phrase messages in the imperative mood (e.g., `adjust submissions pagination`). Open PRs against `dev`, include a brief change summary, test artifacts, and references to linked issues or Globus tickets. Secure peer review before merging; once validated in the dev environment, raise a `dev`→`main` PR for production promotion.

## Security & Configuration Tips
Store Globus credentials in environment variables or `.mdfsecrets`; never commit secrets. Use helper utilities such as `scripts/get_mdf_token.py` and `scripts/status_versions.py` when troubleshooting to avoid manual token handling. Coordinate any IAM or policy modifications under `infra/` with the platform team and verify logs via CloudWatch using environment-scoped credentials.
