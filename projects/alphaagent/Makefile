# AlphaAgent — one-command project orchestrator
# Usage: make help

SHELL := /bin/bash
PYTHON := python3.11
VENV := .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ---------- Setup ----------

.PHONY: setup
setup: ## Create venv, install Python deps, copy .env
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"
	[ -f .env ] || cp .env.example .env
	@echo "Setup complete. Activate with: source $(VENV)/bin/activate"

.PHONY: install
install: ## Install Python deps (assumes venv exists)
	$(PIP) install -e ".[dev]"

# ---------- Infrastructure ----------

.PHONY: up
up: ## Start all services (postgres, redpanda, airflow, marquez)
	docker compose up -d
	@echo "Waiting for services..."
	@sleep 10
	@echo ""
	@echo "Services up:"
	@echo "  Postgres:  localhost:5432 (user: alphaagent / pw: alphaagent / db: alphaagent)"
	@echo "  Redpanda:  localhost:9092"
	@echo "  Airflow:   http://localhost:8080 (admin / admin)"
	@echo "  Marquez:   http://localhost:3000"

.PHONY: down
down: ## Stop all services
	docker compose down

.PHONY: nuke
nuke: ## Stop services and delete all volumes (DESTRUCTIVE)
	docker compose down -v

.PHONY: logs
logs: ## Tail all service logs
	docker compose logs -f --tail=50

# ---------- Data ----------

.PHONY: seed
seed: ## Generate synthetic data and load into Postgres raw schema
	$(PY) -m data_generator.generate --portfolios 50 --securities 500 --days 730
	$(PY) -m ingestion.batch.load_raw

.PHONY: stream
stream: ## Start Kafka trade-event producer (runs in foreground)
	$(PY) -m ingestion.streaming.producer

.PHONY: consume
consume: ## Start Kafka consumer writing to Postgres (runs in foreground)
	$(PY) -m ingestion.streaming.consumer

# ---------- Transform ----------

.PHONY: dbt-deps
dbt-deps: ## Install dbt packages
	cd dbt_project && $(PWD)/$(VENV)/bin/dbt deps

.PHONY: dbt-run
dbt-run: ## Run all dbt models
	cd dbt_project && $(PWD)/$(VENV)/bin/dbt run

.PHONY: dbt-test
dbt-test: ## Run dbt tests
	cd dbt_project && $(PWD)/$(VENV)/bin/dbt test

.PHONY: dbt-docs
dbt-docs: ## Generate and serve dbt docs
	cd dbt_project && $(PWD)/$(VENV)/bin/dbt docs generate && $(PWD)/$(VENV)/bin/dbt docs serve

.PHONY: transform
transform: dbt-deps dbt-run dbt-test ## Full transform: deps + run + test

# ---------- Quality ----------

.PHONY: ge
ge: ## Run Great Expectations suites on mart tables
	$(PY) -m dq.run_expectations

# ---------- Orchestration ----------

.PHONY: run-pipeline
run-pipeline: seed transform ge ## Full batch pipeline end-to-end (dev shortcut)
	@echo "Batch pipeline complete."

# ---------- Agent + API + UI ----------

.PHONY: eval
eval: ## Run agent eval harness on golden Q&A set
	$(PY) -m evals.run --questions evals/golden.yaml --out evals/scorecard.md

.PHONY: serve
serve: ## Start FastAPI + Streamlit
	@echo "Starting API on :8000 and UI on :8501 (ctrl-c to stop)"
	$(PY) -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload & \
	$(PY) -m streamlit run ui/app.py --server.port 8501

.PHONY: api
api: ## Start only the FastAPI service
	$(PY) -m uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

.PHONY: ui
ui: ## Start only the Streamlit UI
	$(PY) -m streamlit run ui/app.py --server.port 8501

# ---------- Quality gates ----------

.PHONY: lint
lint: ## Run ruff + black checks
	$(VENV)/bin/ruff check .
	$(VENV)/bin/black --check .

.PHONY: format
format: ## Auto-format with ruff + black
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/black .

.PHONY: test
test: ## Run unit tests
	$(VENV)/bin/pytest tests/unit -v

.PHONY: test-integration
test-integration: ## Run integration tests (requires services up)
	$(VENV)/bin/pytest tests/integration -v

.PHONY: ci
ci: lint test ## What CI runs

# ---------- One-shot demo ----------

.PHONY: demo
demo: up seed transform ge eval ## Spin everything up and run the whole pipeline
	@echo ""
	@echo "✓ Demo ready. Run 'make serve' to open the UI."
