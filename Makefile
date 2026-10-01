# airflow-etl -- local Airflow 3.1.8 environment helpers (macOS / Linux)
#
# Make is preinstalled with macOS Command Line Tools (xcode-select --install).
# These targets are optional: every one of them is a thin wrapper around the
# docker compose commands documented in README.md.

COMPOSE := docker compose

.DEFAULT_GOAL := help
.PHONY: help check init up down restart ps logs psql cli scheduler-version version test-trigger trigger-dag dags-list dags-list-import-errors venv verify-dag reset

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

check: ## Verify Docker and Compose are installed and the engine is running
	docker --version
	$(COMPOSE) version
	docker info > /dev/null && echo "Docker engine: running"

init: ## First-time initialization (metadata DB, migrations, admin user)
	$(COMPOSE) up airflow-init

up: ## Start all Airflow services in detached mode
	$(COMPOSE) up -d

down: ## Stop and remove all containers and the network
	$(COMPOSE) down

restart: ## Restart the stack (DAG file changes are picked up automatically)
	$(COMPOSE) restart

ps: ## Show container status and health
	$(COMPOSE) ps

logs: ## Follow logs from all services (Ctrl+C to stop)
	$(COMPOSE) logs -f

psql: ## Open a psql shell against the Airflow metadata database
	$(COMPOSE) exec postgres psql -U airflow

cli: ## Open an Airflow CLI shell (bash) inside a container
	$(COMPOSE) exec airflow-scheduler bash

scheduler-version: ## Confirm the Python version inside the Airflow containers
	$(COMPOSE) exec airflow-scheduler python --version

version: ## Report the pinned Airflow image version
	@grep AIRFLOW_IMAGE_NAME .env

venv: ## Create .venv with Python 3.12 and install the Task SDK (with Airflow constraints)
	python3.12 -m venv .venv
	.venv/bin/python -m pip install --upgrade pip
	.venv/bin/python -m pip install -r requirements-dev.txt \
		--constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt

verify-dag: ## Offline check: parse the DAG locally and print tasks + edges (needs .venv)
	@.venv/bin/python scripts/verify_dag.py dags/reliable_pipeline_demo.py

dags-list: ## List the DAGs that Airflow currently sees
	$(COMPOSE) exec airflow-scheduler airflow dags list

dags-list-import-errors: ## Show DAG parsing/import errors
	$(COMPOSE) exec airflow-scheduler airflow dags list-import-errors

trigger-dag: ## Trigger a manual DAG run (make trigger-dag DAG_ID=reliable_pipeline_demo)
	$(COMPOSE) exec airflow-scheduler airflow dags trigger $(DAG_ID)

test-trigger: ## Shortcut: trigger reliable_pipeline_demo from the CLI
	$(MAKE) trigger-dag DAG_ID=reliable_pipeline_demo

reset: ## DESTRUCTIVE: remove all containers, volumes, and generated logs
	$(COMPOSE) down --volumes --remove-orphans
	rm -rf logs/*