# Spotify Data Pipeline Makefile
# -------------------------
# This Makefile provides commands to setup, run, and manage the Spotify data pipeline

# Environment variables
include .env
export

# Variables
DOCKER_COMPOSE = docker-compose
DOCKER_COMPOSE_FILE = docker-compose.yml
LOGS_DIR = logs
DATA_DIR = data
SCALE ?= small
DURATION ?= 60
RATE ?= 10
PIPELINE ?= streaming_history
DATE ?= $(shell date +%Y-%m-%d)
ENV ?= dev
TEST_SIZE ?= tiny

.PHONY: help setup-env build up down restart logs ps clean generate-data run-pipeline start-streaming stop-streaming test exec-airflow backup-db restore-db format lint run-beam-job scio-pipeline test-dev test-prod scio-build scio-run scio-test scio-assembly test-unit test-integration test-performance test-coverage dbt-run dashboard

help: ## Display this help message
	@echo "Spotify Data Pipeline Commands:"
	@echo ""
	@echo "Environment Setup:"
	@echo "  setup           - Create necessary directories for data and logs"
	@echo "  build           - Build all Docker containers"
	@echo ""
	@echo "Container Management:"
	@echo "  up              - Start all containers"
	@echo "  down            - Stop all containers"
	@echo "  restart         - Restart all containers"
	@echo "  logs            - View logs from all containers"
	@echo "  ps              - List running containers"
	@echo "  clean           - Remove all containers, volumes, and data"
	@echo ""
	@echo "Data Operations:"
	@echo "  generate-data   - Generate fake data (SCALE=small|medium|large)"
	@echo "  kafka-stream    - Stream events to Kafka (DURATION=seconds, RATE=events/sec)"
	@echo "  etl             - Trigger the ETL pipeline"
	@echo "  beam-pipeline   - Run Apache Beam pipeline"
	@echo "  scio-pipeline   - Run Scio pipeline (PIPELINE=pipeline_name, DATE=YYYY-MM-DD, ENV=dev|prod)"
	@echo "  dbt-run         - Run DBT models"
	@echo ""
	@echo "Development & Testing:"
	@echo "  test            - Run all tests"
	@echo "  test-dev        - Run development tests (faster, less comprehensive)"
	@echo "  test-prod       - Run production tests (slower, more comprehensive)"
	@echo "  test-unit       - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-performance - Run performance tests only"
	@echo "  test-coverage   - Run tests with coverage reporting"
	@echo "  lint            - Run linting on code"
	@echo "  format          - Run code formatting"
	@echo ""
	@echo "Database Operations:"
	@echo "  backup-db       - Backup PostgreSQL database"
	@echo "  restore-db      - Restore PostgreSQL database"
	@echo ""
	@echo "Service Access:"
	@echo "  airflow         - Access the Airflow container"
	@echo "  scio-build      - Build the Scio project"
	@echo "  scio-run        - Run a specific Scio pipeline"
	@echo "  scio-test       - Run Scio tests"
	@echo "  scio-assembly   - Create Scio assembly JAR"
	@echo "  dashboard       - Open dashboard in browser"

setup-env: ## Setup environment files and directories
	@echo "Setting up environment..."
	@mkdir -p $(LOGS_DIR)/airflow $(LOGS_DIR)/kafka $(LOGS_DIR)/postgres $(LOGS_DIR)/beam $(LOGS_DIR)/scio
	@mkdir -p $(DATA_DIR)/raw $(DATA_DIR)/processed $(DATA_DIR)/analytics
	@mkdir -p scripts/beam_pipelines
	@echo "Environment setup complete."

build: ## Build all Docker containers
	@echo "Building Docker containers..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) build

up: ## Start all services
	@echo "Starting all services..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) up -d

down: ## Stop all services
	@echo "Stopping all services..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) down

restart: down up ## Restart all services

logs: ## View logs from all containers
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) logs -f

ps: ## List all running containers
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) ps

clean: ## Remove all generated data and containers
	@echo "Cleaning up the environment..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) down -v --remove-orphans
	@rm -rf $(DATA_DIR)/* $(LOGS_DIR)/*
	@echo "Cleanup complete."

# Data operations
generate-data: ## Generate fake data (small, medium, large)
	@echo "Generating fake data..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/scripts/data_generator.py --scale $(SCALE)
	@echo "Fake data generated with scale: $(SCALE)"

run-pipeline: ## Trigger the ETL pipeline in Airflow
	@echo "Triggering ETL pipeline..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec airflow-webserver airflow dags trigger spotify_etl

start-streaming: ## Start Kafka streaming (producer and consumer)
	@echo "Starting Kafka streaming..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -d airflow-webserver python /opt/airflow/scripts/kafka_stream_producer.py --duration $(DURATION) --rate $(RATE)
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -d airflow-webserver python /opt/airflow/scripts/kafka_stream_consumer.py
	@echo "Streaming started. Producer will run for $(DURATION) seconds at $(RATE) events/second."

stop-streaming: ## Stop Kafka streaming processes
	@echo "Stopping Kafka streaming..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec airflow-webserver pkill -f "kafka_stream_producer.py" || true
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec airflow-webserver pkill -f "kafka_stream_consumer.py" || true
	@echo "Streaming stopped."

run-beam-job: ## Run an Apache Beam pipeline
	@echo "Running Apache Beam pipeline..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/scripts/beam_pipelines/$(PIPELINE).py --date $(DATE)
	@echo "Beam pipeline completed."

# Testing operations
test: test-dev test-prod ## Run all tests (development and production)
	@echo "All tests completed"

test-unit: ## Run unit tests only
	@echo "Running unit tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/scripts/tests/ -m "unit" -v
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -n Unit'"
	@echo "Unit tests completed"

test-integration: ## Run integration tests only
	@echo "Running integration tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/scripts/tests/ -m "integration" -v
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -n Integration'"
	@echo "Integration tests completed"

test-performance: ## Run performance tests
	@echo "Running performance tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/scripts/utils/run_performance_tests.py
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -n Performance'"
	@echo "Performance tests completed"

test-coverage: ## Run tests with coverage reporting
	@echo "Running tests with coverage reporting..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/scripts/tests/ --cov=/opt/beam/scripts --cov-report=xml --cov-report=term
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt coverage test coverageReport"
	@echo "Coverage tests completed"

test-dev: test-unit ## Run development tests (faster, less comprehensive)
	@echo "Running development tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/scripts/tests/ -m "not production and not performance" -v
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -l Production -l Performance'"
	@echo "Development database tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/scripts/utils/test_utils.py --test-type db --test-size $(TEST_SIZE)
	@echo "Development Kafka tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/scripts/utils/test_utils.py --test-type kafka --test-size $(TEST_SIZE)
	@echo "Development tests completed"

test-prod: test-unit test-integration test-performance ## Run production tests (slower, more comprehensive)
	@echo "Running production tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/scripts/tests/ -m "production" -v
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -n Production'"
	@echo "Production database tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/scripts/utils/test_utils.py --test-type db --test-size medium --production
	@echo "Production Kafka tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/scripts/utils/test_utils.py --test-type kafka --test-size medium --production
	@echo "End-to-end pipeline validation..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/scripts/utils/test_utils.py --test-type pipeline --production
	@echo "Production tests completed"

exec-airflow: ## Open a shell in the Airflow container
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec airflow-webserver /bin/bash

exec-beam: ## Open a shell in the Beam container
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner /bin/bash

exec-scio: ## Open a shell in the Scio container
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner /bin/bash

# Database operations
backup-db: ## Backup PostgreSQL database
	@echo "Backing up database..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec postgres pg_dump -U $(DB_USER) $(DB_NAME) > backup_$(shell date +%Y%m%d%H%M%S).sql
	@echo "Backup complete."

restore-db: ## Restore PostgreSQL database from backup file
	@echo "Restoring database from $(backup)..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -T postgres psql -U $(DB_USER) $(DB_NAME) < $(backup)
	@echo "Restore complete."

# Code quality
format: ## Format Python code
	@echo "Formatting Python code..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec airflow-webserver black /opt/airflow/scripts /opt/airflow/dags
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt scalafix"
	@echo "Code formatting completed"

lint: ## Run linters on Python code
	@echo "Linting Python code..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec airflow-webserver pylint /opt/airflow/scripts /opt/airflow/dags
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt scalastyle"
	@echo "Linting completed"

# Scio operations
scio-pipeline:
	@echo "Running Scio pipeline..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -T scio-runner bash -c "cd /app && ./scripts/run_scio_pipeline.sh -p $(PIPELINE) -d $(DATE) -e $(ENV)"
	@echo "Scio pipeline $(PIPELINE) executed for date $(DATE) in $(ENV) environment"

# Scio project build
scio-build:
	@echo "Building Scio project..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -T scio-runner bash -c "cd /app && sbt clean compile"
	@echo "Scio project built"

# Scio run
scio-run:
	@echo "Running Scio pipeline..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -T scio-runner bash -c "cd /app && sbt 'runMain com.spotify.pipeline.transforms.$(PIPELINE)' -Denv=$(ENV) -Ddate=$(DATE)"
	@echo "Scio pipeline $(PIPELINE) executed with date $(DATE) in $(ENV) environment"

# Scio test
scio-test:
	@echo "Running Scio tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -T scio-runner bash -c "cd /app && sbt test"
	@echo "Scio tests completed"

# Scio assembly
scio-assembly:
	@echo "Creating Scio assembly JAR..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -T scio-runner bash -c "cd /app && sbt assembly"
	@echo "Scio assembly JAR created"

# DBT operations
dbt-run:
	@echo "Running DBT models..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec dbt dbt run
	@echo "DBT models completed"

# Dashboard operations
dashboard:
	@echo "Opening dashboard at http://localhost:8050"
	@which open > /dev/null && open http://localhost:8050 || echo "Open http://localhost:8050 in your browser"