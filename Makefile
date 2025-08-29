# Spotify Data Pipeline Makefile - Azure/Kubernetes Edition
# -------------------------
# This Makefile provides commands to setup, run, and manage the Spotify data pipeline

# Environment variables
-include .env
-include .env.azure
export

# Azure and Kubernetes variables
RESOURCE_GROUP ?= spotify-pipeline-dev-rg
LOCATION ?= eastus
AKS_CLUSTER ?= spotify-k8s-cluster
ACR_NAME ?= spotifypipelineacr

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

.PHONY: help setup-env build up down restart logs ps clean generate-data run-pipeline start-streaming stop-streaming test exec-airflow backup-db restore-db format lint run-beam-job scio-pipeline test-dev test-prod scio-build scio-run scio-test scio-assembly test-unit test-integration test-performance test-coverage dbt-run azure-login azure-setup terraform-init terraform-plan terraform-apply terraform-destroy k8s-deploy k8s-delete docker-build-all docker-push-all

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
	@echo ""
	@echo "Local Development Operations:"
	@echo "  local-setup     - Setup local environment"
	@echo "  local-up        - Start local services (Airflow + PostgreSQL)"
	@echo "  local-down      - Stop local services"
	@echo "  local-pipeline  - Run complete local pipeline"
	@echo "  local-scio      - Run Scio pipeline locally"
	@echo "  local-dbt       - Run DBT locally"
	@echo "  local-test      - Test local pipeline"
	@echo ""
	@echo "Azure & Kubernetes Operations:"
	@echo "  azure-login     - Login to Azure"
	@echo "  azure-setup     - Setup Azure resources with Terraform"
	@echo "  terraform-init  - Initialize Terraform"
	@echo "  terraform-plan  - Plan Terraform changes"
	@echo "  terraform-apply - Apply Terraform changes"
	@echo "  terraform-destroy - Destroy Terraform resources"
	@echo "  k8s-deploy      - Deploy to Kubernetes"
	@echo "  k8s-delete      - Delete Kubernetes deployment"
	@echo "  docker-build-all - Build all Docker images"
	@echo "  docker-push-all - Push images to ACR"

setup-env: ## Setup environment files and directories
	@echo "Setting up environment..."
	@mkdir -p $(LOGS_DIR)/airflow $(LOGS_DIR)/kafka $(LOGS_DIR)/postgres $(LOGS_DIR)/beam $(LOGS_DIR)/scio
	@mkdir -p $(DATA_DIR)/raw $(DATA_DIR)/processed $(DATA_DIR)/analytics
	@mkdir -p src/ingestion src/transformation src/airflow src/scio src/dashboard src/validation
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
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/src/data_generator.py --scale $(SCALE)
	@echo "Fake data generated with scale: $(SCALE)"

run-pipeline: ## Trigger the ETL pipeline in Airflow
	@echo "Triggering ETL pipeline..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec airflow-webserver airflow dags trigger spotify_etl

start-streaming: ## Start Kafka streaming (producer and consumer)
	@echo "Starting Kafka streaming..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -d airflow-webserver python /opt/airflow/src/kafka_stream_producer.py --duration $(DURATION) --rate $(RATE)
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -d airflow-webserver python /opt/airflow/src/kafka_stream_consumer.py
	@echo "Streaming started. Producer will run for $(DURATION) seconds at $(RATE) events/second."

stop-streaming: ## Stop Kafka streaming processes
	@echo "Stopping Kafka streaming..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec airflow-webserver pkill -f "kafka_stream_producer.py" || true
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec airflow-webserver pkill -f "kafka_stream_consumer.py" || true
	@echo "Streaming stopped."

run-beam-job: ## Run an Apache Beam pipeline
	@echo "Running Apache Beam pipeline..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/src/beam_pipelines/$(PIPELINE).py --date $(DATE)
	@echo "Beam pipeline completed."

# Testing operations
test: test-dev test-prod ## Run all tests (development and production)
	@echo "All tests completed"

test-unit: ## Run unit tests only
	@echo "Running unit tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/src/tests/ -m "unit" -v
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -n Unit'"
	@echo "Unit tests completed"

test-integration: ## Run integration tests only
	@echo "Running integration tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/src/tests/ -m "integration" -v
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -n Integration'"
	@echo "Integration tests completed"

test-performance: ## Run performance tests
	@echo "Running performance tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/src/utils/run_performance_tests.py
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -n Performance'"
	@echo "Performance tests completed"

test-coverage: ## Run tests with coverage reporting
	@echo "Running tests with coverage reporting..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/src/tests/ --cov=/opt/beam/scripts --cov-report=xml --cov-report=term
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt coverage test coverageReport"
	@echo "Coverage tests completed"

test-dev: test-unit ## Run development tests (faster, less comprehensive)
	@echo "Running development tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/src/tests/ -m "not production and not performance" -v
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -l Production -l Performance'"
	@echo "Development database tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/src/utils/test_utils.py --test-type db --test-size $(TEST_SIZE)
	@echo "Development Kafka tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/src/utils/test_utils.py --test-type kafka --test-size $(TEST_SIZE)
	@echo "Development tests completed"

test-prod: test-unit test-integration test-performance ## Run production tests (slower, more comprehensive)
	@echo "Running production tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python -m pytest /opt/beam/src/tests/ -m "production" -v
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec scio-runner bash -c "cd /app && sbt 'testOnly * -- -n Production'"
	@echo "Production database tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/src/utils/test_utils.py --test-type db --test-size medium --production
	@echo "Production Kafka tests..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/src/utils/test_utils.py --test-type kafka --test-size medium --production
	@echo "End-to-end pipeline validation..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec beam-runner python /opt/beam/src/utils/test_utils.py --test-type pipeline --production
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
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) exec -T scio-runner bash -c "cd /app/src/scio && ./run_scio_pipeline.sh -p $(PIPELINE) -d $(DATE) -e $(ENV)"
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

# Azure & Kubernetes Operations
azure-login: ## Login to Azure CLI
	@echo "Logging into Azure..."
	az login
	az account set --subscription "$(AZURE_SUBSCRIPTION_ID)"

azure-setup: terraform-init terraform-apply ## Setup complete Azure infrastructure
	@echo "Azure infrastructure setup complete!"

terraform-init: ## Initialize Terraform
	@echo "Initializing Terraform..."
	cd terraform && terraform init

terraform-plan: ## Plan Terraform changes
	@echo "Planning Terraform changes..."
	cd terraform && terraform plan -var="environment=$(ENV)" -out=tfplan

terraform-apply: ## Apply Terraform changes
	@echo "Applying Terraform changes..."
	cd terraform && terraform apply tfplan
	@echo "Generating .env file from Terraform outputs..."
	cd terraform && terraform output -raw env_variables_template > ../.env.generated
	@echo "Environment variables saved to .env.generated - copy to .env"

terraform-destroy: ## Destroy Terraform resources
	@echo "Destroying Terraform resources..."
	cd terraform && terraform destroy -var="environment=$(ENV)" -auto-approve

docker-build-all: ## Build all Docker images
	@echo "Building Scio image..."
	docker build -f docker/Dockerfile.scio -t spotify-pipeline/scio:latest .
	@echo "Building DBT image..."  
	docker build -f docker/Dockerfile.dbt -t spotify-pipeline/dbt:latest .

docker-push-all: docker-build-all ## Push all images to Azure Container Registry
	@echo "Logging into ACR..."
	az acr login --name $(ACR_NAME)
	@echo "Tagging and pushing images..."
	docker tag spotify-pipeline/scio:latest $(ACR_NAME).azurecr.io/spotify-pipeline/scio:latest
	docker tag spotify-pipeline/dbt:latest $(ACR_NAME).azurecr.io/spotify-pipeline/dbt:latest
	docker push $(ACR_NAME).azurecr.io/spotify-pipeline/scio:latest
	docker push $(ACR_NAME).azurecr.io/spotify-pipeline/dbt:latest

k8s-deploy: ## Deploy to Kubernetes
	@echo "Deploying to Kubernetes..."
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/postgres.yaml
	kubectl apply -f k8s/airflow.yaml
	kubectl apply -f k8s/scio-job.yaml
	@echo "Checking deployment status..."
	kubectl get pods -n spotify-pipeline

k8s-delete: ## Delete Kubernetes deployment
	@echo "Deleting Kubernetes deployment..."
	kubectl delete -f k8s/ --ignore-not-found=true
	kubectl delete namespace spotify-pipeline --ignore-not-found=true

# Updated pipeline commands for Azure
scio-pipeline-azure: ## Run Scio pipeline in Azure/K8s
	@echo "Running Scio pipeline in Kubernetes..."
	kubectl create job --from=cronjob/scio-daily-pipeline scio-manual-run-$(shell date +%Y%m%d%H%M%S) -n spotify-pipeline

dbt-run-azure: ## Run DBT in Azure/Databricks
	@echo "Running DBT with Databricks..."
	cd dbt && dbt run --target prod --profiles-dir .

# ================================
# LOCAL DEVELOPMENT COMMANDS
# ================================

local-setup: ## Setup local development environment
	@echo "🏠 Setting up local development environment..."
	@if [ ! -f .env ]; then cp .env.local .env; echo "Created .env from .env.local template"; fi
	@mkdir -p data/{raw,processed,analytics,tmp} logs/{airflow,kafka,postgres,scio}
	@echo "✅ Local environment setup complete!"
	@echo "📝 Next steps:"
	@echo "   1. Review and update .env file with your settings"
	@echo "   2. Run 'make local-up' to start services"
	@echo "   3. Run 'make local-pipeline' to test the pipeline"

local-up: ## Start local services (Airflow + PostgreSQL + Kafka)
	@echo "🚀 Starting local services..."
	docker-compose --env-file .env -f docker-compose.local.yml up -d
	@echo "⏳ Waiting for services to be ready..."
	@sleep 30
	@echo "✅ Local services started!"
	@echo "🌐 Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "📊 Kafka UI: http://localhost:8081"
	@echo "🐘 PostgreSQL Admin: http://localhost:8082 (admin@spotify.com/admin)"
	@echo "💾 PostgreSQL Direct: localhost:5432"
	@echo "📡 Kafka Direct: localhost:9092"

local-down: ## Stop local services
	@echo "🛑 Stopping local services..."
	docker-compose --env-file .env -f docker-compose.local.yml down
	@echo "✅ Local services stopped"

local-status: ## Check status of local services
	@echo "📊 Local services status:"
	docker-compose -f docker-compose.local.yml ps

local-logs: ## View logs from local services
	@echo "📋 Viewing local service logs..."
	docker-compose -f docker-compose.local.yml logs -f

local-scio: ## Run Scio pipeline locally
	@echo "🔧 Running local Scio pipeline..."
	docker-compose -f docker-compose.local.yml --profile scio run --rm scio-runner bash -c "
	cd /app && 
	sbt 'runMain com.spotify.pipeline.transforms.LocalStreamingHistoryTransform $(shell date -d yesterday '+%Y-%m-%d')'
	"

local-dbt: ## Run DBT transformations locally
	@echo "📊 Running DBT locally..."
	docker-compose -f docker-compose.local.yml --profile dbt run --rm dbt-runner bash -c "
	cd /app/dbt && 
	dbt run --profiles-dir . --target dev
	"

local-dbt-databricks: ## Run DBT with Databricks Community Edition
	@echo "☁️ Running DBT with Databricks Community Edition..."
	@echo "⚠️  Make sure you have set DATABRICKS_ACCESS_TOKEN in .env"
	docker-compose -f docker-compose.local.yml --profile dbt run --rm dbt-runner bash -c "
	cd /app/dbt && 
	dbt run --profiles-dir . --target databricks_community
	"

local-pipeline: ## Run complete local pipeline via Airflow
	@echo "🚀 Triggering local pipeline in Airflow..."
	@echo "💡 This will run the 'local_spotify_etl_pipeline' DAG"
	docker-compose -f docker-compose.local.yml exec airflow-webserver airflow dags trigger local_spotify_etl_pipeline
	@echo "✅ Pipeline triggered! Check progress at http://localhost:8080"

local-test: ## Test local pipeline setup
	@echo "🧪 Testing local pipeline setup..."
	@echo "1. Checking database connection..."
	docker-compose -f docker-compose.local.yml exec postgres psql -U $(DB_USER) -d $(DB_NAME) -c "SELECT version();"
	@echo "2. Checking Airflow..."
	docker-compose -f docker-compose.local.yml exec airflow-webserver airflow version
	@echo "3. Checking data directories..."
	@ls -la data/ || echo "No data directory found"
	@echo "4. Checking Scio build..."
	docker-compose -f docker-compose.local.yml --profile scio run --rm scio-runner bash -c "cd /app && sbt compile" | head -20
	@echo "✅ Local test complete!"

local-generate-data: ## Generate sample data for local testing
	@echo "🎲 Generating sample data..."
	docker-compose -f docker-compose.local.yml exec airflow-webserver python -c \
		"import sys; sys.path.append('/opt/airflow/scripts'); \
		from ingestion.generate_fake_data import SpotifyDataGenerator; \
		db_params = {'host': 'postgres', 'port': '5432', 'database': '$(DB_NAME)', 'user': '$(DB_USER)', 'password': '$(DB_PASSWORD)'}; \
		generator = SpotifyDataGenerator(db_params); \
		result = generator.run_full_data_generation(scale='small'); \
		print('Generated data:', result)"

local-clean: ## Clean local data and logs
	@echo "🧹 Cleaning local data and logs..."
	@rm -rf data/* logs/*
	docker-compose -f docker-compose.local.yml down -v
	@echo "✅ Local cleanup complete!"

# Quick local development workflow
local-dev: local-setup local-up local-generate-data local-pipeline ## Complete local development setup and test