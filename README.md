# Spotify Data Pipeline

A scalable data pipeline for processing Spotify streaming data with Azure/Kubernetes deployment support.

## Features

- **Data Processing**: Scala (Scio) pipeline for streaming history transformation
- **Analytics**: DBT models with PostgreSQL and Databricks support
- **Orchestration**: Airflow for scheduling and monitoring
- **Infrastructure**: Azure/Kubernetes deployment with Terraform

## Quick Start

### Local Development
```bash
# Setup local environment
make local-setup
make local-up

# Run pipeline
make local-pipeline
```

### Azure/Kubernetes Deployment  
```bash
# Deploy to Azure
make azure-setup
make k8s-deploy
```

### Key Commands
- `make local-up` - Start local services
- `make local-pipeline` - Run complete pipeline locally  
- `make azure-setup` - Deploy Azure infrastructure
- `make k8s-deploy` - Deploy to Kubernetes

## Services
- **Airflow**: http://localhost:8080 (admin/admin)
- **Kafka UI**: http://localhost:8081
- **PostgreSQL**: localhost:5432

