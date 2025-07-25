# Spotify Data Pipeline

A comprehensive data pipeline for processing Spotify streaming history data, featuring dual implementation in both Python (Apache Beam) and Scala (Scio).

## Overview

This project implements a complete data pipeline for Spotify streaming data that includes:

- **Data Generation**: Synthetic data generation for development and testing
- **Data Ingestion**: Streaming and batch data ingestion capabilities
- **Data Storage**: PostgreSQL for structured data and MinIO (S3-compatible) for raw files
- **Data Processing**: Both Python (Apache Beam) and Scala (Scio) implementations
- **Data Transformation**: DBT models for analytics transformations
- **Orchestration**: Airflow for scheduling and monitoring
- **Visualization**: Interactive Dash dashboard for insights

## Architecture

The pipeline architecture consists of the following components:

```
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│  Data Sources  │────▶│  Ingestion     │────▶│  Storage       │
└────────────────┘     └────────────────┘     └────────────────┘
                                                      │
                                                      ▼
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│  Visualization │◀────│  Transformation│◀────│  Processing    │
└────────────────┘     └────────────────┘     └────────────────┘
        ▲                                             ▲
        │                                             │
        └─────────────┐               ┌───────────────┘
                      ▼               ▼
                  ┌────────────────────────┐
                  │      Orchestration     │
                  └────────────────────────┘
```

### Directory Structure

- `/config`: Configuration files
- `/dags`: Airflow DAG definitions
- `/dashboards`: Dash dashboard application
- `/data`: Data directories (raw, processed, analytics)
- `/dbt`: DBT models and configurations
- `/docker`: Docker configuration files
- `/logs`: Log files
- `/scripts`: Processing scripts (Python and Scala/Scio)
- `/sql_queries`: SQL queries for reporting and analysis

## Key Features

- **Dual Implementation**: Processing pipelines in both Python (Apache Beam) and Scala (Scio)
- **Real-time Processing**: Kafka integration for streaming capabilities
- **Batch Processing**: Comprehensive ETL for historical data
- **Comprehensive Testing**: Unit, integration, and end-to-end tests
- **Containerization**: Complete Docker setup for all components
- **Modular Design**: Easily extensible for additional data sources or transformations
- **Configurability**: Environment-specific configurations

## Scala/Scio Implementation

The project includes a robust Scala implementation using Spotify's Scio framework:

- Type-safe data processing with Scala
- JDBC integration for database connections
- Comprehensive test suite including unit, integration, and production tests

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Make (optional, for using the Makefile commands)

### Setup

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/spotify-pipeline.git
   cd spotify-pipeline
   ```

2. Start the services:
   ```
   make up
   ```
   or
   ```
   docker compose up -d
   ```

3. Generate sample data:
   ```
   make generate-data
   ```

4. Run the processing pipelines:
   ```
   # Run Apache Beam pipeline (Python)
   make beam-pipeline
   
   # Run Scio pipeline (Scala)
   make scio-pipeline
   ```

5. Run DBT models:
   ```
   make dbt-run
   ```

6. Access the dashboard:
   ```
   make dashboard
   ```
   or open `http://localhost:8050` in your browser

### Environment Configuration

The project uses environment variables for configuration:

- Database connection details
- Kafka configuration
- MinIO/S3 settings
- Airflow parameters

These are defined in the `.env` file and passed to each service.

## Makefile Commands

For convenience, the project includes a comprehensive Makefile:

- `make build`: Build all Docker containers
- `make up`: Start all services
- `make down`: Stop all services
- `make ps`: Show running services
- `make logs`: Show logs from all services
- `make logs-<service>`: Show logs from a specific service
- `make generate-data`: Run data generator
- `make beam-pipeline`: Run Apache Beam pipeline
- `make scio-pipeline`: Run Scala/Scio pipeline
- `make dbt-run`: Run DBT models
- `make dashboard`: Open dashboard in browser
- `make clean`: Remove all generated data and containers

## Troubleshooting

### Common Issues

- **Database Connection Failures**: Ensure PostgreSQL is up and healthy
- **Kafka Connection Issues**: Check Kafka and Zookeeper status
- **Pipeline Failures**: Check the logs directory for detailed error messages

## License

This project is licensed under the MIT License - see the LICENSE file for details.

