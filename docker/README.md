# Docker Configuration

This directory contains Dockerfiles and related configuration for all services in the Spotify data pipeline.

## Directory Structure

- `data-generator/`: Data generator service for creating synthetic Spotify data
- `beam/`: Apache Beam pipeline service for data processing
- `scio/`: Scala/Scio pipeline service for high-performance data processing
- `airflow/`: Airflow service for workflow orchestration
- `postgres/`: PostgreSQL database initialization and configuration
- `dbt/`: Data build tool (dbt) for data transformations
- `dashboard/`: Dashboard application for visualizing analytics

## Services Overview

### Data Generator

The data generator service creates synthetic Spotify data, including:
- Users, artists, albums, and tracks
- Playlists and playlist tracks
- Streaming history events

Built with Python and uses the Faker library to generate realistic data.

### Apache Beam Pipeline

The Apache Beam pipeline processes streaming data and performs transformations such as:
- Enriching raw streaming history data
- Calculating user listening statistics
- Computing track popularity metrics

Implemented in Python using the Apache Beam framework.

### Scio Pipeline

The Scio pipeline is a high-performance alternative to the Beam pipeline, implemented in Scala using Spotify's Scio framework (a Scala API for Apache Beam).

### PostgreSQL

PostgreSQL is used as the main data store, with schemas for:
- Raw data (raw schema)
- Processed data (processed schema)
- Analytics (analytics schema)

### Airflow

Apache Airflow orchestrates the entire pipeline, scheduling and monitoring data processing jobs.

## Building and Running

All services can be built and run using Docker Compose:

```bash
docker-compose build
docker-compose up -d
```

Individual services can be built and run separately:

```bash
docker-compose build data-generator
docker-compose up -d data-generator
```

## Configuration

Service configuration is managed through environment variables defined in the `.env` file at the root of the project.

## Logging

Logs for all services are stored in the `logs/` directory, with subdirectories for each service. 