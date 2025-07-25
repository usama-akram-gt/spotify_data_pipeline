# Airflow Configuration Directory

This directory contains configuration files and DAGs (Directed Acyclic Graphs) for Apache Airflow, which orchestrates the Spotify data pipeline workflows.

## Directory Structure

- `dags/` - Contains the DAG definition files
- `plugins/` - Custom Airflow plugins specific to the Spotify pipeline
- `config/` - Airflow configuration overrides

## DAG Categories

The DAGs are categorized as follows:

### Data Ingestion DAGs
- `spotify_data_generation_dag.py` - Generates synthetic data for testing
- `spotify_streaming_history_dag.py` - Loads streaming history data
- `spotify_catalog_ingestion_dag.py` - Loads artist, album, and track data

### Processing DAGs
- `spotify_beam_processing_dag.py` - Runs Apache Beam transformations
- `spotify_scio_processing_dag.py` - Runs Scala/Scio transformations
- `spotify_real_time_processing_dag.py` - Manages Kafka streaming jobs

### Analytics DAGs
- `spotify_user_analytics_dag.py` - Generates user behavior analytics
- `spotify_content_analytics_dag.py` - Generates content popularity analytics
- `spotify_reporting_dag.py` - Produces scheduled reports

### Maintenance DAGs
- `spotify_data_quality_dag.py` - Runs data quality checks
- `spotify_cleanup_dag.py` - Performs database maintenance and cleanup
- `spotify_monitoring_dag.py` - Monitors pipeline health

## DAG Development

When developing new DAGs, follow these guidelines:

1. Use the template DAG in `dags/templates/template_dag.py`
2. Follow the established naming conventions
3. Include appropriate documentation
4. Add comprehensive error handling
5. Configure sensible default arguments
6. Include data quality checks
7. Add monitoring and alerting

## Configuration

The Airflow configuration is managed through:

1. Environment variables in the Docker Compose file
2. Custom configuration in `config/airflow.cfg`
3. Connection definitions for external systems

## Connections

The following connections need to be configured in Airflow:

- `postgres_default` - Connection to the PostgreSQL database
- `kafka_default` - Connection to the Kafka broker
- `slack_notifications` - For alerts and notifications
- `email_notifications` - For email reports

## Variables

Key Airflow variables used by the DAGs:

- `spotify_env` - Environment (dev, test, prod)
- `data_directory` - Path to data files
- `processing_parallelism` - Level of parallelism for processing
- `notification_recipients` - List of email recipients for alerts
- `retention_days` - Data retention period

## Scheduling

DAGs are scheduled with different frequencies:

- Hourly: Real-time aggregations and monitoring
- Daily: Most processing and analytics DAGs
- Weekly: Reports and heavy analytics
- Monthly: Long-term trend analysis and maintenance 