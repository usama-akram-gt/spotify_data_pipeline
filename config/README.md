# Configuration Directory

This directory contains configuration files for the Spotify data pipeline components.

## Structure

- `postgres/`: PostgreSQL database configuration
  - `init.sql`: Database initialization script
- `airflow/`: Apache Airflow configuration
- `kafka/`: Kafka configuration

## PostgreSQL Configuration

The `postgres/init.sql` file sets up the database schema and tables for the Spotify data pipeline:

- Creates schemas: raw, processed, analytics, and monitoring
- Defines tables for:
  - Users, artists, albums, tracks, playlists
  - Streaming history events
  - Processed streaming data
  - Analytics tables for user listening stats, track popularity, etc.
  - Pipeline execution monitoring

## Airflow Configuration

The Airflow configuration includes:

- DAG definitions
- Connection settings
- Variables and environment settings

## Kafka Configuration

Kafka configuration includes:

- Topic definitions
- Broker settings
- Consumer group configurations
- Stream processing settings

## Environment Settings

Environment-specific settings are stored in `.env` files at the root of the project directory, which contain variables for:

- Database connections
- Kafka brokers
- Service endpoints
- Credentials
- Processing parameters

## Usage

Configuration files are mounted into their respective containers or used by scripts to set up the environment. The PostgreSQL init script runs automatically when the database container starts for the first time. 