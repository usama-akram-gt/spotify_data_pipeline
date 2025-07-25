# PostgreSQL Configuration

This directory contains configuration files and initialization scripts for the PostgreSQL database used in the Spotify data pipeline.

## Contents

- `init-scripts/`: Directory containing SQL scripts that run when the PostgreSQL container starts for the first time
- `postgresql.conf`: PostgreSQL configuration overrides (optional)
- `pg_hba.conf`: PostgreSQL client authentication configuration (optional)

## Usage

The initialization scripts are automatically executed alphabetically when the PostgreSQL container starts for the first time. They create the necessary database schema, users, and initial tables for the Spotify data pipeline.

## Database Schema

The database has the following schema structure:

- `public`: Default schema containing user, artist, album, and track tables
- `staging`: Schema for holding data during the ETL process
- `processed`: Schema for storing processed data from pipelines
- `analytics`: Schema for storing aggregated data and metrics
- `monitoring`: Schema for pipeline execution metrics and logs

## Important Notes

- To modify the initialization scripts, edit the files in the `init-scripts/` directory
- The PostgreSQL container persists data to a Docker volume
- Default credentials are set in the docker-compose.yml file 