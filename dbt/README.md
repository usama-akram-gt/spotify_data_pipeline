# dbt (Data Build Tool) Directory

This directory contains the dbt project for the Spotify data pipeline, which handles the transformation layer of the analytics workflow.

## Overview

The dbt project transforms processed data into analytics-ready models, implementing business logic, aggregations, and data quality tests. It creates a semantic layer that business users and BI tools can directly query.

## Project Structure

- `models/` - Contains SQL models organized by subject area
  - `core/` - Core dimension and fact tables
  - `intermediate/` - Intermediate calculation tables
  - `marts/` - Business-specific data marts
  - `staging/` - Staging models that clean source data
  
- `macros/` - Reusable SQL snippets and utilities
  - `helpers/` - Helper macros
  - `tests/` - Custom test macros
  - `utils/` - Utility macros
  
- `snapshots/` - Point-in-time snapshots of slowly changing dimensions
  - `artists/` - Artist dimension snapshots
  - `users/` - User dimension snapshots
  
- `tests/` - Data quality tests
  - `generic/` - Generic tests that apply to multiple models
  - `singular/` - Tests for specific models
  
- `seeds/` - Static reference data
  - `country_codes.csv` - ISO country codes
  - `genre_categories.csv` - Genre categorization

- `analysis/` - Ad-hoc analytical queries
  - `user_segments.sql` - User segmentation analysis
  - `content_affinity.sql` - Content affinity analysis

## Key Models

### Core Models

- `dim_users.sql` - User dimension table
- `dim_artists.sql` - Artist dimension table
- `dim_tracks.sql` - Track dimension table
- `dim_albums.sql` - Album dimension table
- `dim_dates.sql` - Date dimension table
- `fact_streaming.sql` - Streaming fact table

### Mart Models

- `mart_user_engagement.sql` - User engagement metrics
- `mart_content_popularity.sql` - Content popularity metrics
- `mart_listening_patterns.sql` - Listening pattern analysis
- `mart_regional_trends.sql` - Regional trend analysis

## Usage

### Setup

1. Install dbt:
   ```bash
   pip install dbt-postgres
   ```

2. Set up the profile:
   ```bash
   cp profiles.yml ~/.dbt/
   ```

3. Test the connection:
   ```bash
   dbt debug
   ```

### Running dbt

- Run all models:
  ```bash
  dbt run
  ```

- Run specific models:
  ```bash
  dbt run --select mart_user_engagement
  ```

- Run tests:
  ```bash
  dbt test
  ```

- Generate documentation:
  ```bash
  dbt docs generate
  dbt docs serve
  ```

### CI/CD Integration

The dbt project is integrated with the CI/CD pipeline:

1. PRs trigger test runs
2. Main branch merges trigger full builds
3. Scheduled jobs run daily builds and tests
4. Documentation is auto-generated

## Best Practices

1. **Documentation**: Document all models with descriptions and column comments
2. **Testing**: Create tests for all primary keys and critical business logic
3. **Modularity**: Break complex logic into intermediate models
4. **Naming**: Follow consistent naming conventions
5. **Performance**: Optimize models for performance with appropriate materialization
6. **Freshness**: Define freshness requirements for source data

## Dependencies

- PostgreSQL database (version 12+)
- dbt-postgres adapter (version 1.0.0+)
- Python 3.8+ 