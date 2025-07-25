# SQL Queries Directory

This directory contains SQL queries used in the Spotify data pipeline for data analysis, reporting, and ad-hoc investigations.

## Categories

The queries are organized into several categories:

### Data Quality
- Validation queries to check data integrity
- Gap identification in streaming history
- Duplicate detection and resolution

### Analytics
- User behavior analytics
- Track and artist popularity
- Listening pattern analysis
- Trending content identification

### Reporting
- Daily/weekly/monthly aggregations
- Subscription type analysis
- Regional listening trends
- Device usage statistics

### Maintenance
- Table optimization queries
- Index management
- Partition maintenance
- Database health checks

## Usage

Queries can be executed:

1. Directly against the database:
   ```bash
   psql -h localhost -U postgres -d spotify -f sql_queries/analytics/top_tracks.sql
   ```

2. Through the Python script interface:
   ```bash
   python scripts/utils/run_sql_query.py --query_file=sql_queries/reporting/monthly_report.sql
   ```

3. Via Airflow tasks using the PostgresOperator

## Query Naming Convention

Queries follow this naming convention:
```
{category}_{subject}_{frequency|action|metric}.sql
```

Examples:
- `analytics_user_listening_time.sql`
- `reporting_monthly_active_users.sql`
- `maintenance_optimize_indices.sql`

## Important Notes

- Always test queries on a development environment before running in production
- Large queries should include comments explaining the logic
- Consider performance implications for queries that scan large tables
- Use parameterized queries when appropriate 