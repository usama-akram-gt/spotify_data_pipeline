# Dashboards Directory

This directory contains configuration files, templates, and documentation for the Spotify data pipeline dashboards.

## Overview

The dashboards provide visual insights into the Spotify data pipeline, including:

- User listening behaviors and patterns
- Track and artist popularity metrics
- Streaming history trends
- Pipeline performance monitoring
- Data quality metrics

## Dashboard Tools

The Spotify data pipeline uses the following dashboard tools:

1. **Metabase** - Primary BI tool for analytics dashboards
2. **Airflow UI** - For pipeline monitoring and orchestration
3. **Kafka UI** - For real-time streaming monitoring
4. **Custom Web Dashboards** - For specific use cases

## Dashboard Configuration

### Metabase Dashboards

The `metabase/` directory contains:
- Export files for predefined dashboards
- SQL queries used in Metabase cards
- Dashboard import/export scripts

### Custom Dashboards

The `custom/` directory contains:
- Frontend code for custom dashboards
- API endpoints for dashboard data
- Configuration files

## Accessing Dashboards

### Local Development

- Metabase: http://localhost:3000
- Airflow: http://localhost:8081
- Kafka UI: http://localhost:8080

### Production Environment

- Metabase: https://metabase.example.com
- Airflow: https://airflow.example.com
- Kafka UI: https://kafka-ui.example.com

## Dashboard Categories

1. **Executive Dashboards**
   - Overview of key metrics
   - High-level user engagement statistics
   - Platform growth trends

2. **Analytical Dashboards**
   - Detailed user behavior analysis
   - Content popularity and trending analysis
   - Genre and artist insights

3. **Operational Dashboards**
   - Pipeline performance metrics
   - Data quality monitoring
   - System health indicators

4. **Troubleshooting Dashboards**
   - Error rates and details
   - Pipeline bottlenecks
   - Data validation issues

## Dashboard Development

To add a new dashboard:

1. Create a new directory under the appropriate category
2. Add dashboard configuration files
3. Document the dashboard purpose and target audience
4. Include any required SQL queries or data sources
5. Update the dashboard inventory file

## Best Practices

- Keep dashboards focused on specific user needs
- Use consistent naming conventions and color schemes
- Provide context with clear titles and descriptions
- Include date ranges and data freshness indicators
- Add drill-down capabilities for detailed analysis 