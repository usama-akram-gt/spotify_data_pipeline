# Raw Data Directory

This directory stores raw, unprocessed data files from various sources:

- Streaming history events from the Spotify API
- User activity data 
- Artist and track metadata
- Generated test data

Raw data is stored here before being processed by the data pipeline. This data represents the source of truth and should not be modified after ingestion.

## Data Format

Data in this directory may be stored in various formats:

- CSV files
- JSON files
- Parquet files
- Raw SQL database dumps

## Usage

Raw data is accessed by:

- ETL processes that transform it into processed/analytics data
- Kafka producers for data generation testing
- Beam/Scio pipelines for batch processing

## Notes

- Data in this directory should be considered immutable
- Files should follow a consistent naming convention:
  - `{data_type}_{date}_{source}.{format}`
  - Example: `streaming_history_2023-01-01_api.json` 