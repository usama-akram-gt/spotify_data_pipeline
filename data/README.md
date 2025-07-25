# Data Directory

This directory contains data used and produced by the Spotify data pipeline.

## Structure

- `raw/`: Raw data files, including input files and dumps from the raw database
- `processed/`: Intermediate data that has been processed but is not yet in final form
- `analytics/`: Final data ready for analysis, reporting, and visualization
- `beam-temp/`: Temporary files used by Apache Beam pipelines

## Data Flow

The typical data flow in the pipeline is:

1. Raw data is ingested into the `raw/` directory or directly into the raw database
2. Data is processed using Apache Beam (Python) or Scio (Scala) pipelines
3. Processed data is stored in the `processed/` directory or the processed database
4. Analytics queries and transformations prepare the data for analysis
5. Final analytics data is stored in the `analytics/` directory or analytics tables

## File Formats

The pipeline supports multiple file formats:
- JSON: Used primarily for configuration and raw data exports
- CSV: Used for simple tabular data
- Parquet: Used for efficient storage of structured data
- Avro: Used for schema-based binary serialization

## Notes

- The `raw/` directory contains immutable data that should not be modified
- The `processed/` directory may contain intermediate results that can be regenerated
- The `analytics/` directory contains derived data for business insights 