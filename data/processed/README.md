# Processed Data Directory

This directory contains intermediate data that has been processed but is not yet in its final analytics-ready form. It serves as a staging area between raw data ingestion and final analytics output.

## Purpose

The processed data directory serves several purposes:

- Storage for intermediate transformation results
- Checkpoint data for multi-stage pipelines
- Temporary data storage during ETL operations
- Interim results from Beam/Scio pipelines

## Data Format

Data in this directory is typically stored in formats optimized for processing:

- Parquet files with optimized schemas
- CSV files with standardized formatting
- JSON files with normalized structures
- Temporary database tables

## Usage

Processed data is accessed by:

- Downstream analytics processes
- Second-stage transformation pipelines
- Data quality validation tools
- Incremental processing jobs

## Notes

- Data in this directory is transient and may be regenerated
- Files should follow a consistent naming convention:
  - `{data_type}_{processing_stage}_{date}.{format}`
  - Example: `streaming_history_enriched_2023-01-01.parquet` 