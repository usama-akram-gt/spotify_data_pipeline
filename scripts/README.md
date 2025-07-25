# Scripts Directory

This directory contains all the code for the Spotify data pipeline processing logic.

## Structure

- `data_generator.py`: Generates synthetic Spotify streaming data for testing
- `beam_pipeline.py`: Apache Beam pipeline for processing streaming history data
- `scio_pipelines/`: Scala/Scio implementations of data processing pipelines
- `utils/`: Shared utility modules used by multiple scripts

## Python Scripts

### Data Generator
- `data_generator.py`: Creates realistic synthetic data for users, artists, tracks, playlists, and streaming events

### Apache Beam Pipelines
- `beam_pipeline.py`: Main Beam pipeline for processing streaming history data
- Implements transformations for:
  - Enriching raw streaming data
  - Calculating user listening statistics
  - Computing track and artist popularity metrics

## Scala/Scio Pipelines

The `scio_pipelines/` directory contains Scala implementations of data processing pipelines using Spotify's Scio framework:

- `src/main/scala/com/spotify/pipeline/transforms/`: Pipeline transformation code
- `src/test/scala/com/spotify/pipeline/transforms/`: Test cases for the pipeline

## Utilities

The `utils/` directory contains shared Python modules:

- `db_utils.py`: Database connection and query utilities
- `kafka_utils.py`: Kafka producer and consumer utilities
- `log_utils.py`: Logging configuration and utilities
- `test_utils.py`: Utilities for testing

## Development Guide

### Running the Data Generator

```bash
python data_generator.py --scale small|medium|large
```

### Running the Beam Pipeline

```bash
python beam_pipeline.py --start_date 2023-01-01 --end_date 2023-01-31
```

### Building and Running Scio Pipelines

```bash
cd scio_pipelines
sbt "runMain com.spotify.pipeline.transforms.StreamingHistoryTransform"
```

## Testing

Tests for Python scripts are in the corresponding `test_*.py` files. 