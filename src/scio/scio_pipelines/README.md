# Spotify Data Pipeline - Scio Implementation

This directory contains the Scala/Scio implementation of the data transformation pipelines for the Spotify data pipeline project.

## Overview

The Spotify Scio pipeline is a high-performance data processing solution built using:

- [Scio](https://spotify.github.io/scio/) - A Scala API for Apache Beam
- [Apache Beam](https://beam.apache.org/) - A unified programming model for batch and streaming data processing
- Scala 2.13 - A modern, functional programming language for the JVM

This implementation replaces the Python-based Apache Beam pipelines with more performant Scala code, taking advantage of:

- Static typing for increased reliability
- Functional programming patterns for concise, maintainable code
- JVM performance benefits
- Strong integration with SQL databases via JDBC

## Architecture

The pipeline follows a modular architecture:

```
scio_pipelines/
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── application.conf  # Configuration settings 
│   │   └── scala/com/spotify/pipeline/transforms/
│   │       └── StreamingHistoryTransform.scala  # Main pipeline definition
│   └── test/
│       └── scala/com/spotify/pipeline/transforms/
│           ├── StreamingHistoryTransformTest.scala  # Unit tests
│           └── ScioJdbcTest.scala  # JDBC testing examples
├── project/
│   └── plugins.sbt  # SBT plugin definitions
├── build.sbt  # Project build configuration
├── Dockerfile  # Container definition
└── README.md  # This file
```

## Pipeline Implementations

### StreamingHistoryTransform

This pipeline transforms raw streaming history data from the PostgreSQL database by:

1. Reading streaming events from the `streaming_history` table via JDBC
2. Enriching them with time-based information (time of day, weekend/weekday)
3. Calculating completion ratios to identify listening patterns
4. Computing user listening statistics (aggregated by date and user)
5. Computing track popularity metrics (aggregated by date and track)
6. Writing the results back to the analytics database

## Development

### Prerequisites

- JDK 11+
- SBT 1.9.x
- PostgreSQL
- Docker (optional, for containerized execution)

### Building the Project

```bash
# Build and compile
cd scio_pipelines
sbt clean compile

# Run tests
sbt test

# Create assembly JAR (for deployment)
sbt assembly
```

### Running a Pipeline

The project includes a convenience script for running pipelines:

```bash
# Run with default settings
./scripts/run_scio_pipeline.sh -p streaming_history

# Run with specific date and environment
./scripts/run_scio_pipeline.sh -p streaming_history -d 2023-12-01 -e prod
```

Or directly with SBT:

```bash
# Run pipeline via SBT with parameters
sbt "runMain com.spotify.pipeline.transforms.StreamingHistoryTransform \
  --date=2023-12-01 \
  --env=dev"
```

## Configuration

The application uses the [Typesafe Config](https://github.com/lightbend/config) library for configuration management, with settings defined in `src/main/resources/application.conf`.

Key configuration settings include:

- Environment selection (`dev` or `prod`)
- Database connection parameters (overridable with environment variables)
- Beam execution environment (Direct or Dataflow)

## Testing

The project includes multiple levels of testing:

### Unit Tests

Basic unit tests for individual components:

```bash
# Run all unit tests
sbt "testOnly * -- -n Unit"
```

### Integration Tests

Tests that validate integration with external systems:

```bash
# Run all integration tests
sbt "testOnly * -- -n Integration"
```

### Performance Tests

Tests that benchmark pipeline performance:

```bash
# Run all performance tests
sbt "testOnly * -- -n Performance"
```

### Production Tests

Comprehensive tests that validate the entire pipeline:

```bash
# Run all production tests
sbt "testOnly * -- -n Production"
```

## Docker

A Dockerfile is provided for containerized execution:

```bash
# Build the Docker image
docker build -t spotify-scio-pipeline .

# Run a pipeline in the container
docker run spotify-scio-pipeline ./scripts/run_scio_pipeline.sh -p streaming_history
```

## Benefits of Scio over Python Beam

This implementation offers several advantages:

1. **Performance**: JVM-based execution is typically faster than Python
2. **Type Safety**: Scala's type system catches many errors at compile time
3. **Functional Programming**: Scala's FP features enable more concise code
4. **Testing**: Scio's testing framework makes it easier to write comprehensive tests
5. **Integration**: Better integration with the JVM ecosystem

## Troubleshooting

Common issues and solutions:

- **Database Connection Issues**: Verify your database credentials in the config
- **Memory Problems**: Increase JVM heap size with `-Xmx` flag in SBT or Docker
- **Pipeline Failures**: Check logs for specific error messages
- **Slow Performance**: Try tuning the `numShards` parameter or increasing parallelism

## References

- [Scio Documentation](https://spotify.github.io/scio/)
- [Apache Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)
- [Scala Documentation](https://docs.scala-lang.org/)
- [SBT Reference Manual](https://www.scala-sbt.org/1.x/docs/) 