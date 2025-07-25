#!/bin/bash
# Run Scio Pipeline
# This script is used to run a Scio pipeline as part of the Spotify data pipeline

set -e

# Default values
PIPELINE="streaming_history"
DATE=$(date +%Y-%m-%d)
ENV="dev"
RUNNER="direct"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  key="$1"
  case $key in
    -p|--pipeline)
      PIPELINE="$2"
      shift
      shift
      ;;
    -d|--date)
      DATE="$2"
      shift
      shift
      ;;
    -e|--env)
      ENV="$2"
      shift
      shift
      ;;
    -r|--runner)
      RUNNER="$2"
      shift
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Path to Scio pipelines directory
SCIO_DIR="$(dirname "$0")/scio_pipelines"

# Export environment variables
export PIPELINE_ENV=$ENV
export BEAM_RUNNER=$RUNNER

echo "Running Scio pipeline: $PIPELINE for date: $DATE in environment: $ENV with runner: $RUNNER"

# Check if we need to build the project
if [ ! -f "$SCIO_DIR/target/scala-2.13/spotify-data-pipeline-0.1.0-SNAPSHOT.jar" ] || [ "$ENV" == "dev" ]; then
  echo "Building Scio pipeline project..."
  cd "$SCIO_DIR" && sbt clean assembly
fi

# Run the appropriate pipeline
case $PIPELINE in
  "streaming_history")
    java -cp "$SCIO_DIR/target/scala-2.13/spotify-data-pipeline-0.1.0-SNAPSHOT.jar" \
      com.spotify.pipeline.transforms.StreamingHistoryTransform "$DATE"
    ;;
  *)
    echo "Unknown pipeline: $PIPELINE"
    exit 1
    ;;
esac

echo "Pipeline execution completed." 