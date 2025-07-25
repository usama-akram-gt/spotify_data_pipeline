#!/bin/bash
set -e

# Create directories if they don't exist
mkdir -p /opt/airflow/dags
mkdir -p /opt/airflow/logs
mkdir -p /opt/airflow/plugins

# Function to create a symlink if the target exists and the link doesn't
create_symlink_if_target_exists() {
  local target=$1
  local link=$2
  
  if [[ -d $target && ! -L $link ]]; then
    echo "Creating symlink from $target to $link"
    ln -sf $target $link
  elif [[ ! -d $target ]]; then
    echo "Warning: Target directory $target does not exist. Skipping symlink creation."
  fi
}

# Ensure DAGs from mounted volume are accessible
if [[ -d /opt/airflow/dags_from_volume ]]; then
  for dag_file in /opt/airflow/dags_from_volume/*.py; do
    if [[ -f $dag_file ]]; then
      dag_name=$(basename $dag_file)
      echo "Linking DAG: $dag_name"
      ln -sf $dag_file /opt/airflow/dags/$dag_name
    fi
  done
fi

# Copy example DAGs if dags directory is empty
if [[ -z "$(ls -A /opt/airflow/dags)" ]]; then
  echo "DAGs directory is empty. Creating example DAGs..."
  
  # Create a simple example DAG
  cat > /opt/airflow/dags/example_spotify_pipeline_dag.py << 'EOF'
"""
Example Spotify Pipeline DAG

This DAG demonstrates the typical workflow of the Spotify data pipeline,
including data ingestion, processing, and analytics generation.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spotify_pipeline',
    default_args=default_args,
    description='A Spotify data pipeline workflow',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['spotify', 'pipeline', 'example'],
)

# Generate sample data
generate_data = BashOperator(
    task_id='generate_sample_data',
    bash_command='python /app/scripts/data_generator.py --scale small',
    dag=dag,
)

# Run Kafka producer to simulate streaming
start_kafka_producer = BashOperator(
    task_id='start_kafka_producer',
    bash_command='python /app/scripts/kafka_stream_producer.py --duration 300 &',
    dag=dag,
)

# Run Kafka consumer to process streaming data
start_kafka_consumer = BashOperator(
    task_id='start_kafka_consumer',
    bash_command='python /app/scripts/kafka_stream_consumer.py --duration 310',
    dag=dag,
)

# Run batch transformation with Apache Beam
run_beam_pipeline = BashOperator(
    task_id='run_beam_pipeline',
    bash_command='cd /app && python -m scripts.beam_pipelines.streaming_history_transform',
    dag=dag,
)

# Run batch transformation with Scio
run_scio_pipeline = BashOperator(
    task_id='run_scio_pipeline',
    bash_command='cd /app && ./scripts/run_scio_pipeline.sh streaming_history',
    dag=dag,
)

# Generate analytics from processed data
generate_analytics = PostgresOperator(
    task_id='generate_analytics',
    postgres_conn_id='postgres_default',
    sql="""
    INSERT INTO analytics.user_listening_stats
    SELECT 
        user_id,
        COUNT(DISTINCT track_id) as unique_tracks_played,
        SUM(ms_played) / 60000.0 as total_minutes_played,
        COUNT(*) as total_plays
    FROM 
        processed.streaming_history
    WHERE 
        played_at >= NOW() - INTERVAL '1 day'
    GROUP BY 
        user_id
    ON CONFLICT (user_id) 
    DO UPDATE SET
        unique_tracks_played = EXCLUDED.unique_tracks_played,
        total_minutes_played = EXCLUDED.total_minutes_played,
        total_plays = EXCLUDED.total_plays;
    """,
    dag=dag,
)

# Set task dependencies
generate_data >> start_kafka_producer >> start_kafka_consumer
generate_data >> run_beam_pipeline >> generate_analytics
generate_data >> run_scio_pipeline >> generate_analytics
EOF

  echo "Example DAG created."
fi

echo "DAG initialization completed." 