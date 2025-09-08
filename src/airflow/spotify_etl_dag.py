#!/usr/bin/env python3
"""
Spotify Data Pipeline ETL DAG

This DAG orchestrates the ETL process for the Spotify-like data pipeline:
1. Extracts raw streaming data from Kafka topics
2. Processes and transforms the data using Apache Beam
3. Loads the processed data into analytics tables
4. Creates summary reports

Schedule: Daily at midnight
"""

import os
import sys
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Add src directory to path
sys.path.append('/opt/airflow/src')

# Import configuration and custom modules
from config import config, get_db_params
from ingestion.generate_fake_data import SpotifyDataGenerator
from transformation.batch_song_plays_processor import run_song_metrics_pipeline, run_daily_active_users_pipeline

# Database connection parameters using new config system
DB_PARAMS = get_db_params()

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Helper functions
def generate_data_func(scale='small', **kwargs):
    """Generate fake Spotify data for testing."""
    data_generator = SpotifyDataGenerator(DB_PARAMS)
    results = data_generator.run_full_data_generation(scale=scale)
    return results

def process_song_plays_func(execution_date, **kwargs):
    """Process song plays for the previous day."""
    # Get the previous day's date
    execution_date = pendulum.instance(execution_date)
    prev_day = execution_date.add(days=-1).strftime('%Y-%m-%d')
    
    # Run the batch processing
    run_song_metrics_pipeline(prev_day, prev_day)
    
    return f"Processed song metrics for {prev_day}"

def calculate_active_users_func(execution_date, **kwargs):
    """Calculate daily active users for the previous day."""
    # Get the previous day's date
    execution_date = pendulum.instance(execution_date)
    prev_day = execution_date.add(days=-1).strftime('%Y-%m-%d')
    
    # Run the DAU pipeline
    run_daily_active_users_pipeline(prev_day)
    
    return f"Calculated daily active users for {prev_day}"

# Create DAG
dag = DAG(
    'spotify_etl_pipeline',
    default_args=default_args,
    description='Spotify ETL Data Pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['spotify', 'etl', 'data_pipeline'],
)

# Define Tasks

# Task to log pipeline start
start_pipeline = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "Starting Spotify ETL Pipeline at $(date)"',
    dag=dag,
)

# Task to generate data (in production, this would be a data ingestion task)
generate_data = BashOperator(
    task_id='generate_data',
    bash_command='echo "Running data generator script" && python /app/src/ingestion/data_generator.py',
    dag=dag,
)

# Task to run Scio pipeline (replaces deprecated Python Beam pipeline)
run_scio_pipeline_main = BashOperator(
    task_id='run_scio_pipeline_main',
    bash_command='echo "Running Scio pipeline" && cd /app/src/scio/scio_pipelines && sbt "runMain com.spotify.pipeline.transforms.StreamingHistoryTransform {{ execution_date.strftime("%Y-%m-%d") }}"',
    dag=dag,
)

# Remove this duplicate - already handled above

# Task to record pipeline execution in monitoring table
record_pipeline_execution = PostgresOperator(
    task_id='record_pipeline_execution',
    postgres_conn_id='postgres_default',
    sql="""
    INSERT INTO monitoring.pipeline_executions 
    (pipeline_name, status, start_time, end_time, duration_seconds, records_processed)
    VALUES 
    ('spotify_etl_pipeline', 'completed', 
     TIMESTAMP '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}', 
     TIMESTAMP '{{ (execution_date + macros.timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S") }}',
     {{ (macros.timedelta(hours=1).total_seconds()) }},
     (SELECT COUNT(*) FROM raw.streaming_history))
    """,
    dag=dag,
)

# Task to run DBT transformations
run_dbt = BashOperator(
    task_id='run_dbt',
    bash_command='cd /app/dbt && dbt run --profiles-dir .',
    dag=dag,
)

# Task to log pipeline completion
end_pipeline = BashOperator(
    task_id='end_pipeline',
    bash_command='echo "Completed Spotify ETL Pipeline at $(date)"',
    dag=dag,
)

# Set task dependencies - Updated for Scio-only pipeline
start_pipeline >> generate_data
generate_data >> run_scio_pipeline_main
run_scio_pipeline_main >> record_pipeline_execution
record_pipeline_execution >> run_dbt
run_dbt >> end_pipeline 