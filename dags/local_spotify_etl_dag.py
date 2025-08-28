#!/usr/bin/env python3
"""
Local Spotify Data Pipeline ETL DAG

This DAG orchestrates the ETL process for local development:
1. Generates sample data (for testing)
2. Runs Scio pipeline for data processing
3. Runs DBT for analytics transformations
4. Records pipeline execution metrics

Schedule: Daily at 2 AM (or manual trigger)
Environment: Local development on Mac
"""

import os
import sys
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Add scripts directory to path
sys.path.append('/opt/airflow/scripts')

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('DB_HOST', 'postgres'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'spotify_data'),
    'user': os.getenv('DB_USER', 'spotify_user'),
    'password': os.getenv('DB_PASSWORD', 'spotify_password')
}

# Define default arguments
default_args = {
    'owner': 'local-dev',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Helper functions
def generate_sample_data_func(**kwargs):
    """Generate sample data for local testing."""
    try:
        # Import here to avoid issues if module doesn't exist
        from ingestion.generate_fake_data import SpotifyDataGenerator
        data_generator = SpotifyDataGenerator(DB_PARAMS)
        results = data_generator.run_full_data_generation(scale='small')
        return f"Generated sample data: {results}"
    except ImportError:
        return "Data generator not available - using existing data"

def check_data_quality_func(**kwargs):
    """Basic data quality checks."""
    import psycopg2
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        
        # Check if we have data for processing date
        execution_date = kwargs['execution_date']
        check_date = execution_date.strftime('%Y-%m-%d')
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM raw.streaming_history 
            WHERE DATE(played_at) = %s
        """, (check_date,))
        
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        if count == 0:
            raise ValueError(f"No streaming data found for date {check_date}")
        
        return f"Data quality check passed: {count} records for {check_date}"
        
    except Exception as e:
        raise ValueError(f"Data quality check failed: {str(e)}")

# Create DAG
dag = DAG(
    'local_spotify_etl_pipeline',
    default_args=default_args,
    description='Local Spotify ETL Data Pipeline (Mac Development)',
    schedule_interval=None,  # Manual trigger only for local dev
    start_date=days_ago(1),
    catchup=False,
    tags=['spotify', 'etl', 'local', 'scio', 'development'],
)

# Define Tasks

# Task to log pipeline start
start_pipeline = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "🚀 Starting Local Spotify ETL Pipeline at $(date)"',
    dag=dag,
)

# Task to generate sample data (optional, for testing)
generate_sample_data = PythonOperator(
    task_id='generate_sample_data',
    python_callable=generate_sample_data_func,
    dag=dag,
)

# Task to check data quality
data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality_func,
    dag=dag,
)

# Task to run Scio pipeline (Local version)
run_scio_pipeline = BashOperator(
    task_id='run_local_scio_pipeline',
    bash_command="""
    echo "🔧 Running Local Scio Pipeline..."
    cd /opt/airflow/scripts/scio_pipelines
    sbt "runMain com.spotify.pipeline.transforms.LocalStreamingHistoryTransform {{ ds }}"
    """,
    dag=dag,
)

# Task to run DBT transformations (Local PostgreSQL)
run_dbt_local = BashOperator(
    task_id='run_dbt_local',
    bash_command="""
    echo "📊 Running DBT transformations (Local PostgreSQL)..."
    cd /opt/airflow/dbt
    dbt run --profiles-dir . --target dev
    dbt test --profiles-dir . --target dev
    """,
    dag=dag,
)

# Task to run DBT with Databricks Community Edition (optional)
run_dbt_databricks = BashOperator(
    task_id='run_dbt_databricks',
    bash_command="""
    echo "☁️ Running DBT with Databricks Community Edition..."
    cd /opt/airflow/dbt
    dbt run --profiles-dir . --target databricks_community
    """,
    dag=dag,
)

# Task to record pipeline execution in monitoring table
record_pipeline_execution = PostgresOperator(
    task_id='record_pipeline_execution',
    postgres_conn_id='postgres_default',
    sql="""
    INSERT INTO analytics.pipeline_executions 
    (pipeline_name, environment, status, start_time, end_time, duration_seconds, records_processed, execution_date)
    VALUES 
    ('local_spotify_etl_pipeline', 'local_mac',  'completed', 
     TIMESTAMP '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}', 
     NOW(),
     EXTRACT(EPOCH FROM (NOW() - TIMESTAMP '{{ execution_date.strftime("%Y-%m-%d %H:%M:%S") }}')),
     (SELECT COUNT(*) FROM raw.streaming_history WHERE DATE(played_at) = '{{ ds }}'),
     DATE '{{ ds }}'
    )
    """,
    dag=dag,
)

# Task to check output files
check_output_files = BashOperator(
    task_id='check_output_files',
    bash_command="""
    echo "📁 Checking output files..."
    echo "Raw data directory:"
    ls -la /opt/airflow/data/raw/ || echo "No raw data directory"
    echo "Processed data directory:" 
    ls -la /opt/airflow/data/processed/ || echo "No processed data directory"
    echo "Analytics data directory:"
    ls -la /opt/airflow/data/analytics/ || echo "No analytics data directory"
    
    echo "📊 Checking database analytics tables:"
    psql postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME} -c "
    SELECT 'daily_user_listening_stats' as table_name, COUNT(*) as record_count 
    FROM analytics.daily_user_listening_stats
    UNION ALL
    SELECT 'track_popularity_metrics' as table_name, COUNT(*) as record_count 
    FROM analytics.track_popularity_metrics;
    " || echo "Analytics tables not ready yet"
    """,
    dag=dag,
)

# Task to log pipeline completion
end_pipeline = BashOperator(
    task_id='end_pipeline',
    bash_command='echo "✅ Completed Local Spotify ETL Pipeline at $(date)"',
    dag=dag,
)

# Set task dependencies - Two parallel paths after data quality check
start_pipeline >> generate_sample_data >> data_quality_check

# Main path: Local PostgreSQL processing
data_quality_check >> run_scio_pipeline >> run_dbt_local >> record_pipeline_execution

# Optional path: Databricks Community Edition (runs in parallel)
data_quality_check >> run_dbt_databricks

# Both paths converge at output check
[record_pipeline_execution, run_dbt_databricks] >> check_output_files >> end_pipeline