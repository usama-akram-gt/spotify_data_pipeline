#!/bin/bash
set -e

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
  local max_retries=30
  local retry_interval=5
  local retries=0
  
  echo "Waiting for PostgreSQL to be ready..."
  
  while ! nc -z postgres 5432; do
    if [[ $retries -ge $max_retries ]]; then
      echo "Error: PostgreSQL is not available after $max_retries retries."
      exit 1
    fi
    
    echo "PostgreSQL is not available yet. Retrying in $retry_interval seconds..."
    sleep $retry_interval
    ((retries++))
  done
  
  echo "PostgreSQL is ready!"
}

# Wait for PostgreSQL to be available
wait_for_postgres

# Initialize the database
airflow db init

# Create default admin user if it doesn't exist
if [[ ! $(airflow users list | grep admin) ]]; then
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
fi

# Initialize DAGs if script exists
if [[ -f /opt/airflow/scripts/init_dags.sh ]]; then
  echo "Initializing DAGs..."
  /opt/airflow/scripts/init_dags.sh
fi

# Run the command provided as CMD in the Dockerfile
exec airflow "$@" 