#!/usr/bin/env python3
"""
Example usage of the unified configuration system
This demonstrates how to use environment-specific configurations
"""

import os
from config import config

def demonstrate_config_usage():
    """Demonstrate how to use the configuration system"""
    
    print("=== Spotify Pipeline Configuration Demo ===")
    print(f"Current Environment: {config.environment}")
    print(f"Project Name: {config.project_name}")
    print(f"Owner: {config.owner}")
    print()
    
    print("=== Database Configuration ===")
    print(f"Host: {config.database.host}")
    print(f"Port: {config.database.port}")
    print(f"Database: {config.database.database}")
    print(f"User: {config.database.user}")
    print(f"Connection String: {config.get_connection_string()}")
    print()
    
    print("=== Kafka Configuration ===")
    print(f"Bootstrap Servers: {config.kafka.bootstrap_servers}")
    print(f"Streaming Topic: {config.kafka.topic_streaming}")
    if config.is_production:
        print(f"Event Hubs Connection: {config.kafka.connection_string}")
    print()
    
    print("=== Storage Configuration ===")
    print(f"Raw Data Path: {config.storage.raw_path}")
    print(f"Processed Data Path: {config.storage.processed_path}")
    print(f"Analytics Data Path: {config.storage.analytics_path}")
    print()
    
    print("=== Environment Checks ===")
    print(f"Is Local Environment: {config.is_local}")
    print(f"Is Production Environment: {config.is_production}")
    print(f"Recommended Airflow Executor: {config.get_airflow_executor()}")
    print()
    
    if config.is_production:
        print("=== Production-Only Configurations ===")
        print(f"Databricks Workspace: {config.databricks.workspace_url}")
        print(f"Databricks Warehouse ID: {config.databricks.warehouse_id}")
        print()


def switch_environment_demo():
    """Demonstrate switching between environments"""
    
    print("=== Environment Switching Demo ===")
    
    # Import the Config class to create new instances
    from config import Config
    
    # Local environment
    local_config = Config('local')
    print(f"Local DB Host: {local_config.database.host}")
    print(f"Local Kafka: {local_config.kafka.bootstrap_servers}")
    print(f"Local Storage: {local_config.storage.raw_path}")
    print()
    
    # Production environment
    prod_config = Config('production')
    print(f"Production DB Host: {prod_config.database.host}")
    print(f"Production Kafka: {prod_config.kafka.bootstrap_servers}")
    print(f"Production Storage: {prod_config.storage.raw_path}")
    print()


def usage_in_application():
    """Example of how to use config in your application code"""
    
    print("=== Application Usage Examples ===")
    
    # Database connection example
    try:
        import psycopg2
        db_params = {
            'host': config.database.host,
            'port': config.database.port,
            'database': config.database.database,
            'user': config.database.user,
            'password': config.database.password
        }
        print(f"Database connection params ready: {db_params}")
        # conn = psycopg2.connect(**db_params)  # Uncomment to actually connect
    except ImportError:
        print("psycopg2 not installed, skipping database connection demo")
    
    # Kafka producer example
    kafka_config = {
        'bootstrap_servers': config.kafka.bootstrap_servers,
        'topic': config.kafka.topic_streaming
    }
    print(f"Kafka configuration: {kafka_config}")
    
    # File paths for data processing
    input_path = config.storage.raw_path
    output_path = config.storage.processed_path
    print(f"Data processing paths - Input: {input_path}, Output: {output_path}")
    
    # Environment-specific logic
    if config.is_local:
        print("Running in local mode - using simplified processing")
    elif config.is_production:
        print("Running in production mode - using full Azure stack")


if __name__ == "__main__":
    demonstrate_config_usage()
    print("\n" + "="*60 + "\n")
    switch_environment_demo()
    print("\n" + "="*60 + "\n")
    usage_in_application()