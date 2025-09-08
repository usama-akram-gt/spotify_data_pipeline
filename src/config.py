"""
Configuration management for Spotify Data Pipeline
Handles environment-specific configurations based on ENVIRONMENT variable
"""

import os
from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    """Database configuration based on environment"""
    host: str
    port: int
    database: str
    user: str
    password: str
    
    @classmethod
    def from_environment(cls, env: str = None) -> 'DatabaseConfig':
        env = env or os.getenv('ENVIRONMENT', 'local')
        
        if env == 'production':
            return cls(
                host=os.getenv('AZURE_DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', '5432')),
                database=os.getenv('DB_NAME', 'spotify_data'),
                user=os.getenv('AZURE_DB_USER', 'spotify_user'),
                password=os.getenv('AZURE_DB_PASSWORD', 'spotify_password')
            )
        else:  # local
            return cls(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', '5432')),
                database=os.getenv('DB_NAME', 'spotify_data'),
                user=os.getenv('DB_USER', 'spotify_user'),
                password=os.getenv('DB_PASSWORD', 'spotify_password')
            )


@dataclass
class KafkaConfig:
    """Kafka/Event Hubs configuration based on environment"""
    bootstrap_servers: str
    topic_streaming: str
    connection_string: str = None
    
    @classmethod
    def from_environment(cls, env: str = None) -> 'KafkaConfig':
        env = env or os.getenv('ENVIRONMENT', 'local')
        
        if env == 'production':
            return cls(
                bootstrap_servers=os.getenv('AZURE_EVENTHUBS_NAMESPACE', ''),
                topic_streaming=os.getenv('KAFKA_TOPIC_STREAMING', 'streaming-events'),
                connection_string=os.getenv('AZURE_EVENTHUBS_CONNECTION_STRING', '')
            )
        else:  # local
            return cls(
                bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
                topic_streaming=os.getenv('KAFKA_TOPIC_STREAMING', 'streaming-events')
            )


@dataclass
class StorageConfig:
    """Storage paths configuration based on environment"""
    raw_path: str
    processed_path: str
    analytics_path: str
    
    @classmethod
    def from_environment(cls, env: str = None) -> 'StorageConfig':
        env = env or os.getenv('ENVIRONMENT', 'local')
        
        if env == 'production':
            return cls(
                raw_path=os.getenv('AZURE_RAW_PATH', ''),
                processed_path=os.getenv('AZURE_PROCESSED_PATH', ''),
                analytics_path=os.getenv('AZURE_ANALYTICS_PATH', '')
            )
        else:  # local
            return cls(
                raw_path=os.getenv('RAW_DATA_PATH', './data/raw'),
                processed_path=os.getenv('PROCESSED_DATA_PATH', './data/processed'),
                analytics_path=os.getenv('ANALYTICS_DATA_PATH', './data/analytics')
            )


@dataclass
class DatabricksConfig:
    """Databricks configuration"""
    workspace_url: str
    access_token: str
    cluster_id: str = None
    warehouse_id: str = None
    
    @classmethod
    def from_environment(cls, env: str = None) -> 'DatabricksConfig':
        return cls(
            workspace_url=os.getenv('DATABRICKS_WORKSPACE_URL', ''),
            access_token=os.getenv('DATABRICKS_ACCESS_TOKEN', ''),
            cluster_id=os.getenv('DATABRICKS_CLUSTER_ID', ''),
            warehouse_id=os.getenv('DATABRICKS_WAREHOUSE_ID', '')
        )


class Config:
    """Main configuration class that provides environment-specific configs"""
    
    def __init__(self, environment: str = None):
        self.environment = environment or os.getenv('ENVIRONMENT', 'local')
        self.project_name = os.getenv('PROJECT_NAME', 'spotify-pipeline')
        self.owner = os.getenv('OWNER', 'data-engineering-team')
        
        # Initialize environment-specific configurations
        self.database = DatabaseConfig.from_environment(self.environment)
        self.kafka = KafkaConfig.from_environment(self.environment)
        self.storage = StorageConfig.from_environment(self.environment)
        self.databricks = DatabricksConfig.from_environment(self.environment)
    
    @property
    def is_local(self) -> bool:
        """Check if running in local environment"""
        return self.environment == 'local'
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.environment == 'production'
    
    def get_airflow_executor(self) -> str:
        """Get appropriate Airflow executor for environment"""
        if self.is_production:
            return os.getenv('AIRFLOW__CORE__EXECUTOR_PROD', 'CeleryExecutor')
        return os.getenv('AIRFLOW__CORE__EXECUTOR', 'LocalExecutor')
    
    def get_connection_string(self) -> str:
        """Get database connection string for environment"""
        db = self.database
        return f"postgresql+psycopg2://{db.user}:{db.password}@{db.host}:{db.port}/{db.database}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary for easy access"""
        return {
            'environment': self.environment,
            'project_name': self.project_name,
            'owner': self.owner,
            'database': {
                'host': self.database.host,
                'port': self.database.port,
                'database': self.database.database,
                'user': self.database.user,
                'password': self.database.password
            },
            'kafka': {
                'bootstrap_servers': self.kafka.bootstrap_servers,
                'topic_streaming': self.kafka.topic_streaming,
                'connection_string': self.kafka.connection_string
            },
            'storage': {
                'raw_path': self.storage.raw_path,
                'processed_path': self.storage.processed_path,
                'analytics_path': self.storage.analytics_path
            }
        }


# Global config instance
config = Config()


# Convenience functions for backward compatibility
def get_db_params() -> Dict[str, Any]:
    """Get database parameters as dictionary"""
    db = config.database
    return {
        'host': db.host,
        'port': db.port,
        'database': db.database,
        'user': db.user,
        'password': db.password
    }


def get_kafka_config() -> Dict[str, Any]:
    """Get Kafka configuration as dictionary"""
    kafka = config.kafka
    return {
        'bootstrap_servers': kafka.bootstrap_servers,
        'topic_streaming': kafka.topic_streaming,
        'connection_string': kafka.connection_string
    }


if __name__ == "__main__":
    # Example usage and testing
    print("=== Configuration Test ===")
    print(f"Environment: {config.environment}")
    print(f"Is Local: {config.is_local}")
    print(f"Is Production: {config.is_production}")
    print(f"Database Host: {config.database.host}")
    print(f"Kafka Servers: {config.kafka.bootstrap_servers}")
    print(f"Storage Raw Path: {config.storage.raw_path}")
    print(f"Airflow Executor: {config.get_airflow_executor()}")