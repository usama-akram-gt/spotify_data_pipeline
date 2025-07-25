#!/usr/bin/env python3
"""
Configuration module for the Spotify data pipeline.

This module centralizes all configuration settings for the pipeline,
including database connections, Kafka settings, logging, and more.
"""

import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Environment
ENV = os.getenv("PIPELINE_ENV", "dev")

# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Database configuration
DB_CONFIG = {
    "raw": {
        "host": os.getenv("DB_RAW_HOST", "postgres"),
        "port": int(os.getenv("DB_RAW_PORT", "5432")),
        "database": os.getenv("DB_RAW_NAME", "spotify_raw"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    },
    "analytics": {
        "host": os.getenv("DB_ANALYTICS_HOST", "postgres"),
        "port": int(os.getenv("DB_ANALYTICS_PORT", "5432")),
        "database": os.getenv("DB_ANALYTICS_NAME", "spotify_analytics"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    }
}

# Kafka configuration
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:29092"),
    "group_id": os.getenv("KAFKA_GROUP_ID", "spotify-consumers"),
    "topics": {
        "play_events": os.getenv("KAFKA_TOPIC_PLAYS", "spotify.events.plays"),
        "user_events": os.getenv("KAFKA_TOPIC_USERS", "spotify.events.users"),
        "system_events": os.getenv("KAFKA_TOPIC_SYSTEM", "spotify.events.system")
    }
}

# Apache Beam configuration
BEAM_CONFIG = {
    "runner": os.getenv("BEAM_RUNNER", "DirectRunner"),
    "project": os.getenv("BEAM_PROJECT", "spotify-pipeline"),
    "temp_location": os.getenv("BEAM_TEMP_LOCATION", "/tmp/beam-temp"),
    "job_name": os.getenv("BEAM_JOB_NAME", "spotify-streaming-history"),
    "region": os.getenv("BEAM_REGION", "us-central1"),
    "max_workers": int(os.getenv("BEAM_MAX_WORKERS", "2")),
    "machine_type": os.getenv("BEAM_MACHINE_TYPE", "n1-standard-1")
}

# Data generation settings
DATA_GEN_CONFIG = {
    "scale": os.getenv("DATA_GEN_SCALE", "small"),
    "scales": {
        "small": {
            "users": 100,
            "artists": 20,
            "albums_per_artist": 3,
            "tracks_per_album": 10,
            "streaming_history_per_user": 50
        },
        "medium": {
            "users": 1000,
            "artists": 100,
            "albums_per_artist": 5,
            "tracks_per_album": 12,
            "streaming_history_per_user": 200
        },
        "large": {
            "users": 10000,
            "artists": 500,
            "albums_per_artist": 7,
            "tracks_per_album": 15,
            "streaming_history_per_user": 500
        }
    }
}

# Airflow configuration
AIRFLOW_CONFIG = {
    "dag_dir": os.getenv("AIRFLOW_DAG_DIR", "/opt/airflow/dags"),
    "dag_id": os.getenv("AIRFLOW_DAG_ID", "spotify_pipeline"),
    "schedule_interval": os.getenv("AIRFLOW_SCHEDULE", "0 0 * * *"),
    "start_date_days_ago": int(os.getenv("AIRFLOW_START_DATE_DAYS_AGO", "1")),
    "catchup": os.getenv("AIRFLOW_CATCHUP", "False").lower() == "true",
    "email_on_failure": os.getenv("AIRFLOW_EMAIL_ON_FAILURE", "False").lower() == "true",
    "email": os.getenv("AIRFLOW_EMAIL", "airflow@example.com")
}

# Schema definitions for various tables
SCHEMAS = {
    "users": [
        {"name": "user_id", "type": "VARCHAR(50)", "primary_key": True},
        {"name": "username", "type": "VARCHAR(100)", "unique": True},
        {"name": "email", "type": "VARCHAR(255)", "unique": True},
        {"name": "date_of_birth", "type": "DATE"},
        {"name": "gender", "type": "VARCHAR(20)"},
        {"name": "country", "type": "VARCHAR(2)"},
        {"name": "created_at", "type": "TIMESTAMP", "default": "CURRENT_TIMESTAMP"},
        {"name": "subscription_tier", "type": "VARCHAR(20)"}
    ],
    "artists": [
        {"name": "artist_id", "type": "VARCHAR(50)", "primary_key": True},
        {"name": "name", "type": "VARCHAR(255)"},
        {"name": "genre", "type": "VARCHAR(100)"},
        {"name": "popularity", "type": "INTEGER"},
        {"name": "founded_year", "type": "INTEGER"},
        {"name": "country", "type": "VARCHAR(2)"}
    ],
    "albums": [
        {"name": "album_id", "type": "VARCHAR(50)", "primary_key": True},
        {"name": "artist_id", "type": "VARCHAR(50)", "foreign_key": "artists(artist_id)"},
        {"name": "title", "type": "VARCHAR(255)"},
        {"name": "release_date", "type": "DATE"},
        {"name": "genre", "type": "VARCHAR(100)"},
        {"name": "total_tracks", "type": "INTEGER"}
    ],
    "tracks": [
        {"name": "track_id", "type": "VARCHAR(50)", "primary_key": True},
        {"name": "album_id", "type": "VARCHAR(50)", "foreign_key": "albums(album_id)"},
        {"name": "artist_id", "type": "VARCHAR(50)", "foreign_key": "artists(artist_id)"},
        {"name": "title", "type": "VARCHAR(255)"},
        {"name": "duration_ms", "type": "INTEGER"},
        {"name": "explicit", "type": "BOOLEAN"},
        {"name": "popularity", "type": "INTEGER"},
        {"name": "track_number", "type": "INTEGER"}
    ],
    "streaming_history": [
        {"name": "event_id", "type": "VARCHAR(50)", "primary_key": True},
        {"name": "user_id", "type": "VARCHAR(50)", "foreign_key": "users(user_id)"},
        {"name": "track_id", "type": "VARCHAR(50)", "foreign_key": "tracks(track_id)"},
        {"name": "played_at", "type": "TIMESTAMP"},
        {"name": "ms_played", "type": "INTEGER"},
        {"name": "reason_start", "type": "VARCHAR(100)"},
        {"name": "reason_end", "type": "VARCHAR(100)"},
        {"name": "device_type", "type": "VARCHAR(50)"},
        {"name": "country", "type": "VARCHAR(2)"}
    ],
    "user_listening_stats": [
        {"name": "date", "type": "DATE", "primary_key": True},
        {"name": "user_id", "type": "VARCHAR(50)", "primary_key": True, "foreign_key": "users(user_id)"},
        {"name": "tracks_played", "type": "INTEGER"},
        {"name": "total_ms_played", "type": "BIGINT"},
        {"name": "avg_track_length", "type": "INTEGER"},
        {"name": "most_played_track", "type": "VARCHAR(50)", "foreign_key": "tracks(track_id)"},
        {"name": "most_played_artist", "type": "VARCHAR(50)", "foreign_key": "artists(artist_id)"},
        {"name": "morning_plays", "type": "INTEGER"},
        {"name": "afternoon_plays", "type": "INTEGER"},
        {"name": "evening_plays", "type": "INTEGER"},
        {"name": "night_plays", "type": "INTEGER"},
        {"name": "weekday_plays", "type": "INTEGER"},
        {"name": "weekend_plays", "type": "INTEGER"}
    ],
    "track_popularity": [
        {"name": "date", "type": "DATE", "primary_key": True},
        {"name": "track_id", "type": "VARCHAR(50)", "primary_key": True, "foreign_key": "tracks(track_id)"},
        {"name": "total_streams", "type": "INTEGER"},
        {"name": "unique_listeners", "type": "INTEGER"},
        {"name": "avg_completion_rate", "type": "FLOAT"},
        {"name": "skip_rate", "type": "FLOAT"}
    ]
}

# SQL queries
SQL_QUERIES = {
    "streaming_history_extract": """
        SELECT
            event_id,
            user_id,
            track_id,
            played_at,
            ms_played,
            reason_start,
            reason_end,
            device_type,
            country
        FROM
            streaming_history
        WHERE
            played_at >= %s AND played_at < %s
    """,
    "user_streaming_patterns": """
        SELECT
            u.user_id,
            u.username,
            COUNT(sh.event_id) as play_count,
            SUM(sh.ms_played) as total_ms_played,
            AVG(sh.ms_played) as avg_ms_played,
            MAX(sh.played_at) as last_played_at
        FROM
            users u
        JOIN
            streaming_history sh ON u.user_id = sh.user_id
        WHERE
            sh.played_at >= %s AND sh.played_at < %s
        GROUP BY
            u.user_id, u.username
        ORDER BY
            play_count DESC
    """
}


def configure_logging(name: str = None, level: str = None) -> logging.Logger:
    """
    Configure and return a logger with the specified name and level.
    
    Args:
        name: Logger name (defaults to root logger if None)
        level: Log level (defaults to LOG_LEVEL env var if None)
        
    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger(name)
    level = level or LOG_LEVEL
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    logger.setLevel(level_map.get(level.upper(), logging.INFO))
    
    # Add console handler if no handlers exist
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def get_db_config(db_type: str = "raw") -> Dict[str, Any]:
    """
    Get database configuration for the specified database type.
    
    Args:
        db_type: Database type ('raw' or 'analytics')
        
    Returns:
        Dict[str, Any]: Database configuration
    """
    if db_type not in DB_CONFIG:
        raise ValueError(f"Invalid database type: {db_type}, must be one of {list(DB_CONFIG.keys())}")
    return DB_CONFIG[db_type]


def get_kafka_topic(topic_key: str) -> str:
    """
    Get Kafka topic name by key.
    
    Args:
        topic_key: Topic key ('play_events', 'user_events', 'system_events')
        
    Returns:
        str: Kafka topic name
    """
    if topic_key not in KAFKA_CONFIG["topics"]:
        raise ValueError(f"Invalid topic key: {topic_key}, must be one of {list(KAFKA_CONFIG['topics'].keys())}")
    return KAFKA_CONFIG["topics"][topic_key]


def get_data_gen_scale_config(scale: str = None) -> Dict[str, int]:
    """
    Get data generation scale configuration.
    
    Args:
        scale: Scale name ('small', 'medium', 'large')
        
    Returns:
        Dict[str, int]: Scale configuration
    """
    scale = scale or DATA_GEN_CONFIG["scale"]
    if scale not in DATA_GEN_CONFIG["scales"]:
        raise ValueError(f"Invalid scale: {scale}, must be one of {list(DATA_GEN_CONFIG['scales'].keys())}")
    return DATA_GEN_CONFIG["scales"][scale]


def get_table_schema(table_name: str) -> list:
    """
    Get schema definition for the specified table.
    
    Args:
        table_name: Table name
        
    Returns:
        list: Schema definition
    """
    if table_name not in SCHEMAS:
        raise ValueError(f"Invalid table name: {table_name}, must be one of {list(SCHEMAS.keys())}")
    return SCHEMAS[table_name]


def get_create_table_sql(table_name: str) -> str:
    """
    Generate CREATE TABLE SQL statement for the specified table.
    
    Args:
        table_name: Table name
        
    Returns:
        str: CREATE TABLE SQL statement
    """
    if table_name not in SCHEMAS:
        raise ValueError(f"Invalid table name: {table_name}, must be one of {list(SCHEMAS.keys())}")
    
    schema = SCHEMAS[table_name]
    columns = []
    primary_keys = []
    foreign_keys = []
    
    for column in schema:
        col_def = f"{column['name']} {column['type']}"
        
        if column.get('primary_key'):
            primary_keys.append(column['name'])
        
        if column.get('unique'):
            col_def += " UNIQUE"
        
        if column.get('default'):
            col_def += f" DEFAULT {column['default']}"
        
        if column.get('foreign_key'):
            foreign_keys.append(f"FOREIGN KEY ({column['name']}) REFERENCES {column['foreign_key']}")
        
        columns.append(col_def)
    
    if primary_keys:
        columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
    
    columns.extend(foreign_keys)
    
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns) + "\n);"


def get_sql_query(query_name: str) -> str:
    """
    Get SQL query by name.
    
    Args:
        query_name: Query name
        
    Returns:
        str: SQL query
    """
    if query_name not in SQL_QUERIES:
        raise ValueError(f"Invalid query name: {query_name}, must be one of {list(SQL_QUERIES.keys())}")
    return SQL_QUERIES[query_name]


def save_config_to_json(file_path: str) -> bool:
    """
    Save the current configuration to a JSON file.
    
    Args:
        file_path: Path to save the JSON file
        
    Returns:
        bool: Success status
    """
    try:
        config = {
            "env": ENV,
            "log_level": LOG_LEVEL,
            "db_config": DB_CONFIG,
            "kafka_config": KAFKA_CONFIG,
            "beam_config": BEAM_CONFIG,
            "data_gen_config": DATA_GEN_CONFIG,
            "airflow_config": AIRFLOW_CONFIG
        }
        
        with open(file_path, 'w') as f:
            json.dump(config, f, indent=2)
        return True
    except Exception as e:
        print(f"Error saving config to JSON: {e}")
        return False 