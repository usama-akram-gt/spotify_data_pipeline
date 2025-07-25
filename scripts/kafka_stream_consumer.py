#!/usr/bin/env python3
"""
Spotify Data Pipeline - Kafka Stream Consumer

This script consumes streaming events from Kafka topics and processes them in real-time.
It performs the following tasks:
- Consumes events from play, user, and system event topics
- Processes the events in real-time
- Writes the events to the PostgreSQL database
- Calculates real-time metrics and statistics

Usage:
  python kafka_stream_consumer.py --group-id consumer1
"""

import argparse
import datetime
import json
import logging
import os
import sys
import time
from typing import Dict, List, Optional, Any

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
KAFKA_TOPICS = [
    "spotify.events.plays",
    "spotify.events.users",
    "spotify.events.system"
]
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "spotify-consumers")

# Database connection parameters
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "spotify_raw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


def get_db_connection():
    """Create a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        sys.exit(1)


def get_kafka_consumer(group_id: str = KAFKA_GROUP_ID):
    """Create and return a Kafka consumer."""
    try:
        consumer_conf = {
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        consumer = Consumer(consumer_conf)
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        sys.exit(1)


def process_play_event(conn, event: Dict[str, Any]) -> None:
    """Process a play event and store it in the database."""
    try:
        cursor = conn.cursor()
        
        # Insert streaming history record
        cursor.execute(
            """
            INSERT INTO raw.streaming_history
            (event_id, user_id, track_id, timestamp, ms_played, reason_start, 
             reason_end, shuffle, skipped, platform, country, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event['event_id'],
                event['user_id'],
                event['track_id'],
                event['timestamp'],
                event['ms_played'],
                event['reason_start'],
                event['reason_end'],
                event['shuffle'],
                event['skipped'],
                event['platform'],
                event['country'],
                event['ip_address'],
                event.get('user_agent', 'Unknown')
            )
        )
        
        # Update real-time metrics
        # For demonstration, we'll update a real-time metrics table
        # In a production system, this might be stored in Redis or another in-memory database
        cursor.execute(
            """
            INSERT INTO raw.real_time_metrics
            (metric_type, entity_id, count, last_updated)
            VALUES (%s, %s, 1, NOW())
            ON CONFLICT (metric_type, entity_id) 
            DO UPDATE SET 
                count = raw.real_time_metrics.count + 1,
                last_updated = NOW()
            """,
            ("track_play", event['track_id'])
        )
        
        # Update user activity
        cursor.execute(
            """
            INSERT INTO raw.real_time_metrics
            (metric_type, entity_id, count, last_updated)
            VALUES (%s, %s, 1, NOW())
            ON CONFLICT (metric_type, entity_id) 
            DO UPDATE SET 
                count = raw.real_time_metrics.count + 1,
                last_updated = NOW()
            """,
            ("user_activity", event['user_id'])
        )
        
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error processing play event: {e}")


def process_user_event(conn, event: Dict[str, Any]) -> None:
    """Process a user event and store it in the database."""
    try:
        cursor = conn.cursor()
        
        # For user events, we'll just update the real-time metrics
        cursor.execute(
            """
            INSERT INTO raw.real_time_metrics
            (metric_type, entity_id, metadata, count, last_updated)
            VALUES (%s, %s, %s, 1, NOW())
            ON CONFLICT (metric_type, entity_id) 
            DO UPDATE SET 
                count = raw.real_time_metrics.count + 1,
                metadata = %s,
                last_updated = NOW()
            """,
            (
                f"user_{event['event_type']}", 
                event['user_id'],
                json.dumps(event),
                json.dumps(event)
            )
        )
        
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error processing user event: {e}")


def process_system_event(conn, event: Dict[str, Any]) -> None:
    """Process a system event and store it in the database."""
    try:
        cursor = conn.cursor()
        
        # For system events, we'll update the real-time metrics
        cursor.execute(
            """
            INSERT INTO raw.real_time_metrics
            (metric_type, entity_id, metadata, count, last_updated)
            VALUES (%s, %s, %s, 1, NOW())
            ON CONFLICT (metric_type, entity_id) 
            DO UPDATE SET 
                count = raw.real_time_metrics.count + 1,
                metadata = %s,
                last_updated = NOW()
            """,
            (
                f"system_{event['event_type']}", 
                event.get('service', 'unknown'),
                json.dumps(event),
                json.dumps(event)
            )
        )
        
        # If it's an error event, log it separately
        if event['event_type'] == 'error':
            cursor.execute(
                """
                INSERT INTO raw.system_errors
                (error_id, service, error_code, error_message, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (error_id) DO NOTHING
                """,
                (
                    event['event_id'],
                    event.get('service', 'unknown'),
                    event.get('error_code', 'unknown'),
                    event.get('error_message', ''),
                    event['timestamp']
                )
            )
        
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error processing system event: {e}")


def ensure_tables_exist(conn):
    """Create necessary tables if they don't exist."""
    try:
        cursor = conn.cursor()
        
        # Create real-time metrics table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.real_time_metrics (
                metric_type VARCHAR(50),
                entity_id VARCHAR(50),
                count BIGINT DEFAULT 0,
                metadata JSONB,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (metric_type, entity_id)
            )
            """
        )
        
        # Create system errors table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.system_errors (
                error_id VARCHAR(50) PRIMARY KEY,
                service VARCHAR(50),
                error_code VARCHAR(50),
                error_message TEXT,
                timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error creating tables: {e}")


def process_message(conn, message):
    """Process a Kafka message based on the topic."""
    try:
        topic = message.topic()
        event = json.loads(message.value().decode('utf-8'))
        
        if topic == "spotify.events.plays":
            process_play_event(conn, event)
        elif topic == "spotify.events.users":
            process_user_event(conn, event)
        elif topic == "spotify.events.system":
            process_system_event(conn, event)
        else:
            logger.warning(f"Unknown topic: {topic}")
        
        return True
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON: {e}")
        return False
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return False


def consume_events(consumer, conn, batch_size: int = 100, max_poll_interval: int = 30):
    """Consume events from Kafka topics and process them."""
    try:
        # Subscribe to topics
        consumer.subscribe(KAFKA_TOPICS)
        logger.info(f"Subscribed to topics: {KAFKA_TOPICS}")
        
        # Ensure necessary tables exist
        ensure_tables_exist(conn)
        
        # Counters
        messages_processed = 0
        last_commit_time = time.time()
        
        # Main processing loop
        while True:
            try:
                # Poll for messages
                messages = consumer.consume(batch_size, timeout=1.0)
                
                if not messages:
                    # If no messages, check if it's time to commit offsets
                    if time.time() - last_commit_time > max_poll_interval:
                        consumer.commit()
                        last_commit_time = time.time()
                    continue
                
                # Process messages
                for message in messages:
                    if message is None:
                        continue
                    
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event, not an error
                            logger.debug(f"Reached end of partition: {message.topic()}/{message.partition()}")
                        else:
                            # Actual error
                            logger.error(f"Error consuming message: {message.error()}")
                        continue
                    
                    # Process the message
                    if process_message(conn, message):
                        messages_processed += 1
                
                # Commit offsets periodically
                if time.time() - last_commit_time > max_poll_interval:
                    consumer.commit()
                    last_commit_time = time.time()
                    logger.info(f"Processed {messages_processed} messages so far")
            
            except KafkaException as e:
                logger.error(f"Kafka error: {e}")
                time.sleep(1)  # Wait before retrying
            
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(1)  # Wait before retrying
    
    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    
    finally:
        # Commit final offsets
        try:
            consumer.commit()
        except Exception as e:
            logger.error(f"Error committing final offsets: {e}")
        
        # Close consumer
        consumer.close()
        logger.info(f"Consumer closed. Processed {messages_processed} messages in total.")


def main():
    """Main function to consume events from Kafka."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Consume events from Kafka')
    parser.add_argument('--group-id', type=str, default=KAFKA_GROUP_ID,
                        help='Kafka consumer group ID')
    args = parser.parse_args()
    
    # Create Kafka consumer
    consumer = get_kafka_consumer(args.group_id)
    
    # Create database connection
    conn = get_db_connection()
    
    try:
        # Consume and process events
        consume_events(consumer, conn)
    
    finally:
        conn.close()


if __name__ == "__main__":
    main() 