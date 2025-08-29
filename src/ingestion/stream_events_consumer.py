#!/usr/bin/env python3
"""
Spotify Events Kafka Consumer
----------------------------
This script consumes streaming Spotify events from Kafka and processes them,
storing the data in PostgreSQL and performing real-time analytics.
"""

import os
import json
import time
import logging
import argparse
import psycopg2
import threading
from typing import Dict, List, Any, Optional, Callable
from confluent_kafka import Consumer, KafkaError, KafkaException
from datetime import datetime, timedelta
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BROKERS', 'localhost:9092'),
    'group.id': 'spotify-events-consumer',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'spotify_data'),
    'user': os.getenv('DB_USER', 'spotify_user'),
    'password': os.getenv('DB_PASSWORD', 'spotify_password')
}

# Kafka topics
TOPICS = {
    'song_plays': 'spotify-song-plays',
    'auth_events': 'spotify-auth-events',
    'search_events': 'spotify-search-events',
    'playlist_events': 'spotify-playlist-events',
    'recommendations': 'spotify-recommendations',
    'errors': 'spotify-error-events'
}

class SpotifyEventsConsumer:
    """Class for consuming and processing Spotify events from Kafka."""
    
    def __init__(self, kafka_config: Dict[str, str], db_params: Dict[str, str], topics: List[str]):
        """Initialize with Kafka config, DB params, and topics to consume."""
        self.kafka_config = kafka_config
        self.db_params = db_params
        self.topics = topics
        self.consumer = Consumer(kafka_config)
        self.conn = None
        self.running = False
        self.commit_interval = 1000  # Commit after processing 1000 messages
        self.message_count = 0
        self.last_commit_time = time.time()
        
        # Real-time analytics counters
        self.realtime_metrics = {
            'song_plays_count': 0,
            'unique_users': set(),
            'unique_songs': set(),
            'search_queries': {},
            'errors_by_type': {},
            'events_by_platform': {},
            'start_time': datetime.now()
        }
        
        # Batch processing buffers
        self.song_plays_buffer = []
        self.auth_events_buffer = []
        self.search_events_buffer = []
        self.playlist_events_buffer = []
        self.error_events_buffer = []
        self.buffer_size = 100  # Flush buffer to DB when it reaches this size
    
    def connect_to_db(self) -> None:
        """Connect to PostgreSQL database."""
        try:
            logger.info("Connecting to PostgreSQL database...")
            self.conn = psycopg2.connect(**self.db_params)
            self.conn.autocommit = False  # Manual transaction control
            logger.info("Connected to PostgreSQL database successfully")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL database: {e}")
            raise
    
    def close_connection(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def subscribe_to_topics(self) -> None:
        """Subscribe to the specified Kafka topics."""
        try:
            logger.info(f"Subscribing to topics: {self.topics}")
            self.consumer.subscribe(self.topics)
        except KafkaException as e:
            logger.error(f"Error subscribing to topics: {e}")
            raise
    
    def process_song_play(self, event: Dict[str, Any]) -> None:
        """Process a song play event."""
        # Extract the required fields from the event
        try:
            # Update real-time metrics
            self.realtime_metrics['song_plays_count'] += 1
            self.realtime_metrics['unique_users'].add(event['user_id'])
            self.realtime_metrics['unique_songs'].add(event['song_id'])
            
            platform = event.get('platform', 'unknown')
            if platform in self.realtime_metrics['events_by_platform']:
                self.realtime_metrics['events_by_platform'][platform] += 1
            else:
                self.realtime_metrics['events_by_platform'][platform] = 1
            
            # Format the event for database insertion
            location_json = json.dumps(event.get('location', {}))
            metadata_json = json.dumps(event.get('metadata', {}))
            
            db_record = {
                'event_id': event.get('event_id'),
                'user_id': event.get('user_id'),
                'song_id': event.get('song_id'),
                'timestamp': datetime.fromisoformat(event.get('timestamp')) if 'timestamp' in event else datetime.now(),
                'session_id': event.get('session_id'),
                'platform': platform,
                'ms_played': event.get('ms_played', 0),
                'user_agent': event.get('user_agent', ''),
                'device_type': event.get('device_type', ''),
                'location': location_json,
                'network_type': event.get('network_type', ''),
                'ip_address': event.get('ip_address', ''),
                'raw_data': json.dumps(event)
            }
            
            # Add to buffer for batch insertion
            self.song_plays_buffer.append(db_record)
            
            # Flush buffer if it reaches the threshold
            if len(self.song_plays_buffer) >= self.buffer_size:
                self.flush_song_plays_buffer()
                
        except Exception as e:
            logger.error(f"Error processing song play event: {e}")
    
    def process_auth_event(self, event: Dict[str, Any]) -> None:
        """Process an authentication event."""
        try:
            # Update real-time metrics
            self.realtime_metrics['unique_users'].add(event['user_id'])
            
            platform = event.get('platform', 'unknown')
            if platform in self.realtime_metrics['events_by_platform']:
                self.realtime_metrics['events_by_platform'][platform] += 1
            else:
                self.realtime_metrics['events_by_platform'][platform] = 1
            
            # Format the event for database insertion
            db_record = {
                'event_id': event.get('event_id'),
                'user_id': event.get('user_id'),
                'timestamp': datetime.fromisoformat(event.get('timestamp')) if 'timestamp' in event else datetime.now(),
                'auth_type': event.get('auth_type', ''),
                'success': event.get('success', False),
                'ip_address': event.get('ip_address', ''),
                'user_agent': event.get('user_agent', ''),
                'country': event.get('country', ''),
                'raw_data': json.dumps(event)
            }
            
            # Add to buffer for batch insertion
            self.auth_events_buffer.append(db_record)
            
            # Flush buffer if it reaches the threshold
            if len(self.auth_events_buffer) >= self.buffer_size:
                self.flush_auth_events_buffer()
                
        except Exception as e:
            logger.error(f"Error processing auth event: {e}")
    
    def process_search_event(self, event: Dict[str, Any]) -> None:
        """Process a search event."""
        try:
            # Update real-time metrics
            self.realtime_metrics['unique_users'].add(event['user_id'])
            
            search_query = event.get('search_query', '').lower()
            if search_query in self.realtime_metrics['search_queries']:
                self.realtime_metrics['search_queries'][search_query] += 1
            else:
                self.realtime_metrics['search_queries'][search_query] = 1
            
            platform = event.get('platform', 'unknown')
            if platform in self.realtime_metrics['events_by_platform']:
                self.realtime_metrics['events_by_platform'][platform] += 1
            else:
                self.realtime_metrics['events_by_platform'][platform] = 1
            
            # Format the event for database insertion
            db_record = {
                'event_id': event.get('event_id'),
                'user_id': event.get('user_id'),
                'timestamp': datetime.fromisoformat(event.get('timestamp')) if 'timestamp' in event else datetime.now(),
                'search_query': search_query,
                'result_count': event.get('result_count', 0),
                'platform': platform,
                'user_agent': event.get('user_agent', ''),
                'raw_data': json.dumps(event)
            }
            
            # Add to buffer for batch insertion
            self.search_events_buffer.append(db_record)
            
            # Flush buffer if it reaches the threshold
            if len(self.search_events_buffer) >= self.buffer_size:
                self.flush_search_events_buffer()
                
        except Exception as e:
            logger.error(f"Error processing search event: {e}")
    
    def process_playlist_event(self, event: Dict[str, Any]) -> None:
        """Process a playlist event."""
        try:
            # Update real-time metrics
            self.realtime_metrics['unique_users'].add(event['user_id'])
            
            platform = event.get('platform', 'unknown')
            if platform in self.realtime_metrics['events_by_platform']:
                self.realtime_metrics['events_by_platform'][platform] += 1
            else:
                self.realtime_metrics['events_by_platform'][platform] = 1
            
            # Format the event for database insertion
            db_record = {
                'event_id': event.get('event_id'),
                'user_id': event.get('user_id'),
                'playlist_id': event.get('playlist_id'),
                'timestamp': datetime.fromisoformat(event.get('timestamp')) if 'timestamp' in event else datetime.now(),
                'action_type': event.get('action_type', ''),
                'song_id': event.get('song_id'),
                'platform': platform,
                'raw_data': json.dumps(event)
            }
            
            # Add to buffer for batch insertion
            self.playlist_events_buffer.append(db_record)
            
            # Flush buffer if it reaches the threshold
            if len(self.playlist_events_buffer) >= self.buffer_size:
                self.flush_playlist_events_buffer()
                
        except Exception as e:
            logger.error(f"Error processing playlist event: {e}")
    
    def process_error_event(self, event: Dict[str, Any]) -> None:
        """Process an error event."""
        try:
            # Update real-time metrics
            error_type = event.get('error_type', 'unknown')
            if error_type in self.realtime_metrics['errors_by_type']:
                self.realtime_metrics['errors_by_type'][error_type] += 1
            else:
                self.realtime_metrics['errors_by_type'][error_type] = 1
            
            platform = event.get('platform', 'unknown')
            if platform in self.realtime_metrics['events_by_platform']:
                self.realtime_metrics['events_by_platform'][platform] += 1
            else:
                self.realtime_metrics['events_by_platform'][platform] = 1
            
            # For simplicity, we'll just log error events, but they could be stored in a separate table
            logger.warning(f"Error event received: {error_type} - {event.get('error_message', '')}")
            
            # Add to buffer for potential batch insertion (for this example, we're not storing these)
            self.error_events_buffer.append(event)
            
        except Exception as e:
            logger.error(f"Error processing error event: {e}")
    
    def flush_song_plays_buffer(self) -> None:
        """Flush the song plays buffer to the database."""
        if not self.song_plays_buffer:
            return
        
        try:
            cursor = self.conn.cursor()
            
            # Define column names for the song plays table
            columns = [
                'event_id', 'user_id', 'song_id', 'timestamp', 'session_id', 
                'platform', 'ms_played', 'user_agent', 'device_type', 'location',
                'network_type', 'ip_address', 'raw_data'
            ]
            
            # Extract values in the same order as columns
            values = [
                [
                    record.get(column) for column in columns
                ] for record in self.song_plays_buffer
            ]
            
            # Batch insert
            execute_values(
                cursor,
                f"INSERT INTO raw.song_plays ({', '.join(columns)}) VALUES %s",
                values,
                template=None
            )
            
            self.conn.commit()
            logger.info(f"Inserted {len(self.song_plays_buffer)} song play events into database")
            
            # Clear the buffer
            self.song_plays_buffer = []
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error flushing song plays buffer: {e}")
    
    def flush_auth_events_buffer(self) -> None:
        """Flush the auth events buffer to the database."""
        if not self.auth_events_buffer:
            return
        
        try:
            cursor = self.conn.cursor()
            
            # Define column names for the auth events table
            columns = [
                'event_id', 'user_id', 'timestamp', 'auth_type', 'success',
                'ip_address', 'user_agent', 'country', 'raw_data'
            ]
            
            # Extract values in the same order as columns
            values = [
                [
                    record.get(column) for column in columns
                ] for record in self.auth_events_buffer
            ]
            
            # Batch insert
            execute_values(
                cursor,
                f"INSERT INTO raw.auth_events ({', '.join(columns)}) VALUES %s",
                values,
                template=None
            )
            
            self.conn.commit()
            logger.info(f"Inserted {len(self.auth_events_buffer)} auth events into database")
            
            # Clear the buffer
            self.auth_events_buffer = []
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error flushing auth events buffer: {e}")
    
    def flush_search_events_buffer(self) -> None:
        """Flush the search events buffer to the database."""
        if not self.search_events_buffer:
            return
        
        try:
            cursor = self.conn.cursor()
            
            # Define column names for the search events table
            columns = [
                'event_id', 'user_id', 'timestamp', 'search_query', 'result_count',
                'platform', 'user_agent', 'raw_data'
            ]
            
            # Extract values in the same order as columns
            values = [
                [
                    record.get(column) for column in columns
                ] for record in self.search_events_buffer
            ]
            
            # Batch insert
            execute_values(
                cursor,
                f"INSERT INTO raw.search_events ({', '.join(columns)}) VALUES %s",
                values,
                template=None
            )
            
            self.conn.commit()
            logger.info(f"Inserted {len(self.search_events_buffer)} search events into database")
            
            # Clear the buffer
            self.search_events_buffer = []
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error flushing search events buffer: {e}")
    
    def flush_playlist_events_buffer(self) -> None:
        """Flush the playlist events buffer to the database."""
        if not self.playlist_events_buffer:
            return
        
        try:
            cursor = self.conn.cursor()
            
            # Define column names for the playlist events table
            columns = [
                'event_id', 'user_id', 'playlist_id', 'timestamp', 'action_type',
                'song_id', 'platform', 'raw_data'
            ]
            
            # Extract values in the same order as columns
            values = [
                [
                    record.get(column) for column in columns
                ] for record in self.playlist_events_buffer
            ]
            
            # Batch insert
            execute_values(
                cursor,
                f"INSERT INTO raw.playlist_events ({', '.join(columns)}) VALUES %s",
                values,
                template=None
            )
            
            self.conn.commit()
            logger.info(f"Inserted {len(self.playlist_events_buffer)} playlist events into database")
            
            # Clear the buffer
            self.playlist_events_buffer = []
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error flushing playlist events buffer: {e}")
    
    def flush_all_buffers(self) -> None:
        """Flush all event buffers to the database."""
        self.flush_song_plays_buffer()
        self.flush_auth_events_buffer()
        self.flush_search_events_buffer()
        self.flush_playlist_events_buffer()
    
    def print_metrics(self) -> None:
        """Print real-time metrics periodically."""
        if not self.running:
            return
        
        try:
            current_time = datetime.now()
            elapsed_seconds = (current_time - self.realtime_metrics['start_time']).total_seconds()
            
            # Top-level metrics
            logger.info("----- Real-time Spotify Streaming Metrics -----")
            logger.info(f"Time window: {self.realtime_metrics['start_time']} to {current_time} ({elapsed_seconds:.1f} seconds)")
            logger.info(f"Total song plays: {self.realtime_metrics['song_plays_count']}")
            logger.info(f"Unique users: {len(self.realtime_metrics['unique_users'])}")
            logger.info(f"Unique songs: {len(self.realtime_metrics['unique_songs'])}")
            
            # Platform breakdown
            logger.info("Events by platform:")
            for platform, count in sorted(self.realtime_metrics['events_by_platform'].items(), key=lambda x: x[1], reverse=True)[:5]:
                logger.info(f"  {platform}: {count}")
            
            # Top search queries
            logger.info("Top search queries:")
            for query, count in sorted(self.realtime_metrics['search_queries'].items(), key=lambda x: x[1], reverse=True)[:5]:
                logger.info(f"  '{query}': {count}")
            
            # Error types
            if self.realtime_metrics['errors_by_type']:
                logger.info("Errors by type:")
                for error_type, count in sorted(self.realtime_metrics['errors_by_type'].items(), key=lambda x: x[1], reverse=True):
                    logger.info(f"  {error_type}: {count}")
            
            logger.info("---------------------------------------------")
            
            # Schedule the next metrics printing
            metrics_thread = threading.Timer(30.0, self.print_metrics)
            metrics_thread.daemon = True
            metrics_thread.start()
            
        except Exception as e:
            logger.error(f"Error printing metrics: {e}")
    
    def process_message(self, message: Any) -> None:
        """Process a message from Kafka."""
        try:
            # Parse the message value as JSON
            event_data = json.loads(message.value().decode('utf-8'))
            
            # Determine event type and route to appropriate handler
            event_type = event_data.get('event_type', '')
            
            if event_type == 'song_play':
                self.process_song_play(event_data)
            elif event_type == 'auth':
                self.process_auth_event(event_data)
            elif event_type == 'search':
                self.process_search_event(event_data)
            elif event_type == 'playlist':
                self.process_playlist_event(event_data)
            elif event_type == 'error':
                self.process_error_event(event_data)
            else:
                logger.warning(f"Unknown event type: {event_type}")
            
            # Increment message count
            self.message_count += 1
            
            # Commit offsets periodically
            current_time = time.time()
            if (self.message_count % self.commit_interval == 0 or 
                current_time - self.last_commit_time >= 5):  # Also commit every 5 seconds
                self.consumer.commit(asynchronous=False)
                self.last_commit_time = current_time
                
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def run(self, timeout: float = 1.0) -> None:
        """Run the consumer loop."""
        try:
            self.connect_to_db()
            self.subscribe_to_topics()
            self.running = True
            
            # Start the metrics printing thread
            self.print_metrics()
            
            logger.info(f"Starting to consume messages from topics: {self.topics}")
            
            while self.running:
                message = self.consumer.poll(timeout=timeout)
                
                if message is None:
                    continue
                
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event - not an error
                        logger.debug(f"Reached end of partition {message.partition()}")
                    else:
                        logger.error(f"Error while consuming message: {message.error()}")
                else:
                    self.process_message(message)
                    
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
        finally:
            self.running = False
            # Final flush of all buffers
            self.flush_all_buffers()
            # Final commit
            self.consumer.commit()
            # Close consumer
            self.consumer.close()
            # Close DB connection
            self.close_connection()
            logger.info("Consumer shutdown complete")

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Consume and process Spotify events from Kafka.')
    parser.add_argument(
        '--topics', 
        type=str, 
        nargs='+',
        default=[TOPICS['song_plays'], TOPICS['auth_events'], TOPICS['search_events'], TOPICS['playlist_events'], TOPICS['errors']],
        help='Kafka topics to consume from (default: all event topics)'
    )
    args = parser.parse_args()
    
    try:
        consumer = SpotifyEventsConsumer(KAFKA_CONFIG, DB_PARAMS, args.topics)
        consumer.run()
    except Exception as e:
        logger.error(f"Consumer execution failed: {e}")
        exit(1)

if __name__ == "__main__":
    main() 