#!/usr/bin/env python3
"""
Spotify Event Streaming Producer
--------------------------------
This script simulates real-time Spotify events using Kafka.
It generates various user events (song plays, searches, etc.) and
sends them to appropriate Kafka topics.
"""

import os
import json
import uuid
import time
import random
import logging
import argparse
import psycopg2
import datetime
from typing import Dict, List, Any, Optional
from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Faker
fake = Faker()

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BROKERS', 'localhost:9092'),
    'client.id': 'spotify-events-producer'
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

# Constants for data generation (copied from generate_fake_data.py for consistency)
SUBSCRIPTION_TYPES = ['free', 'premium', 'family', 'student', 'duo']
PLATFORMS = ['android', 'ios', 'web', 'desktop_windows', 'desktop_mac', 'desktop_linux', 'smarttv', 'game_console']
DEVICE_TYPES = ['smartphone', 'tablet', 'computer', 'smart_tv', 'game_console', 'smart_speaker']
NETWORK_TYPES = ['wifi', 'cellular_4g', 'cellular_5g', 'cellular_3g', 'ethernet', 'unknown']
AUTH_TYPES = ['password', 'facebook', 'google', 'apple', 'email_link']
PLAYLIST_ACTIONS = ['create', 'add_song', 'remove_song', 'rename', 'delete', 'make_public', 'make_private']

def delivery_report(err, msg):
    """Kafka delivery callback function."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

class SpotifyStreamingProducer:
    """Class to simulate streaming Spotify events to Kafka."""
    
    def __init__(self, kafka_config: Dict[str, str], db_params: Dict[str, str]):
        """Initialize with Kafka config and PostgreSQL params."""
        self.kafka_config = kafka_config
        self.db_params = db_params
        self.producer = Producer(kafka_config)
        self.conn = None
        self.users = []
        self.songs = []
        self.playlists = []
        
    def connect_to_db(self) -> None:
        """Connect to PostgreSQL database."""
        try:
            logger.info("Connecting to PostgreSQL database...")
            self.conn = psycopg2.connect(**self.db_params)
            logger.info("Connected to PostgreSQL database successfully")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL database: {e}")
            raise
    
    def close_connection(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def load_reference_data(self) -> None:
        """Load user, song, and playlist reference data from the database."""
        try:
            logger.info("Loading reference data from database...")
            cursor = self.conn.cursor()
            
            # Load users
            cursor.execute("SELECT user_id, country, subscription_type FROM raw.users LIMIT 1000")
            self.users = [{"user_id": row[0], "country": row[1], "subscription_type": row[2]} for row in cursor.fetchall()]
            
            # Load songs
            cursor.execute("""
                SELECT s.song_id, s.duration_ms, s.artist_id, a.album_id 
                FROM raw.songs s
                JOIN raw.albums a ON s.album_id = a.album_id
                LIMIT 1000
            """)
            self.songs = [{"song_id": row[0], "duration_ms": row[1], "artist_id": row[2], "album_id": row[3]} for row in cursor.fetchall()]
            
            # Get playlist IDs (in a real system these would be stored)
            # For simplicity, we'll generate random UUIDs as playlist IDs
            self.playlists = [str(uuid.uuid4()) for _ in range(100)]
            
            cursor.close()
            logger.info(f"Loaded {len(self.users)} users, {len(self.songs)} songs, and {len(self.playlists)} playlist IDs")
            
            if not self.users or not self.songs:
                raise ValueError("No users or songs found in the database. Please run the data generation script first.")
                
        except Exception as e:
            logger.error(f"Error loading reference data: {e}")
            raise
    
    def produce_song_play_event(self) -> Dict[str, Any]:
        """Generate and produce a song play event."""
        user = random.choice(self.users)
        song = random.choice(self.songs)
        timestamp = datetime.datetime.now().isoformat()
        session_id = str(uuid.uuid4())
        platform = random.choice(PLATFORMS)
        device_type = random.choice(DEVICE_TYPES)
        network_type = random.choice(NETWORK_TYPES)
        
        # Sometimes users don't listen to the full song
        completion_rate = random.uniform(0, 1.1)  # Occasionally over 100% (repeat)
        ms_played = int(song['duration_ms'] * completion_rate)
        
        event = {
            'event_id': str(uuid.uuid4()),
            'event_type': 'song_play',
            'user_id': user['user_id'],
            'song_id': song['song_id'],
            'timestamp': timestamp,
            'session_id': session_id,
            'platform': platform,
            'ms_played': ms_played,
            'user_agent': fake.user_agent(),
            'device_type': device_type,
            'location': {
                'latitude': float(fake.latitude()),
                'longitude': float(fake.longitude()),
                'city': fake.city(),
                'country': fake.country_code()
            },
            'network_type': network_type,
            'ip_address': fake.ipv4(),
            'metadata': {
                'battery_level': random.uniform(0, 1) if platform in ['android', 'ios'] else None,
                'volume_level': random.uniform(0, 1),
                'shuffle_enabled': fake.boolean(),
                'repeat_enabled': fake.boolean(),
                'offline_mode': fake.boolean(chance_of_getting_true=10),
                'app_version': f"{random.randint(1,9)}.{random.randint(0,9)}.{random.randint(0,9)}"
            }
        }
        
        self.producer.produce(
            TOPICS['song_plays'],
            key=user['user_id'],
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        
        return event
    
    def produce_auth_event(self) -> Dict[str, Any]:
        """Generate and produce an authentication event."""
        user = random.choice(self.users)
        timestamp = datetime.datetime.now().isoformat()
        auth_type = random.choice(AUTH_TYPES)
        
        # Most authentication attempts succeed
        success = fake.boolean(chance_of_getting_true=95)
        
        event = {
            'event_id': str(uuid.uuid4()),
            'event_type': 'auth',
            'user_id': user['user_id'],
            'timestamp': timestamp,
            'auth_type': auth_type,
            'success': success,
            'ip_address': fake.ipv4(),
            'user_agent': fake.user_agent(),
            'country': fake.country_code(),
            'metadata': {
                'device_id': str(uuid.uuid4()),
                'attempt_number': random.randint(1, 3),
                'failure_reason': None if success else random.choice([
                    'incorrect_password', 'account_locked', 'suspicious_location', 
                    'expired_token', 'invalid_credentials'
                ])
            }
        }
        
        self.producer.produce(
            TOPICS['auth_events'],
            key=user['user_id'],
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        
        return event
    
    def produce_search_event(self) -> Dict[str, Any]:
        """Generate and produce a search event."""
        user = random.choice(self.users)
        timestamp = datetime.datetime.now().isoformat()
        platform = random.choice(PLATFORMS)
        
        search_types = ['artist', 'album', 'song', 'playlist', 'podcast', 'user']
        search_terms = [
            # Artists
            'Drake', 'Taylor Swift', 'Ed Sheeran', 'Ariana Grande', 'Beyoncé',
            'The Weeknd', 'Billie Eilish', 'Post Malone', 'Justin Bieber',
            # Genres
            'rock', 'pop', 'hip hop', 'country', 'electronic', 'jazz', 'classical',
            # Generic terms
            'best songs', 'top hits', 'new releases', 'workout', 'party', 'chill',
            'sleep', 'focus', 'study', 'running', 'yoga', 'meditation',
            # Specific songs
            'shape of you', 'blinding lights', 'despacito', 'uptown funk'
        ]
        
        # Sometimes users search for specific terms, sometimes more random
        if random.random() < 0.7:
            search_query = random.choice(search_terms)
        else:
            search_query = ' '.join(fake.words(nb=random.randint(1, 4)))
        
        # Random number of results
        result_count = random.randint(0, 200)
        
        event = {
            'event_id': str(uuid.uuid4()),
            'event_type': 'search',
            'user_id': user['user_id'],
            'timestamp': timestamp,
            'search_query': search_query,
            'result_count': result_count,
            'platform': platform,
            'user_agent': fake.user_agent(),
            'metadata': {
                'filter_type': random.choice(search_types),
                'result_clicked': fake.boolean() if result_count > 0 else False,
                'voice_search': fake.boolean(chance_of_getting_true=20),
                'session_id': str(uuid.uuid4()),
                'search_duration_ms': random.randint(100, 5000)
            }
        }
        
        self.producer.produce(
            TOPICS['search_events'],
            key=user['user_id'],
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        
        return event
    
    def produce_playlist_event(self) -> Dict[str, Any]:
        """Generate and produce a playlist event."""
        user = random.choice(self.users)
        playlist_id = random.choice(self.playlists)
        timestamp = datetime.datetime.now().isoformat()
        action_type = random.choice(PLAYLIST_ACTIONS)
        platform = random.choice(PLATFORMS)
        
        # Only include song_id for relevant actions
        song_id = random.choice(self.songs)['song_id'] if action_type in ['add_song', 'remove_song'] else None
        
        event = {
            'event_id': str(uuid.uuid4()),
            'event_type': 'playlist',
            'user_id': user['user_id'],
            'playlist_id': playlist_id,
            'timestamp': timestamp,
            'action_type': action_type,
            'song_id': song_id,
            'platform': platform,
            'metadata': {
                'session_id': str(uuid.uuid4()),
                'is_public_playlist': fake.boolean(),
                'playlist_follower_count': random.randint(0, 1000),
                'playlist_song_count': random.randint(1, 100)
            }
        }
        
        self.producer.produce(
            TOPICS['playlist_events'],
            key=user['user_id'],
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        
        return event
    
    def produce_error_event(self) -> Dict[str, Any]:
        """Generate and produce an error event."""
        user = random.choice(self.users)
        timestamp = datetime.datetime.now().isoformat()
        platform = random.choice(PLATFORMS)
        
        error_types = [
            'network_error', 'api_error', 'authentication_error',
            'playback_error', 'database_error', 'client_error'
        ]
        
        error_severity = random.choice(['low', 'medium', 'high', 'critical'])
        
        event = {
            'event_id': str(uuid.uuid4()),
            'event_type': 'error',
            'user_id': user['user_id'],
            'timestamp': timestamp,
            'platform': platform,
            'error_type': random.choice(error_types),
            'error_message': fake.sentence(),
            'error_severity': error_severity,
            'metadata': {
                'session_id': str(uuid.uuid4()),
                'app_version': f"{random.randint(1,9)}.{random.randint(0,9)}.{random.randint(0,9)}",
                'device_info': {
                    'os': random.choice(['iOS', 'Android', 'Windows', 'macOS', 'Linux']),
                    'os_version': f"{random.randint(10,15)}.{random.randint(0,9)}",
                    'device_model': fake.word().capitalize() + ' ' + fake.word().capitalize()
                },
                'stack_trace': '\n'.join(fake.sentences(nb=random.randint(3, 8)))
            }
        }
        
        self.producer.produce(
            TOPICS['errors'],
            key=user['user_id'],
            value=json.dumps(event).encode('utf-8'),
            callback=delivery_report
        )
        
        return event
    
    def run_simulation(self, duration_seconds: int = 300, events_per_second: float = 5.0) -> None:
        """Run the event simulation for a specified duration."""
        if not self.users or not self.songs:
            self.load_reference_data()
        
        logger.info(f"Starting simulation for {duration_seconds} seconds at {events_per_second} events/second")
        
        # Event type weights (probability of each event type)
        event_weights = {
            'song_play': 0.6,  # Most common event
            'search': 0.15,
            'auth': 0.1,
            'playlist': 0.1,
            'error': 0.05  # Least common event
        }
        
        event_generators = {
            'song_play': self.produce_song_play_event,
            'search': self.produce_search_event,
            'auth': self.produce_auth_event,
            'playlist': self.produce_playlist_event,
            'error': self.produce_error_event
        }
        
        event_types = list(event_weights.keys())
        event_probabilities = list(event_weights.values())
        
        start_time = time.time()
        event_count = 0
        
        try:
            while time.time() - start_time < duration_seconds:
                # Determine how many events to generate this second
                current_events = max(1, int(random.gauss(events_per_second, events_per_second / 4)))
                
                for _ in range(current_events):
                    # Select event type based on weights
                    event_type = random.choices(event_types, event_probabilities, k=1)[0]
                    
                    # Generate and produce the event
                    event_generators[event_type]()
                    event_count += 1
                    
                    # Flush producer periodically
                    if event_count % 100 == 0:
                        self.producer.flush(timeout=1.0)
                
                # Calculate sleep time to maintain desired rate
                elapsed = time.time() - start_time
                expected_events = elapsed * events_per_second
                if event_count < expected_events:
                    # We're falling behind, no sleep
                    continue
                else:
                    # We're ahead of schedule, sleep a bit
                    sleep_time = (event_count / events_per_second) - elapsed
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                
                # Print progress every 5 seconds
                if int(time.time() - start_time) % 5 == 0:
                    logger.info(f"Generated {event_count} events so far ({event_count / (time.time() - start_time):.2f} events/sec)")
        
        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")
        finally:
            # Flush any remaining events
            self.producer.flush(timeout=10.0)
            
            # Final stats
            total_time = time.time() - start_time
            logger.info(f"Simulation complete: {event_count} events in {total_time:.2f} seconds " +
                        f"({event_count / total_time:.2f} events/sec)")

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Simulate streaming Spotify events to Kafka.')
    parser.add_argument(
        '--duration', 
        type=int, 
        default=300,
        help='Duration of simulation in seconds (default: 300)'
    )
    parser.add_argument(
        '--rate', 
        type=float, 
        default=5.0,
        help='Average events per second (default: 5.0)'
    )
    args = parser.parse_args()
    
    try:
        producer = SpotifyStreamingProducer(KAFKA_CONFIG, DB_PARAMS)
        producer.connect_to_db()
        producer.run_simulation(duration_seconds=args.duration, events_per_second=args.rate)
    except Exception as e:
        logger.error(f"Streaming simulation failed: {e}")
        exit(1)
    finally:
        if hasattr(producer, 'conn') and producer.conn:
            producer.close_connection()

if __name__ == "__main__":
    main() 