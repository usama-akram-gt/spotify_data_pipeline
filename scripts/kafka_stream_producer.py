#!/usr/bin/env python3
"""
Spotify Data Pipeline - Kafka Stream Producer

This script generates and streams events to Kafka topics to simulate real-time
user activity on a Spotify-like platform. It produces events for:
- Song plays
- User activity
- System events

Usage:
  python kafka_stream_producer.py --duration 300 --rate 10
"""

import argparse
import datetime
import json
import logging
import os
import random
import sys
import time
import uuid
from typing import Dict, List, Optional, Union

import psycopg2
from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker

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

# Initialize Faker
fake = Faker()

# Kafka configuration
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:29092")
KAFKA_TOPICS = {
    "play_events": "spotify.events.plays",
    "user_events": "spotify.events.users",
    "system_events": "spotify.events.system"
}

# Database connection parameters
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "spotify_raw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


def kafka_delivery_callback(err, msg):
    """Callback for Kafka message delivery."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def get_kafka_producer():
    """Create and return a Kafka producer."""
    try:
        producer_conf = {
            'bootstrap.servers': KAFKA_BROKERS,
            'client.id': 'spotify-stream-producer'
        }
        producer = Producer(producer_conf)
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        sys.exit(1)


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


def fetch_user_ids(conn) -> List[str]:
    """Fetch user IDs from the database."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM raw.users")
        return [row[0] for row in cursor.fetchall()]
    except psycopg2.Error as e:
        logger.error(f"Error fetching user IDs: {e}")
        return []


def fetch_track_ids(conn) -> List[str]:
    """Fetch track IDs from the database."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT track_id FROM raw.tracks")
        return [row[0] for row in cursor.fetchall()]
    except psycopg2.Error as e:
        logger.error(f"Error fetching track IDs: {e}")
        return []


def generate_play_event(user_ids: List[str], track_ids: List[str]) -> Dict:
    """Generate a play event."""
    user_id = random.choice(user_ids)
    track_id = random.choice(track_ids)
    
    # Platforms and reasons
    platforms = ["mobile", "desktop", "web", "tablet", "smart_speaker", "tv", "gaming_console"]
    start_reasons = ["trackdone", "clickrow", "playbtn", "remote", "appload", "backbtn", "fwdbtn", "playbtn"]
    end_reasons = ["trackdone", "endplay", "forwarded", "backbtn", "remote", "appclose", "logout"]
    
    now = datetime.datetime.now()
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "play",
        "user_id": user_id,
        "track_id": track_id,
        "timestamp": now.isoformat(),
        "ms_played": random.randint(0, 360000),  # 0-6 minutes
        "reason_start": random.choice(start_reasons),
        "reason_end": random.choice(end_reasons),
        "shuffle": random.choice([True, False]),
        "skipped": random.choice([True, False]),
        "platform": random.choice(platforms),
        "country": fake.country_code(),
        "ip_address": fake.ipv4()
    }
    
    return event


def generate_user_event(user_ids: List[str]) -> Dict:
    """Generate a user event."""
    user_id = random.choice(user_ids)
    
    event_types = [
        "login", "logout", "search", "view_artist", "view_album", 
        "create_playlist", "update_playlist", "follow_artist", "like_song"
    ]
    
    event_type = random.choice(event_types)
    now = datetime.datetime.now()
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "timestamp": now.isoformat(),
        "platform": random.choice(["mobile", "desktop", "web"]),
        "country": fake.country_code(),
        "ip_address": fake.ipv4()
    }
    
    # Add event-specific data
    if event_type == "search":
        event["search_query"] = fake.sentence(nb_words=3)[:-1]
    elif event_type in ["view_artist", "follow_artist"]:
        event["artist_id"] = str(uuid.uuid4())
    elif event_type == "view_album":
        event["album_id"] = str(uuid.uuid4())
    elif event_type in ["create_playlist", "update_playlist"]:
        event["playlist_id"] = str(uuid.uuid4())
        event["playlist_name"] = fake.sentence(nb_words=2)[:-1]
    elif event_type == "like_song":
        event["track_id"] = str(uuid.uuid4())
    
    return event


def generate_system_event() -> Dict:
    """Generate a system event."""
    event_types = [
        "api_request", "error", "cache_hit", "cache_miss", 
        "recommendation_generated", "playlist_generated"
    ]
    
    event_type = random.choice(event_types)
    now = datetime.datetime.now()
    
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": now.isoformat(),
        "service": random.choice(["api", "recommendation", "auth", "storage", "search"]),
        "status": random.choice(["success", "warning", "error"]) if event_type == "error" else "success",
        "duration_ms": random.randint(1, 5000)  # 1-5000 ms
    }
    
    # Add event-specific data
    if event_type == "api_request":
        event["endpoint"] = random.choice(["/api/tracks", "/api/artists", "/api/users", "/api/playlists"])
        event["method"] = random.choice(["GET", "POST", "PUT", "DELETE"])
        event["status_code"] = random.choice([200, 201, 400, 404, 500])
    elif event_type == "error":
        event["error_code"] = random.choice(["AUTH_FAILED", "RATE_LIMIT", "NOT_FOUND", "SERVER_ERROR"])
        event["error_message"] = fake.sentence()
    elif event_type in ["recommendation_generated", "playlist_generated"]:
        event["user_id"] = str(uuid.uuid4())
        event["items_count"] = random.randint(1, 50)
    
    return event


def stream_events(producer, user_ids: List[str], track_ids: List[str], 
                 duration: int = 300, rate: int = 10) -> None:
    """Stream events to Kafka topics for a specified duration and rate."""
    event_generators = [
        (generate_play_event, KAFKA_TOPICS["play_events"], 0.7),  # 70% play events
        (generate_user_event, KAFKA_TOPICS["user_events"], 0.2),  # 20% user events
        (generate_system_event, KAFKA_TOPICS["system_events"], 0.1),  # 10% system events
    ]
    
    # Calculate delay between events based on rate
    delay = 1.0 / rate
    
    logger.info(f"Starting event streaming for {duration} seconds at {rate} events/second")
    
    end_time = time.time() + duration
    events_produced = 0
    
    try:
        while time.time() < end_time:
            # Choose event type based on probability
            rand = random.random()
            cumulative_prob = 0
            
            for generator, topic, prob in event_generators:
                cumulative_prob += prob
                if rand <= cumulative_prob:
                    if generator == generate_play_event:
                        event = generator(user_ids, track_ids)
                    elif generator == generate_user_event:
                        event = generator(user_ids)
                    else:
                        event = generator()
                    
                    # Convert event to JSON and produce to Kafka
                    event_json = json.dumps(event)
                    producer.produce(topic, value=event_json.encode('utf-8'), 
                                    callback=kafka_delivery_callback)
                    break
            
            # Ensure messages are sent
            producer.poll(0)
            
            events_produced += 1
            if events_produced % 100 == 0:
                logger.info(f"Produced {events_produced} events")
            
            # Sleep to maintain the rate
            time.sleep(delay)
    
    except KeyboardInterrupt:
        logger.info("Event streaming interrupted")
    
    finally:
        # Flush any remaining messages
        producer.flush()
        logger.info(f"Event streaming completed. Produced {events_produced} events.")


def main():
    """Main function to stream events to Kafka."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Stream events to Kafka')
    parser.add_argument('--duration', type=int, default=300,
                        help='Duration of streaming in seconds')
    parser.add_argument('--rate', type=int, default=10,
                        help='Event rate per second')
    args = parser.parse_args()
    
    # Create Kafka producer
    producer = get_kafka_producer()
    
    # Create database connection
    conn = get_db_connection()
    
    try:
        # Fetch user and track IDs from the database
        user_ids = fetch_user_ids(conn)
        track_ids = fetch_track_ids(conn)
        
        if not user_ids or not track_ids:
            logger.error("No users or tracks found in the database")
            return
        
        # Stream events
        stream_events(producer, user_ids, track_ids, args.duration, args.rate)
    
    finally:
        conn.close()


if __name__ == "__main__":
    main() 