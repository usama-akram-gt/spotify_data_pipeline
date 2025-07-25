#!/usr/bin/env python3
"""
Spotify Data Pipeline - Fake Data Generator

This script generates fake data for the Spotify data pipeline to simulate:
- Users
- Artists
- Tracks
- Streaming events
- User preferences

The data is inserted directly into the PostgreSQL database and can be scaled
to generate different volumes of data.
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
from typing import Dict, List, Optional, Tuple, Union

import psycopg2
from faker import Faker
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

# Initialize Faker
fake = Faker()

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


def generate_users(conn, count: int = 100) -> List[str]:
    """Generate fake users and insert them into the database."""
    logger.info(f"Generating {count} users...")
    
    users = []
    user_ids = []
    
    for _ in range(count):
        user_id = str(uuid.uuid4())
        username = fake.user_name()
        email = fake.email()
        country = fake.country_code()
        birthdate = fake.date_of_birth(minimum_age=13, maximum_age=70)
        gender = random.choice(["male", "female", "non-binary", "prefer not to say"])
        
        users.append((
            user_id, 
            username, 
            email, 
            country, 
            birthdate, 
            gender
        ))
        user_ids.append(user_id)
    
    try:
        cursor = conn.cursor()
        execute_batch(
            cursor,
            """
            INSERT INTO raw.users 
            (user_id, username, email, country, birthdate, gender)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING
            """,
            users
        )
        conn.commit()
        logger.info(f"Successfully inserted {len(users)} users")
        return user_ids
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error inserting users: {e}")
        return []


def generate_artists(conn, count: int = 50) -> List[str]:
    """Generate fake artists and insert them into the database."""
    logger.info(f"Generating {count} artists...")
    
    artists = []
    artist_ids = []
    
    # Define some realistic music genres
    genres = [
        "pop", "rock", "hip hop", "rap", "electronic", "dance", "r&b", "jazz", 
        "classical", "country", "folk", "alternative", "indie", "metal", 
        "reggae", "blues", "soul", "funk", "disco", "techno", "house"
    ]
    
    for _ in range(count):
        artist_id = str(uuid.uuid4())
        name = fake.name()
        genre = random.choice(genres)
        popularity = random.randint(1, 100)
        followers = random.randint(1000, 10000000)
        
        artists.append((
            artist_id,
            name,
            genre,
            popularity,
            followers
        ))
        artist_ids.append(artist_id)
    
    try:
        cursor = conn.cursor()
        execute_batch(
            cursor,
            """
            INSERT INTO raw.artists 
            (artist_id, name, genre, popularity, followers)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (artist_id) DO NOTHING
            """,
            artists
        )
        conn.commit()
        logger.info(f"Successfully inserted {len(artists)} artists")
        return artist_ids
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error inserting artists: {e}")
        return []


def generate_tracks(conn, artist_ids: List[str], count: int = 200) -> List[str]:
    """Generate fake tracks and insert them into the database."""
    logger.info(f"Generating {count} tracks...")
    
    tracks = []
    track_ids = []
    
    for _ in range(count):
        track_id = str(uuid.uuid4())
        title = fake.sentence(nb_words=4)[:-1]  # Remove period at end
        artist_id = random.choice(artist_ids)
        album = fake.sentence(nb_words=3)[:-1]  # Remove period at end
        duration_ms = random.randint(60000, 360000)  # 1-6 minutes
        explicit = random.choice([True, False])
        popularity = random.randint(1, 100)
        
        tracks.append((
            track_id,
            title,
            artist_id,
            album,
            duration_ms,
            explicit,
            popularity
        ))
        track_ids.append(track_id)
    
    try:
        cursor = conn.cursor()
        execute_batch(
            cursor,
            """
            INSERT INTO raw.tracks 
            (track_id, title, artist_id, album, duration_ms, explicit, popularity)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (track_id) DO NOTHING
            """,
            tracks
        )
        conn.commit()
        logger.info(f"Successfully inserted {len(tracks)} tracks")
        return track_ids
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error inserting tracks: {e}")
        return []


def generate_user_preferences(conn, user_ids: List[str]) -> None:
    """Generate fake user preferences and insert them into the database."""
    logger.info(f"Generating user preferences for {len(user_ids)} users...")
    
    genres = [
        "pop", "rock", "hip hop", "rap", "electronic", "dance", "r&b", "jazz", 
        "classical", "country", "folk", "alternative", "indie", "metal", 
        "reggae", "blues", "soul", "funk", "disco", "techno", "house"
    ]
    
    languages = ["en", "es", "fr", "de", "it", "pt", "ja", "ko", "zh"]
    
    preferences = []
    
    for user_id in user_ids:
        preference_id = str(uuid.uuid4())
        favorite_genre = random.choice(genres)
        preferred_language = random.choice(languages)
        
        preferences.append((
            preference_id,
            user_id,
            favorite_genre,
            preferred_language
        ))
    
    try:
        cursor = conn.cursor()
        execute_batch(
            cursor,
            """
            INSERT INTO raw.user_preferences
            (preference_id, user_id, favorite_genre, preferred_language)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (preference_id) DO NOTHING
            """,
            preferences
        )
        conn.commit()
        logger.info(f"Successfully inserted {len(preferences)} user preferences")
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error inserting user preferences: {e}")


def generate_streaming_events(
    conn, 
    user_ids: List[str], 
    track_ids: List[str], 
    count: int = 1000, 
    days_back: int = 7
) -> None:
    """Generate fake streaming events and insert them into the database."""
    logger.info(f"Generating {count} streaming events...")
    
    streaming_events = []
    
    # Platforms and user agents
    platforms = ["mobile", "desktop", "web", "tablet", "smart_speaker", "tv", "gaming_console"]
    mobile_user_agents = [
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0 Firefox/88.0",
        "Mozilla/5.0 (Linux; Android 10; SM-G960U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Mobile Safari/537.36"
    ]
    desktop_user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
    ]
    
    # Reasons for starting/ending tracks
    start_reasons = ["trackdone", "clickrow", "playbtn", "remote", "appload", "backbtn", "fwdbtn", "playbtn"]
    end_reasons = ["trackdone", "endplay", "forwarded", "backbtn", "remote", "appclose", "logout"]
    
    # Generate events spread across the last X days
    now = datetime.datetime.now()
    
    for _ in range(count):
        event_id = str(uuid.uuid4())
        user_id = random.choice(user_ids)
        track_id = random.choice(track_ids)
        
        # Generate random timestamp between now and X days ago
        random_days = random.uniform(0, days_back)
        timestamp = now - datetime.timedelta(days=random_days)
        
        ms_played = random.randint(0, 360000)  # 0-6 minutes
        reason_start = random.choice(start_reasons)
        reason_end = random.choice(end_reasons)
        shuffle = random.choice([True, False])
        skipped = random.choice([True, False])
        platform = random.choice(platforms)
        
        country = fake.country_code()
        ip_address = fake.ipv4()
        
        # Select user agent based on platform
        if platform in ["mobile", "tablet"]:
            user_agent = random.choice(mobile_user_agents)
        else:
            user_agent = random.choice(desktop_user_agents)
        
        streaming_events.append((
            event_id,
            user_id,
            track_id,
            timestamp,
            ms_played,
            reason_start,
            reason_end,
            shuffle,
            skipped,
            platform,
            country,
            ip_address,
            user_agent
        ))
    
    try:
        cursor = conn.cursor()
        execute_batch(
            cursor,
            """
            INSERT INTO raw.streaming_history
            (event_id, user_id, track_id, timestamp, ms_played, reason_start, 
             reason_end, shuffle, skipped, platform, country, ip_address, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            streaming_events,
            page_size=100
        )
        conn.commit()
        logger.info(f"Successfully inserted {len(streaming_events)} streaming events")
    except psycopg2.Error as e:
        conn.rollback()
        logger.error(f"Error inserting streaming events: {e}")


def main():
    """Main function to generate all fake data."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate fake data for Spotify data pipeline')
    parser.add_argument('--scale', choices=['small', 'medium', 'large'], default='small',
                      help='Scale of data generation (small, medium, large)')
    args = parser.parse_args()
    
    # Scale the data generation based on the command line argument
    scale_map = {
        'small': {
            'users': 100,
            'artists': 50,
            'tracks': 200,
            'events': 1000,
            'days_back': 7
        },
        'medium': {
            'users': 1000,
            'artists': 200,
            'tracks': 1000,
            'events': 10000,
            'days_back': 30
        },
        'large': {
            'users': 5000,
            'artists': 500,
            'tracks': 5000,
            'events': 100000,
            'days_back': 90
        }
    }
    
    scale = scale_map[args.scale]
    
    # Connect to the database
    conn = get_db_connection()
    
    try:
        # Generate all data
        start_time = time.time()
        
        user_ids = generate_users(conn, scale['users'])
        artist_ids = generate_artists(conn, scale['artists'])
        track_ids = generate_tracks(conn, artist_ids, scale['tracks'])
        generate_user_preferences(conn, user_ids)
        generate_streaming_events(
            conn, 
            user_ids, 
            track_ids, 
            scale['events'], 
            scale['days_back']
        )
        
        elapsed_time = time.time() - start_time
        logger.info(f"Data generation completed in {elapsed_time:.2f} seconds")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main() 