#!/usr/bin/env python3
"""
Spotify Data Generator

This script generates synthetic Spotify streaming data for testing the data pipeline.
It creates users, artists, albums, tracks, playlists, and streaming history events.
"""

import os
import sys
import random
import argparse
import uuid
import logging
import datetime
from datetime import timedelta
import time
import json
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_generator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'spotify_data')
DB_USER = os.getenv('DB_USER', 'spotify_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'spotify_password')


class SpotifyDataGenerator:
    """Generate synthetic Spotify data for testing and development."""

    def __init__(self):
        """Initialize the data generator with configuration."""
        self.fake = Faker()
        self.conn = None
        self.cursor = None
        self.user_ids = []
        self.artist_ids = []
        self.album_ids = []
        self.track_ids = []
        self.playlist_ids = []
        
        # Scales for data generation
        self.scales = {
            'tiny': {
                'users': 10,
                'artists': 5,
                'albums_per_artist': 1,
                'tracks_per_album': 5,
                'playlists_per_user': 1,
                'tracks_per_playlist': 5,
                'streaming_events_per_user': 20
            },
            'small': {
                'users': 50,
                'artists': 20,
                'albums_per_artist': 2,
                'tracks_per_album': 10,
                'playlists_per_user': 2,
                'tracks_per_playlist': 10,
                'streaming_events_per_user': 100
            },
            'medium': {
                'users': 200,
                'artists': 50,
                'albums_per_artist': 3,
                'tracks_per_album': 12,
                'playlists_per_user': 5,
                'tracks_per_playlist': 15,
                'streaming_events_per_user': 500
            },
            'large': {
                'users': 1000,
                'artists': 100,
                'albums_per_artist': 5,
                'tracks_per_album': 15,
                'playlists_per_user': 10,
                'tracks_per_playlist': 20,
                'streaming_events_per_user': 1000
            }
        }
        
        # Music genres
        self.genres = [
            "Pop", "Rock", "Hip Hop", "R&B", "Electronic", "Country", 
            "Jazz", "Classical", "Blues", "Reggae", "Folk", "Metal", 
            "Punk", "Soul", "Funk", "Disco", "House", "Techno", 
            "Ambient", "Indie", "Alternative", "Grunge", "K-pop"
        ]
        
        # Subscription types
        self.subscription_types = [
            "Free", "Premium", "Family", "Student", "Duo"
        ]
        
        # Devices
        self.devices = [
            "iPhone", "Android Phone", "iPad", "Android Tablet", 
            "Windows Desktop", "Mac Desktop", "Web Browser", 
            "Smart Speaker", "Smart TV", "Car System"
        ]
        
        # Reason for starting playback
        self.reasons_start = [
            "trackdone", "clickrow", "playbtn", "remote", "back", 
            "appload", "forward", "fwdbtn"
        ]
        
        # Reason for ending playback
        self.reasons_end = [
            "trackdone", "endplay", "logout", "forward", "back", 
            "appclose", "fwdbtn", "popup", "backbtn"
        ]

    def connect_to_db(self):
        """Connect to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            self.cursor = self.conn.cursor()
            logger.info("Connected to the database successfully.")
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            sys.exit(1)

    def close_db_connection(self):
        """Close the PostgreSQL database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.commit()
            self.conn.close()
            logger.info("Database connection closed.")

    def generate_users(self, count):
        """Generate random users."""
        logger.info(f"Generating {count} users...")
        users = []
        
        for _ in range(count):
            user_id = str(uuid.uuid4())
            username = self.fake.user_name()
            email = self.fake.email()
            country = self.fake.country_code()
            joined_at = self.fake.date_time_between(start_date='-5y', end_date='now')
            birth_year = random.randint(1960, 2005)
            gender = random.choice(['Male', 'Female', 'Other', None])
            subscription_type = random.choice(self.subscription_types)
            is_active = random.random() > 0.1  # 90% chance of being active
            last_active = self.fake.date_time_between(start_date='-30d', end_date='now') if is_active else None
            
            users.append((
                user_id, username, email, country, joined_at, 
                birth_year, gender, subscription_type, is_active, last_active
            ))
            self.user_ids.append(user_id)
        
        # Insert users into the database
        query = """
        INSERT INTO raw.users 
        (user_id, username, email, country, joined_at, birth_year, gender, subscription_type, is_active, last_active) 
        VALUES %s
        """
        execute_values(self.cursor, query, users)
        self.conn.commit()
        logger.info(f"Generated {count} users successfully.")

    def generate_artists(self, count):
        """Generate random artists."""
        logger.info(f"Generating {count} artists...")
        artists = []
        
        for _ in range(count):
            artist_id = str(uuid.uuid4())
            name = self.fake.name()
            popularity = random.randint(1, 100)
            # Assign 1-3 genres to each artist
            artist_genres = [random.choice(self.genres) for _ in range(random.randint(1, 3))]
            followers = random.randint(100, 10000000)
            external_url = f"https://spotify.com/artists/{artist_id}"
            
            artists.append((
                artist_id, name, popularity, artist_genres, followers, external_url
            ))
            self.artist_ids.append(artist_id)
        
        # Insert artists into the database
        query = """
        INSERT INTO raw.artists 
        (artist_id, name, popularity, genres, followers, external_url) 
        VALUES %s
        """
        execute_values(self.cursor, query, artists)
        self.conn.commit()
        logger.info(f"Generated {count} artists successfully.")

    def generate_albums(self, count_per_artist):
        """Generate random albums for each artist."""
        logger.info(f"Generating albums ({count_per_artist} per artist)...")
        albums = []
        
        for artist_id in self.artist_ids:
            for _ in range(count_per_artist):
                album_id = str(uuid.uuid4())
                name = self.fake.sentence(nb_words=random.randint(1, 5)).strip('.')
                release_date = self.fake.date_between(start_date='-20y', end_date='today')
                total_tracks = random.randint(5, 20)
                album_type = random.choice(['album', 'single', 'compilation', 'EP'])
                popularity = random.randint(1, 100)
                # Get artist's genres for the album
                self.cursor.execute("SELECT genres FROM raw.artists WHERE artist_id = %s", (artist_id,))
                result = self.cursor.fetchone()
                album_genres = result[0] if result and result[0] else []
                
                albums.append((
                    album_id, name, artist_id, release_date, total_tracks, album_type, popularity, album_genres
                ))
                self.album_ids.append(album_id)
        
        # Insert albums into the database
        query = """
        INSERT INTO raw.albums 
        (album_id, name, artist_id, release_date, total_tracks, album_type, popularity, genres) 
        VALUES %s
        """
        execute_values(self.cursor, query, albums)
        self.conn.commit()
        logger.info(f"Generated {len(albums)} albums successfully.")

    def generate_tracks(self, count_per_album):
        """Generate random tracks for each album."""
        logger.info(f"Generating tracks ({count_per_album} per album)...")
        tracks = []
        
        for album_id in self.album_ids:
            # Get album info
            self.cursor.execute("""
                SELECT artist_id, total_tracks 
                FROM raw.albums 
                WHERE album_id = %s
            """, (album_id,))
            album_info = self.cursor.fetchone()
            artist_id = album_info[0]
            total_tracks = min(album_info[1], count_per_album)  # Respect album's total_tracks
            
            for track_number in range(1, total_tracks + 1):
                track_id = str(uuid.uuid4())
                name = self.fake.sentence(nb_words=random.randint(1, 7)).strip('.')
                duration_ms = random.randint(30000, 600000)  # 30 seconds to 10 minutes
                explicit = random.random() < 0.2  # 20% chance of being explicit
                popularity = random.randint(1, 100)
                
                # Audio features
                danceability = random.random()
                energy = random.random()
                key = random.randint(-1, 11)
                loudness = random.uniform(-20.0, 0.0)
                mode = random.randint(0, 1)
                speechiness = random.random()
                acousticness = random.random()
                instrumentalness = random.random()
                liveness = random.random()
                valence = random.random()
                tempo = random.uniform(60.0, 200.0)
                time_signature = random.choice([3, 4, 5])
                
                tracks.append((
                    track_id, name, album_id, artist_id, duration_ms, explicit, track_number,
                    popularity, danceability, energy, key, loudness, mode, speechiness,
                    acousticness, instrumentalness, liveness, valence, tempo, time_signature
                ))
                self.track_ids.append(track_id)
        
        # Insert tracks into the database
        query = """
        INSERT INTO raw.tracks 
        (track_id, name, album_id, artist_id, duration_ms, explicit, track_number,
        popularity, danceability, energy, key, loudness, mode, speechiness,
        acousticness, instrumentalness, liveness, valence, tempo, time_signature) 
        VALUES %s
        """
        execute_values(self.cursor, query, tracks)
        self.conn.commit()
        logger.info(f"Generated {len(tracks)} tracks successfully.")
        
    def generate_playlists(self, count_per_user):
        """Generate random playlists for each user."""
        logger.info(f"Generating playlists ({count_per_user} per user)...")
        playlists = []
        
        for user_id in self.user_ids:
            for _ in range(count_per_user):
                playlist_id = str(uuid.uuid4())
                name = self.fake.sentence(nb_words=random.randint(1, 5)).strip('.')
                description = self.fake.text(max_nb_chars=100) if random.random() > 0.3 else None
                is_public = random.random() > 0.2  # 80% chance of being public
                created_at = self.fake.date_time_between(start_date='-2y', end_date='now')
                last_modified = self.fake.date_time_between(start_date=created_at, end_date='now')
                follower_count = random.randint(0, 1000)
                
                # We'll update track_count after adding tracks to the playlist
                playlists.append((
                    playlist_id, name, user_id, description, is_public, 
                    created_at, last_modified, follower_count, 0
                ))
                self.playlist_ids.append(playlist_id)
        
        # Insert playlists into the database
        query = """
        INSERT INTO raw.playlists 
        (playlist_id, name, user_id, description, is_public, 
        created_at, last_modified, follower_count, track_count) 
        VALUES %s
        """
        execute_values(self.cursor, query, playlists)
        self.conn.commit()
        logger.info(f"Generated {len(playlists)} playlists successfully.")
    
    def generate_playlist_tracks(self, count_per_playlist):
        """Generate tracks for each playlist."""
        logger.info(f"Adding tracks to playlists ({count_per_playlist} per playlist)...")
        playlist_tracks = []
        
        for playlist_id in self.playlist_ids:
            # Get playlist creator and creation date
            self.cursor.execute("""
                SELECT user_id, created_at 
                FROM raw.playlists 
                WHERE playlist_id = %s
            """, (playlist_id,))
            playlist_info = self.cursor.fetchone()
            user_id = playlist_info[0]
            created_at = playlist_info[1]
            
            # Select random tracks for the playlist
            selected_tracks = random.sample(self.track_ids, min(count_per_playlist, len(self.track_ids)))
            track_count = len(selected_tracks)
            
            # Add tracks to the playlist with positions
            for position, track_id in enumerate(selected_tracks):
                added_at = self.fake.date_time_between(start_date=created_at, end_date='now')
                
                playlist_tracks.append((
                    playlist_id, track_id, added_at, user_id, position
                ))
            
            # Update track count in the playlist
            self.cursor.execute("""
                UPDATE raw.playlists 
                SET track_count = %s 
                WHERE playlist_id = %s
            """, (track_count, playlist_id))
        
        # Insert playlist tracks into the database
        if playlist_tracks:
            query = """
            INSERT INTO raw.playlist_tracks 
            (playlist_id, track_id, added_at, added_by, position) 
            VALUES %s
            """
            execute_values(self.cursor, query, playlist_tracks)
            self.conn.commit()
            logger.info(f"Added {len(playlist_tracks)} tracks to playlists successfully.")
    
    def generate_streaming_history(self, events_per_user):
        """Generate random streaming history events for each user."""
        logger.info(f"Generating streaming history ({events_per_user} per user)...")
        streaming_history = []
        batch_size = 5000  # Process in batches to avoid memory issues
        total_count = 0
        
        # Generate a realistic timestamp distribution
        # Users are more likely to listen to music in the evening and weekends
        def generate_weighted_timestamp(days_back):
            # Random day in the past
            date = datetime.datetime.now() - timedelta(days=random.randint(0, days_back))
            
            # Weight hour probabilities (higher in morning and evening)
            hour_weights = [
                0.01, 0.01, 0.01, 0.01, 0.02, 0.05,  # 0-5 AM
                0.08, 0.10, 0.09, 0.07, 0.06, 0.07,  # 6-11 AM
                0.07, 0.06, 0.06, 0.07, 0.08, 0.10,  # 12-5 PM
                0.12, 0.13, 0.11, 0.08, 0.05, 0.02   # 6-11 PM
            ]
            hour = random.choices(range(24), weights=hour_weights)[0]
            
            # Build timestamp
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            return date.replace(hour=hour, minute=minute, second=second)
        
        # Create streaming sessions (a group of consecutive tracks)
        def generate_session(user_id, start_time, num_tracks):
            session_id = str(uuid.uuid4())
            device = random.choice(self.devices)
            shuffle = random.random() > 0.5
            offline = random.random() > 0.8
            incognito_mode = random.random() > 0.9
            
            events = []
            current_time = start_time
            
            # First track in session
            reason_start = "appload"
            
            for i in range(num_tracks):
                track_id = random.choice(self.track_ids)
                
                # Get track duration
                self.cursor.execute("SELECT duration_ms FROM raw.tracks WHERE track_id = %s", (track_id,))
                track_duration = self.cursor.fetchone()[0]
                
                # Determine if skipped
                skipped = random.random() < 0.2  # 20% chance of skipping
                
                # Calculate ms_played based on whether it was skipped
                if skipped:
                    ms_played = int(track_duration * random.uniform(0.1, 0.8))
                    reason_end = random.choice(["forward", "fwdbtn"])
                else:
                    # Some variation in completion even for non-skipped tracks
                    ms_played = int(track_duration * random.uniform(0.95, 1.0))
                    reason_end = "trackdone"
                
                events.append((
                    user_id, track_id, current_time, ms_played, session_id,
                    device, reason_start, reason_end, shuffle, skipped,
                    offline, incognito_mode
                ))
                
                # Update time for next track
                current_time += timedelta(milliseconds=ms_played + random.randint(0, 3000))
                
                # Next track's reason for starting
                if i < num_tracks - 1:
                    reason_start = "trackdone" if not skipped else random.choice(["clickrow", "remote"])
                
            return events
        
        for user_id in self.user_ids:
            # Generate multiple listening sessions for each user
            remaining_events = events_per_user
            days_back = 90  # Generate events over the last 90 days
            
            while remaining_events > 0:
                # Session length - varies but tends to be around 10-20 tracks
                session_length = min(remaining_events, max(1, int(random.normalvariate(15, 5))))
                start_time = generate_weighted_timestamp(days_back)
                
                session_events = generate_session(user_id, start_time, session_length)
                streaming_history.extend(session_events)
                
                remaining_events -= session_length
                
                # Insert in batches to avoid memory issues
                if len(streaming_history) >= batch_size:
                    query = """
                    INSERT INTO raw.streaming_history 
                    (user_id, track_id, played_at, ms_played, session_id,
                    device, reason_start, reason_end, shuffle, skipped,
                    offline, incognito_mode) 
                    VALUES %s
                    """
                    execute_values(self.cursor, query, streaming_history)
                    self.conn.commit()
                    total_count += len(streaming_history)
                    logger.info(f"Inserted batch of {len(streaming_history)} streaming events. Total: {total_count}")
                    streaming_history = []
        
        # Insert any remaining events
        if streaming_history:
            query = """
            INSERT INTO raw.streaming_history 
            (user_id, track_id, played_at, ms_played, session_id,
            device, reason_start, reason_end, shuffle, skipped,
            offline, incognito_mode) 
            VALUES %s
            """
            execute_values(self.cursor, query, streaming_history)
            self.conn.commit()
            total_count += len(streaming_history)
        
        logger.info(f"Generated {total_count} streaming history events successfully.")
    
    def generate_all_data(self, scale='small'):
        """Generate all data types based on the specified scale."""
        if scale not in self.scales:
            logger.error(f"Invalid scale: {scale}. Available scales: {list(self.scales.keys())}")
            return
        
        config = self.scales[scale]
        logger.info(f"Generating data at {scale} scale: {config}")
        
        try:
            self.connect_to_db()
            
            # Generate data in the proper order to maintain referential integrity
            self.generate_users(config['users'])
            self.generate_artists(config['artists'])
            self.generate_albums(config['albums_per_artist'])
            self.generate_tracks(config['tracks_per_album'])
            self.generate_playlists(config['playlists_per_user'])
            self.generate_playlist_tracks(config['tracks_per_playlist'])
            self.generate_streaming_history(config['streaming_events_per_user'])
            
            logger.info(f"Successfully generated all data at {scale} scale.")
        except Exception as e:
            logger.error(f"Error generating data: {e}")
            if self.conn:
                self.conn.rollback()
        finally:
            self.close_db_connection()


def main():
    """Main entry point for the data generator."""
    parser = argparse.ArgumentParser(description="Generate synthetic Spotify data")
    parser.add_argument('--scale', type=str, default='small',
                        choices=['tiny', 'small', 'medium', 'large'],
                        help='Scale of data generation (default: small)')
    args = parser.parse_args()
    
    generator = SpotifyDataGenerator()
    generator.generate_all_data(args.scale)


if __name__ == "__main__":
    main() 