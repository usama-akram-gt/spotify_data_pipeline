#!/usr/bin/env python3
"""
Spotify Data Generator
---------------------
This script generates fake Spotify data for testing the data pipeline.
It creates users, artists, albums, songs, and various events that simulate 
a real Spotify environment.
"""

import os
import json
import uuid
import random
import datetime
import psycopg2
import logging
from dotenv import load_dotenv
from faker import Faker
from typing import Dict, List, Any, Tuple
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configure Faker
fake = Faker()
Faker.seed(42)  # For reproducibility

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'spotify_data'),
    'user': os.getenv('DB_USER', 'spotify_user'),
    'password': os.getenv('DB_PASSWORD', 'spotify_password')
}

# Constants for data generation
SUBSCRIPTION_TYPES = ['free', 'premium', 'family', 'student', 'duo']
PLATFORMS = ['android', 'ios', 'web', 'desktop_windows', 'desktop_mac', 'desktop_linux', 'smarttv', 'game_console']
DEVICE_TYPES = ['smartphone', 'tablet', 'computer', 'smart_tv', 'game_console', 'smart_speaker']
NETWORK_TYPES = ['wifi', 'cellular_4g', 'cellular_5g', 'cellular_3g', 'ethernet', 'unknown']
GENRES = [
    'rock', 'pop', 'hip-hop', 'rap', 'electronic', 'classical', 'jazz', 'country', 
    'r&b', 'soul', 'blues', 'metal', 'punk', 'indie', 'folk', 'reggae', 'ambient',
    'dance', 'latin', 'world', 'alternative', 'funk', 'disco'
]
ALBUM_TYPES = ['album', 'single', 'compilation', 'ep']
AUTH_TYPES = ['password', 'facebook', 'google', 'apple', 'email_link']
PLAYLIST_ACTIONS = ['create', 'add_song', 'remove_song', 'rename', 'delete', 'make_public', 'make_private']

class SpotifyDataGenerator:
    """Class to generate fake Spotify data."""
    
    def __init__(self, db_params: Dict[str, str]):
        """Initialize the generator with database parameters."""
        self.db_params = db_params
        self.conn = None
        self.users = []
        self.artists = []
        self.albums = []
        self.songs = []
        self.playlists = []
    
    def connect_to_db(self) -> None:
        """Connect to the PostgreSQL database."""
        try:
            logger.info("Connecting to PostgreSQL database...")
            self.conn = psycopg2.connect(**self.db_params)
            logger.info("Connected to PostgreSQL database successfully")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL database: {e}")
            raise
    
    def close_connection(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def generate_users(self, count: int) -> List[Dict[str, Any]]:
        """Generate fake user data."""
        logger.info(f"Generating {count} users...")
        users = []
        
        for _ in range(count):
            user_id = str(uuid.uuid4())
            birth_date = fake.date_of_birth(minimum_age=13, maximum_age=80)
            registration_date = fake.date_time_between(start_date='-5y', end_date='now')
            
            user = {
                'user_id': user_id,
                'username': fake.user_name(),
                'email': fake.email(),
                'country': fake.country_code(),
                'registration_date': registration_date,
                'birth_date': birth_date,
                'gender': random.choice(['male', 'female', 'non-binary', 'prefer_not_to_say']),
                'subscription_type': random.choice(SUBSCRIPTION_TYPES),
                'raw_data': json.dumps({
                    'preferred_language': fake.language_code(),
                    'marketing_consent': fake.boolean(),
                    'app_version': f"{random.randint(1,9)}.{random.randint(0,9)}.{random.randint(0,9)}",
                    'first_login_platform': random.choice(PLATFORMS)
                })
            }
            users.append(user)
        
        self.users = users
        logger.info(f"Generated {len(users)} users")
        return users
    
    def generate_artists(self, count: int) -> List[Dict[str, Any]]:
        """Generate fake artist data."""
        logger.info(f"Generating {count} artists...")
        artists = []
        
        for _ in range(count):
            artist_id = str(uuid.uuid4())
            num_genres = random.randint(1, 3)
            artist_genres = random.sample(GENRES, num_genres)
            popularity = random.randint(1, 100)
            followers = int(fake.random_number(digits=6))
            
            artist = {
                'artist_id': artist_id,
                'artist_name': fake.name(),
                'genres': json.dumps(artist_genres),
                'popularity': popularity,
                'followers': followers,
                'raw_data': json.dumps({
                    'bio': fake.paragraph(),
                    'verified': fake.boolean(chance_of_getting_true=30),
                    'country_of_origin': fake.country(),
                    'formed_year': random.randint(1950, 2022)
                })
            }
            artists.append(artist)
        
        self.artists = artists
        logger.info(f"Generated {len(artists)} artists")
        return artists
    
    def generate_albums(self, count: int) -> List[Dict[str, Any]]:
        """Generate fake album data."""
        if not self.artists:
            raise ValueError("No artists generated. Please generate artists first.")
        
        logger.info(f"Generating {count} albums...")
        albums = []
        
        for _ in range(count):
            album_id = str(uuid.uuid4())
            artist = random.choice(self.artists)
            release_date = fake.date_between(start_date='-30y', end_date='today')
            album_type = random.choice(ALBUM_TYPES)
            total_tracks = random.randint(1, 20) if album_type != 'single' else random.randint(1, 3)
            
            album = {
                'album_id': album_id,
                'album_name': ' '.join(fake.words(nb=random.randint(1, 5))).title(),
                'artist_id': artist['artist_id'],
                'release_date': release_date,
                'album_type': album_type,
                'total_tracks': total_tracks,
                'raw_data': json.dumps({
                    'label': fake.company(),
                    'copyright': f"© {release_date.year} {fake.company()}",
                    'cover_art_url': fake.image_url(),
                    'upc': fake.ean(length=13)
                })
            }
            albums.append(album)
        
        self.albums = albums
        logger.info(f"Generated {len(albums)} albums")
        return albums
    
    def generate_songs(self, count: int) -> List[Dict[str, Any]]:
        """Generate fake song data."""
        if not self.albums:
            raise ValueError("No albums generated. Please generate albums first.")
        
        logger.info(f"Generating {count} songs...")
        songs = []
        
        # Ensure each album has songs
        album_song_counts = {album['album_id']: 0 for album in self.albums}
        
        for _ in range(count):
            song_id = str(uuid.uuid4())
            
            # Select an album that hasn't reached its track limit
            valid_albums = [album for album in self.albums if album_song_counts[album['album_id']] < album['total_tracks']]
            
            if not valid_albums:
                break
            
            album = random.choice(valid_albums)
            album_song_counts[album['album_id']] += 1
            track_number = album_song_counts[album['album_id']]
            
            # Find the artist of this album
            artist_id = album['artist_id']
            
            duration_ms = random.randint(60000, 420000)  # 1 to 7 minutes
            popularity = random.randint(1, 100)
            
            song = {
                'song_id': song_id,
                'song_name': ' '.join(fake.words(nb=random.randint(1, 6))).title(),
                'album_id': album['album_id'],
                'artist_id': artist_id,
                'duration_ms': duration_ms,
                'explicit': fake.boolean(chance_of_getting_true=20),
                'track_number': track_number,
                'popularity': popularity,
                'raw_data': json.dumps({
                    'isrc': ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=12)),
                    'composers': [fake.name() for _ in range(random.randint(1, 3))],
                    'producers': [fake.name() for _ in range(random.randint(1, 2))],
                    'bpm': random.randint(60, 180),
                    'key': random.choice(['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']),
                    'energy': random.uniform(0, 1),
                    'danceability': random.uniform(0, 1)
                })
            }
            songs.append(song)
        
        self.songs = songs
        logger.info(f"Generated {len(songs)} songs")
        return songs
    
    def generate_song_plays(self, count: int) -> List[Dict[str, Any]]:
        """Generate fake song play events."""
        if not self.users or not self.songs:
            raise ValueError("No users or songs generated. Please generate users and songs first.")
        
        logger.info(f"Generating {count} song play events...")
        song_plays = []
        
        for _ in range(count):
            event_id = str(uuid.uuid4())
            user = random.choice(self.users)
            song = random.choice(self.songs)
            timestamp = fake.date_time_between(start_date='-1y', end_date='now')
            session_id = str(uuid.uuid4())
            platform = random.choice(PLATFORMS)
            device_type = random.choice(DEVICE_TYPES)
            network_type = random.choice(NETWORK_TYPES)
            
            # Sometimes users don't listen to the full song
            completion_rate = random.uniform(0, 1.1)  # Occasionally over 100% (repeat)
            ms_played = int(song['duration_ms'] * completion_rate)
            
            song_play = {
                'event_id': event_id,
                'user_id': user['user_id'],
                'song_id': song['song_id'],
                'timestamp': timestamp,
                'session_id': session_id,
                'platform': platform,
                'ms_played': ms_played,
                'user_agent': fake.user_agent(),
                'device_type': device_type,
                'location': json.dumps({
                    'latitude': float(fake.latitude()),
                    'longitude': float(fake.longitude()),
                    'city': fake.city(),
                    'country': fake.country_code()
                }),
                'network_type': network_type,
                'ip_address': fake.ipv4(),
                'raw_data': json.dumps({
                    'battery_level': random.uniform(0, 1) if platform in ['android', 'ios'] else None,
                    'volume_level': random.uniform(0, 1),
                    'shuffle_enabled': fake.boolean(),
                    'repeat_enabled': fake.boolean(),
                    'offline_mode': fake.boolean(chance_of_getting_true=10),
                    'app_version': f"{random.randint(1,9)}.{random.randint(0,9)}.{random.randint(0,9)}"
                })
            }
            song_plays.append(song_play)
        
        logger.info(f"Generated {len(song_plays)} song play events")
        return song_plays
    
    def generate_auth_events(self, count: int) -> List[Dict[str, Any]]:
        """Generate fake authentication events."""
        if not self.users:
            raise ValueError("No users generated. Please generate users first.")
        
        logger.info(f"Generating {count} authentication events...")
        auth_events = []
        
        for _ in range(count):
            event_id = str(uuid.uuid4())
            user = random.choice(self.users)
            timestamp = fake.date_time_between(start_date='-6m', end_date='now')
            auth_type = random.choice(AUTH_TYPES)
            
            # Most authentication attempts succeed
            success = fake.boolean(chance_of_getting_true=95)
            
            auth_event = {
                'event_id': event_id,
                'user_id': user['user_id'],
                'timestamp': timestamp,
                'auth_type': auth_type,
                'success': success,
                'ip_address': fake.ipv4(),
                'user_agent': fake.user_agent(),
                'country': fake.country_code(),
                'raw_data': json.dumps({
                    'device_id': str(uuid.uuid4()),
                    'attempt_number': random.randint(1, 3),
                    'failure_reason': None if success else random.choice([
                        'incorrect_password', 'account_locked', 'suspicious_location', 
                        'expired_token', 'invalid_credentials'
                    ])
                })
            }
            auth_events.append(auth_event)
        
        logger.info(f"Generated {len(auth_events)} authentication events")
        return auth_events
    
    def generate_search_events(self, count: int) -> List[Dict[str, Any]]:
        """Generate fake search events."""
        if not self.users:
            raise ValueError("No users generated. Please generate users first.")
        
        logger.info(f"Generating {count} search events...")
        search_events = []
        
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
        
        for _ in range(count):
            event_id = str(uuid.uuid4())
            user = random.choice(self.users)
            timestamp = fake.date_time_between(start_date='-3m', end_date='now')
            
            # Sometimes users search for specific terms, sometimes more random
            if random.random() < 0.7:
                search_query = random.choice(search_terms)
            else:
                search_query = ' '.join(fake.words(nb=random.randint(1, 4)))
            
            # Random number of results
            result_count = random.randint(0, 200)
            platform = random.choice(PLATFORMS)
            
            search_event = {
                'event_id': event_id,
                'user_id': user['user_id'],
                'timestamp': timestamp,
                'search_query': search_query,
                'result_count': result_count,
                'platform': platform,
                'user_agent': fake.user_agent(),
                'raw_data': json.dumps({
                    'filter_type': random.choice(search_types),
                    'result_clicked': fake.boolean() if result_count > 0 else False,
                    'voice_search': fake.boolean(chance_of_getting_true=20),
                    'session_id': str(uuid.uuid4()),
                    'search_duration_ms': random.randint(100, 5000)
                })
            }
            search_events.append(search_event)
        
        logger.info(f"Generated {len(search_events)} search events")
        return search_events
    
    def generate_playlists(self, count: int) -> List[Dict[str, str]]:
        """Generate fake playlist data (not stored in DB, just for reference)."""
        if not self.users:
            raise ValueError("No users generated. Please generate users first.")
        
        logger.info(f"Generating {count} playlists...")
        self.playlists = []
        
        for _ in range(count):
            playlist_id = str(uuid.uuid4())
            user = random.choice(self.users)
            
            playlist = {
                'playlist_id': playlist_id,
                'user_id': user['user_id'],
                'name': ' '.join(fake.words(nb=random.randint(1, 5))).title()
            }
            self.playlists.append(playlist)
        
        logger.info(f"Generated {len(self.playlists)} playlists")
        return self.playlists
    
    def generate_playlist_events(self, count: int) -> List[Dict[str, Any]]:
        """Generate fake playlist events."""
        if not self.users or not self.songs:
            raise ValueError("No users or songs generated. Please generate users and songs first.")
        
        if not self.playlists:
            self.generate_playlists(count // 10)  # Generate some playlists if none exist
        
        logger.info(f"Generating {count} playlist events...")
        playlist_events = []
        
        for _ in range(count):
            event_id = str(uuid.uuid4())
            playlist = random.choice(self.playlists)
            user_id = playlist['user_id']
            playlist_id = playlist['playlist_id']
            
            timestamp = fake.date_time_between(start_date='-1y', end_date='now')
            action_type = random.choice(PLAYLIST_ACTIONS)
            
            # Only include song_id for relevant actions
            song_id = random.choice(self.songs)['song_id'] if action_type in ['add_song', 'remove_song'] else None
            
            platform = random.choice(PLATFORMS)
            
            playlist_event = {
                'event_id': event_id,
                'user_id': user_id,
                'playlist_id': playlist_id,
                'timestamp': timestamp,
                'action_type': action_type,
                'song_id': song_id,
                'platform': platform,
                'raw_data': json.dumps({
                    'session_id': str(uuid.uuid4()),
                    'is_public_playlist': fake.boolean(),
                    'playlist_follower_count': random.randint(0, 1000),
                    'playlist_song_count': random.randint(1, 100)
                })
            }
            playlist_events.append(playlist_event)
        
        logger.info(f"Generated {len(playlist_events)} playlist events")
        return playlist_events
    
    def insert_data(self, table_name: str, data: List[Dict[str, Any]]) -> int:
        """Insert data into a specified table."""
        if not data:
            logger.warning(f"No data to insert into {table_name}")
            return 0
        
        cursor = self.conn.cursor()
        inserted_count = 0
        
        # Get column names from the first data item (all items have the same structure)
        columns = list(data[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        # Prepare batch insert query
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        # Insert in batches of 1000
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            batch_values = []
            
            for item in batch:
                row_values = [item[column] for column in columns]
                batch_values.append(row_values)
            
            try:
                cursor.executemany(query, batch_values)
                self.conn.commit()
                inserted_count += len(batch)
                logger.info(f"Inserted {len(batch)} records into {table_name}")
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Error inserting batch into {table_name}: {e}")
                raise
        
        cursor.close()
        return inserted_count
    
    def run_full_data_generation(self, scale: str = 'medium') -> Dict[str, int]:
        """Run the full data generation process based on scale."""
        # Define data volume based on scale
        scales = {
            'small': {
                'users': 100,
                'artists': 50,
                'albums': 100,
                'songs': 500,
                'song_plays': 1000,
                'auth_events': 500,
                'search_events': 500,
                'playlist_events': 500
            },
            'medium': {
                'users': 1000,
                'artists': 200,
                'albums': 500,
                'songs': 2000,
                'song_plays': 10000,
                'auth_events': 5000,
                'search_events': 5000,
                'playlist_events': 3000
            },
            'large': {
                'users': 10000,
                'artists': 1000,
                'albums': 2000,
                'songs': 20000,
                'song_plays': 100000,
                'auth_events': 50000,
                'search_events': 50000,
                'playlist_events': 30000
            }
        }
        
        if scale not in scales:
            raise ValueError(f"Invalid scale: {scale}. Must be one of: {list(scales.keys())}")
        
        counts = scales[scale]
        results = {}
        
        try:
            # Connect to database
            self.connect_to_db()
            
            # Generate and insert users
            users = self.generate_users(counts['users'])
            results['users'] = self.insert_data('raw.users', users)
            
            # Generate and insert artists
            artists = self.generate_artists(counts['artists'])
            results['artists'] = self.insert_data('raw.artists', artists)
            
            # Generate and insert albums
            albums = self.generate_albums(counts['albums'])
            results['albums'] = self.insert_data('raw.albums', albums)
            
            # Generate and insert songs
            songs = self.generate_songs(counts['songs'])
            results['songs'] = self.insert_data('raw.songs', songs)
            
            # Generate and insert song plays
            song_plays = self.generate_song_plays(counts['song_plays'])
            results['song_plays'] = self.insert_data('raw.song_plays', song_plays)
            
            # Generate and insert auth events
            auth_events = self.generate_auth_events(counts['auth_events'])
            results['auth_events'] = self.insert_data('raw.auth_events', auth_events)
            
            # Generate and insert search events
            search_events = self.generate_search_events(counts['search_events'])
            results['search_events'] = self.insert_data('raw.search_events', search_events)
            
            # Generate playlists (for reference only, not stored in DB)
            self.generate_playlists(counts['users'] // 2)
            
            # Generate and insert playlist events
            playlist_events = self.generate_playlist_events(counts['playlist_events'])
            results['playlist_events'] = self.insert_data('raw.playlist_events', playlist_events)
            
            logger.info("Data generation completed successfully")
            
        except Exception as e:
            logger.error(f"Error during data generation: {e}")
            raise
        finally:
            self.close_connection()
        
        return results

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Generate fake Spotify data for the pipeline.')
    parser.add_argument(
        '--scale', 
        type=str, 
        choices=['small', 'medium', 'large'], 
        default='medium',
        help='Scale of data generation (small, medium, large)'
    )
    args = parser.parse_args()
    
    try:
        data_generator = SpotifyDataGenerator(DB_PARAMS)
        results = data_generator.run_full_data_generation(scale=args.scale)
        
        logger.info("Data generation summary:")
        for table, count in results.items():
            logger.info(f"Inserted {count} records into {table}")
            
    except Exception as e:
        logger.error(f"Data generation failed: {e}")
        exit(1)

if __name__ == "__main__":
    main() 