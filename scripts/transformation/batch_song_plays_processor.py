#!/usr/bin/env python3
"""
Spotify Song Plays Batch Processor
---------------------------------
This script uses Apache Beam to process song play events in batch mode.
It reads data from PostgreSQL, performs transformations, and outputs
analytics-ready data for visualization and reporting.
"""

import os
import json
import logging
import argparse
import psycopg2
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.io import WriteToText
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict, List, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'spotify_data'),
    'user': os.getenv('DB_USER', 'spotify_user'),
    'password': os.getenv('DB_PASSWORD', 'spotify_password')
}

# JDBC connection URL for Beam
JDBC_URL = f"jdbc:postgresql://{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"

class SongPlayEvent:
    """Class representing a song play event."""
    
    def __init__(self, event_dict: Dict[str, Any]):
        """Initialize from a dictionary."""
        self.event_id = event_dict.get('event_id')
        self.user_id = event_dict.get('user_id')
        self.song_id = event_dict.get('song_id')
        self.timestamp = event_dict.get('timestamp')
        self.session_id = event_dict.get('session_id')
        self.platform = event_dict.get('platform')
        self.ms_played = event_dict.get('ms_played', 0)
        self.device_type = event_dict.get('device_type')
        
        # Parse location from JSON or dict
        location = event_dict.get('location')
        if isinstance(location, str):
            try:
                location = json.loads(location)
            except json.JSONDecodeError:
                location = {}
        elif location is None:
            location = {}
            
        self.country = location.get('country', 'unknown')
        self.city = location.get('city', 'unknown')
        
        # Calculate play completion rate
        self.song_duration = event_dict.get('song_duration', 0)
        if self.song_duration and self.song_duration > 0:
            self.completion_rate = min(float(self.ms_played) / float(self.song_duration), 1.0)
        else:
            self.completion_rate = 0.0
            
        # Parse timestamp if needed
        if isinstance(self.timestamp, str):
            try:
                self.timestamp = datetime.fromisoformat(self.timestamp.replace('Z', '+00:00'))
            except ValueError:
                self.timestamp = datetime.now()
        elif not isinstance(self.timestamp, datetime):
            self.timestamp = datetime.now()
        
        # Extract date components for aggregations
        self.date = self.timestamp.date()
        self.hour = self.timestamp.hour
        self.day_of_week = self.timestamp.weekday()
        self.month = self.timestamp.month
        self.year = self.timestamp.year

class FetchSongMetadata(beam.DoFn):
    """Beam DoFn to fetch song metadata from the database."""
    
    def __init__(self, jdbc_url: str, jdbc_driver: str, username: str, password: str):
        """Initialize with JDBC connection parameters."""
        self.jdbc_url = jdbc_url
        self.jdbc_driver = jdbc_driver
        self.username = username
        self.password = password
        self.song_cache = {}
    
    def setup(self):
        """Set up the connection to PostgreSQL."""
        self.conn = psycopg2.connect(
            host=DB_PARAMS['host'],
            port=DB_PARAMS['port'],
            database=DB_PARAMS['database'],
            user=DB_PARAMS['username'],
            password=DB_PARAMS['password']
        )
    
    def process(self, song_play_event):
        """Fetch song metadata and enrich the event."""
        song_id = song_play_event.song_id
        
        # Check if song is already in cache
        if song_id in self.song_cache:
            song_data = self.song_cache[song_id]
        else:
            # Fetch song data from database
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT s.duration_ms, s.popularity, s.song_name, 
                       a.artist_id, a.artist_name, al.album_name
                FROM raw.songs s
                JOIN raw.artists a ON s.artist_id = a.artist_id
                JOIN raw.albums al ON s.album_id = al.album_id
                WHERE s.song_id = %s
            """, (song_id,))
            
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                song_data = {
                    'duration_ms': row[0],
                    'popularity': row[1],
                    'song_name': row[2],
                    'artist_id': row[3],
                    'artist_name': row[4],
                    'album_name': row[5]
                }
                # Cache the song data
                self.song_cache[song_id] = song_data
            else:
                song_data = {
                    'duration_ms': 0,
                    'popularity': 0,
                    'song_name': 'Unknown',
                    'artist_id': 'unknown',
                    'artist_name': 'Unknown',
                    'album_name': 'Unknown'
                }
        
        # Enrich the event with song data
        enriched_event = song_play_event.__dict__.copy()
        enriched_event.update({
            'song_name': song_data['song_name'],
            'artist_name': song_data['artist_name'],
            'album_name': song_data['album_name'],
            'song_duration': song_data['duration_ms'],
            'song_popularity': song_data['popularity']
        })
        
        # Recalculate completion rate with actual duration
        if song_data['duration_ms'] > 0:
            enriched_event['completion_rate'] = min(float(song_play_event.ms_played) / float(song_data['duration_ms']), 1.0)
        
        return [enriched_event]
    
    def teardown(self):
        """Close the database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

class CalculateDailySongMetrics(beam.DoFn):
    """Calculate daily metrics for each song."""
    
    def process(self, element: Tuple[Tuple[str, datetime.date], List[Dict[str, Any]]]):
        """Process song plays grouped by song_id and date."""
        (song_id, date), plays = element
        
        # Extract the first play to get song details
        first_play = plays[0]
        
        # Calculate metrics
        play_count = len(plays)
        total_ms_played = sum(play['ms_played'] for play in plays)
        avg_ms_played = total_ms_played / play_count if play_count > 0 else 0
        
        # Count unique listeners
        unique_listeners = len(set(play['user_id'] for play in plays))
        
        # Calculate skip rate (plays with completion rate < 0.3)
        skips = sum(1 for play in plays if play.get('completion_rate', 0) < 0.3)
        skip_rate = skips / play_count if play_count > 0 else 0
        
        # Calculate completion rate
        avg_completion_rate = sum(play.get('completion_rate', 0) for play in plays) / play_count if play_count > 0 else 0
        
        # Platform breakdown
        platforms = {}
        for play in plays:
            platform = play.get('platform', 'unknown')
            if platform in platforms:
                platforms[platform] += 1
            else:
                platforms[platform] = 1
        
        # Country breakdown
        countries = {}
        for play in plays:
            country = play.get('country', 'unknown')
            if country in countries:
                countries[country] += 1
            else:
                countries[country] = 1
        
        # Create the output record
        return [{
            'song_id': song_id,
            'date': date.isoformat(),
            'song_name': first_play.get('song_name', 'Unknown'),
            'artist_name': first_play.get('artist_name', 'Unknown'),
            'album_name': first_play.get('album_name', 'Unknown'),
            'play_count': play_count,
            'unique_listeners': unique_listeners,
            'total_ms_played': total_ms_played,
            'avg_ms_played': avg_ms_played,
            'skip_rate': skip_rate,
            'avg_completion_rate': avg_completion_rate,
            'platforms': platforms,
            'countries': countries
        }]

class CalculateDailyUserMetrics(beam.DoFn):
    """Calculate daily metrics for each user."""
    
    def process(self, element: Tuple[Tuple[str, datetime.date], List[Dict[str, Any]]]):
        """Process song plays grouped by user_id and date."""
        (user_id, date), plays = element
        
        # Calculate metrics
        play_count = len(plays)
        total_ms_played = sum(play['ms_played'] for play in plays)
        
        # Count unique songs, artists, and albums
        unique_songs = len(set(play['song_id'] for play in plays))
        unique_artists = len(set(play.get('artist_id', 'unknown') for play in plays))
        
        # Calculate top genres (would require additional data enrichment)
        # For simplicity, assume no genre data here
        
        # Platform breakdown
        platforms = {}
        for play in plays:
            platform = play.get('platform', 'unknown')
            if platform in platforms:
                platforms[platform] += 1
            else:
                platforms[platform] = 1
        
        # Top artist (by play count)
        artist_plays = {}
        for play in plays:
            artist_name = play.get('artist_name', 'Unknown')
            if artist_name in artist_plays:
                artist_plays[artist_name] += 1
            else:
                artist_plays[artist_name] = 1
        
        top_artist = max(artist_plays.items(), key=lambda x: x[1])[0] if artist_plays else 'Unknown'
        
        # Create the output record
        return [{
            'user_id': user_id,
            'date': date.isoformat(),
            'song_count': play_count,
            'unique_songs': unique_songs,
            'unique_artists': unique_artists,
            'total_listening_time': total_ms_played,
            'top_artist': top_artist,
            'platforms': platforms
        }]

class WriteToDatabaseFn(beam.DoFn):
    """Write metrics to the database."""
    
    def __init__(self, table_name: str):
        """Initialize with target table name."""
        self.table_name = table_name
    
    def setup(self):
        """Set up the connection to PostgreSQL."""
        self.conn = psycopg2.connect(**DB_PARAMS)
        self.conn.autocommit = False
    
    def process(self, batch):
        """Write a batch of records to the database."""
        if not batch:
            return
        
        cursor = self.conn.cursor()
        
        try:
            # For song metrics
            if self.table_name == 'analytics.song_metrics':
                records = []
                for record in batch:
                    # Main metrics record for each country
                    for country, count in record.get('countries', {}).items():
                        # For each platform
                        platform_counts = record.get('platforms', {})
                        if not platform_counts:
                            platform_counts = {'unknown': record['play_count']}
                            
                        for platform, platform_count in platform_counts.items():
                            # Calculate the proportion for this country-platform combination
                            proportion = platform_count / record['play_count'] if record['play_count'] > 0 else 0
                            country_platform_count = int(count * proportion)
                            
                            if country_platform_count > 0:
                                records.append((
                                    record['song_id'],
                                    record['date'],
                                    country_platform_count,
                                    int(record['unique_listeners'] * (country_platform_count / record['play_count'])) if record['play_count'] > 0 else 0,
                                    record['avg_ms_played'],
                                    record['skip_rate'],
                                    country,
                                    platform,
                                    datetime.now(),
                                    datetime.now()
                                ))
                
                # Batch insert
                cursor.executemany("""
                    INSERT INTO analytics.song_metrics 
                    (song_id, date, play_count, unique_listeners, average_play_duration, skip_rate, country, platform, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (song_id, date, country, platform) 
                    DO UPDATE SET 
                        play_count = EXCLUDED.play_count,
                        unique_listeners = EXCLUDED.unique_listeners,
                        average_play_duration = EXCLUDED.average_play_duration,
                        skip_rate = EXCLUDED.skip_rate,
                        updated_at = EXCLUDED.updated_at
                """, records)
                
            # For user metrics
            elif self.table_name == 'analytics.user_listening_habits':
                records = []
                for record in batch:
                    # Get the primary platform
                    platforms = record.get('platforms', {})
                    primary_platform = max(platforms.items(), key=lambda x: x[1])[0] if platforms else 'unknown'
                    
                    records.append((
                        record['user_id'],
                        record['date'],
                        record['total_listening_time'],
                        record['song_count'],
                        'unknown',  # No genre info in this example
                        record['top_artist'],
                        record['total_listening_time'] // record['song_count'] if record['song_count'] > 0 else 0,
                        primary_platform,
                        datetime.now(),
                        datetime.now()
                    ))
                
                # Batch insert
                cursor.executemany("""
                    INSERT INTO analytics.user_listening_habits 
                    (user_id, date, total_listening_time, song_count, top_genre, top_artist, avg_session_duration, platform, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id, date) 
                    DO UPDATE SET 
                        total_listening_time = EXCLUDED.total_listening_time,
                        song_count = EXCLUDED.song_count,
                        top_genre = EXCLUDED.top_genre,
                        top_artist = EXCLUDED.top_artist,
                        avg_session_duration = EXCLUDED.avg_session_duration,
                        platform = EXCLUDED.platform,
                        updated_at = EXCLUDED.updated_at
                """, records)
            
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error writing to {self.table_name}: {e}")
            
        finally:
            cursor.close()
    
    def teardown(self):
        """Close the database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

def run_song_metrics_pipeline(start_date: str, end_date: str, output_dir: str = None):
    """Run the batch processing pipeline for song metrics."""
    # Parse dates
    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)
    
    # Format date strings for SQL
    start_date_sql = start_dt.strftime('%Y-%m-%d')
    end_date_sql = end_dt.strftime('%Y-%m-%d')
    
    # SQL query to fetch song play events
    song_plays_query = f"""
        SELECT sp.event_id, sp.user_id, sp.song_id, sp.timestamp, sp.session_id, 
               sp.platform, sp.ms_played, sp.device_type, sp.location, 
               s.duration_ms as song_duration, s.popularity as song_popularity,
               s.song_name, a.artist_id, a.artist_name, al.album_name
        FROM raw.song_plays sp
        JOIN raw.songs s ON sp.song_id = s.song_id
        JOIN raw.artists a ON s.artist_id = a.artist_id
        JOIN raw.albums al ON s.album_id = al.album_id
        WHERE sp.timestamp BETWEEN '{start_date_sql}' AND '{end_date_sql}'
    """
    
    # Pipeline options
    options = PipelineOptions([
        '--runner=DirectRunner',
        '--direct_running_mode=multi_processing',
        '--direct_num_workers=4'
    ])
    
    # Run the pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Read song plays from database
        song_plays = (
            pipeline
            | "ReadSongPlays" >> beam.io.ReadFromJdbc(
                table_name='',
                query=song_plays_query,
                jdbc_url=JDBC_URL,
                jdbc_driver_class_name='org.postgresql.Driver',
                username=DB_PARAMS['user'],
                password=DB_PARAMS['password'],
                fetch_size=10000
            )
            | "ParseSongPlayEvents" >> beam.Map(lambda row: SongPlayEvent({
                'event_id': row[0],
                'user_id': row[1],
                'song_id': row[2],
                'timestamp': row[3],
                'session_id': row[4],
                'platform': row[5],
                'ms_played': row[6],
                'device_type': row[7],
                'location': row[8],
                'song_duration': row[9],
                'song_popularity': row[10],
                'song_name': row[11],
                'artist_id': row[12],
                'artist_name': row[13],
                'album_name': row[14]
            }))
        )
        
        # Convert to dict for easier processing
        song_plays_dict = song_plays | "ConvertToDict" >> beam.Map(lambda x: x.__dict__)
        
        # Calculate song metrics
        song_metrics = (
            song_plays_dict
            | "KeyBySongAndDate" >> beam.Map(lambda x: ((x['song_id'], x['date']), x))
            | "GroupBySongAndDate" >> beam.GroupByKey()
            | "CalculateSongMetrics" >> beam.ParDo(CalculateDailySongMetrics())
        )
        
        # Write song metrics to database
        song_metrics | "WriteSongMetrics" >> beam.ParDo(WriteToDatabaseFn('analytics.song_metrics'))
        
        # Calculate user metrics
        user_metrics = (
            song_plays_dict
            | "KeyByUserAndDate" >> beam.Map(lambda x: ((x['user_id'], x['date']), x))
            | "GroupByUserAndDate" >> beam.GroupByKey()
            | "CalculateUserMetrics" >> beam.ParDo(CalculateDailyUserMetrics())
        )
        
        # Write user metrics to database
        user_metrics | "WriteUserMetrics" >> beam.ParDo(WriteToDatabaseFn('analytics.user_listening_habits'))
        
        # Optionally write to text files for debugging
        if output_dir:
            song_metrics | "WriteSongMetricsToText" >> WriteToText(f"{output_dir}/song_metrics", file_name_suffix='.json')
            user_metrics | "WriteUserMetricsToText" >> WriteToText(f"{output_dir}/user_metrics", file_name_suffix='.json')

def run_daily_active_users_pipeline(date_str: str):
    """Run the batch processing pipeline for daily active users."""
    # Parse date
    date_dt = datetime.fromisoformat(date_str)
    
    # Format date string for SQL
    date_sql = date_dt.strftime('%Y-%m-%d')
    month_start = date_dt.replace(day=1).strftime('%Y-%m-%d')
    week_start = (date_dt - timedelta(days=date_dt.weekday())).strftime('%Y-%m-%d')
    
    # Connect to database
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()
    
    try:
        # Calculate active users
        cursor.execute(f"""
            -- Daily active users
            WITH daily_users AS (
                SELECT DISTINCT user_id 
                FROM raw.song_plays
                WHERE DATE(timestamp) = '{date_sql}'
            ),
            -- Weekly active users
            weekly_users AS (
                SELECT DISTINCT user_id 
                FROM raw.song_plays
                WHERE DATE(timestamp) BETWEEN '{week_start}' AND '{date_sql}'
            ),
            -- Monthly active users
            monthly_users AS (
                SELECT DISTINCT user_id 
                FROM raw.song_plays
                WHERE DATE(timestamp) BETWEEN '{month_start}' AND '{date_sql}'
            ),
            -- New users on this date
            new_users AS (
                SELECT user_id
                FROM raw.users
                WHERE DATE(registration_date) = '{date_sql}'
            ),
            -- Users who were active in previous 30 days but not on this date
            churned_users AS (
                SELECT u.user_id
                FROM (
                    SELECT DISTINCT user_id 
                    FROM raw.song_plays
                    WHERE DATE(timestamp) BETWEEN '{(date_dt - timedelta(days=30)).strftime('%Y-%m-%d')}' AND '{(date_dt - timedelta(days=1)).strftime('%Y-%m-%d')}'
                ) u
                LEFT JOIN daily_users d ON u.user_id = d.user_id
                WHERE d.user_id IS NULL
            )
            
            INSERT INTO analytics.daily_active_users
            (date, count, weekly_active_count, monthly_active_count, new_users_count, churn_count, created_at, updated_at)
            VALUES (
                '{date_sql}',
                (SELECT COUNT(*) FROM daily_users),
                (SELECT COUNT(*) FROM weekly_users),
                (SELECT COUNT(*) FROM monthly_users),
                (SELECT COUNT(*) FROM new_users),
                (SELECT COUNT(*) FROM churned_users),
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (date) 
            DO UPDATE SET 
                count = EXCLUDED.count,
                weekly_active_count = EXCLUDED.weekly_active_count,
                monthly_active_count = EXCLUDED.monthly_active_count,
                new_users_count = EXCLUDED.new_users_count,
                churn_count = EXCLUDED.churn_count,
                updated_at = EXCLUDED.updated_at
        """)
        
        conn.commit()
        logger.info(f"Successfully calculated daily active users for {date_sql}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error calculating daily active users: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Process Spotify song plays in batch mode using Apache Beam.')
    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='Start date for batch processing (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date for batch processing (YYYY-MM-DD, defaults to start date)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Directory to write output files (optional)'
    )
    args = parser.parse_args()
    
    # If end date is not provided, use start date
    end_date = args.end_date if args.end_date else args.start_date
    
    try:
        # Run the song metrics pipeline
        logger.info(f"Running song metrics pipeline for {args.start_date} to {end_date}")
        run_song_metrics_pipeline(args.start_date, end_date, args.output_dir)
        
        # Calculate daily active users for each day in the range
        start_dt = datetime.fromisoformat(args.start_date)
        end_dt = datetime.fromisoformat(end_date)
        current_dt = start_dt
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime('%Y-%m-%d')
            logger.info(f"Calculating daily active users for {date_str}")
            run_daily_active_users_pipeline(date_str)
            current_dt += timedelta(days=1)
            
        logger.info("Batch processing completed successfully")
        
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        exit(1)

if __name__ == "__main__":
    main() 