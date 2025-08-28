#!/usr/bin/env python3
"""
DEPRECATED: This Python Beam pipeline has been replaced by Scio (Scala) implementation.
See scripts/scio_pipelines/src/main/scala/com/spotify/pipeline/transforms/StreamingHistoryTransform.scala

This file is kept for reference only and will be removed in future versions.
"""

import os
import argparse
import logging
import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.jdbc import ReadFromJdbc, WriteToJdbc
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.textio import WriteToText
import psycopg2
from dotenv import load_dotenv

# ===============================
# Imports and Logging Setup
# ===============================
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/beam_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# ===============================
# Environment and Database Config
# ===============================
# Database connection parameters
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'spotify_data')
DB_USER = os.getenv('DB_USER', 'spotify_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'spotify_password')
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_DRIVER = "org.postgresql.Driver"

# Date parameters
TODAY = datetime.datetime.now().strftime('%Y-%m-%d')
YESTERDAY = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')


# ===============================
# Beam DoFn: EnrichStreamingHistory
# ===============================
# This DoFn enriches each streaming history record with additional fields such as day of week, hour of day, completion rate, and processing timestamp.
class EnrichStreamingHistory(beam.DoFn):
    """Add additional fields to streaming history records."""
    
    def process(self, element):
        """Process a streaming history record."""
        try:
            # Extract datetime components for analysis
            played_at = datetime.datetime.fromisoformat(element['played_at'].replace('Z', '+00:00'))
            day_of_week = played_at.weekday()  # 0 = Monday, 6 = Sunday
            hour_of_day = played_at.hour
            
            # Calculate completion rate
            ms_played = element['ms_played']
            duration_ms = element['duration_ms']
            completion_rate = min(1.0, ms_played / duration_ms) if duration_ms > 0 else 0.0
            
            # Add enriched fields to the element
            element['day_of_week'] = day_of_week
            element['hour_of_day'] = hour_of_day
            element['completion_rate'] = completion_rate
            element['processed_at'] = datetime.datetime.now().isoformat()
            
            yield element
        except Exception as e:
            logger.error(f"Error enriching record: {e}, element: {element}")


# ===============================
# Beam DoFn: CalcUserListeningStats
# ===============================
# This DoFn computes per-user listening statistics, such as unique tracks played, total minutes, favorite artist/genre, etc.
class CalcUserListeningStats(beam.DoFn):
    """Calculate user listening statistics from streaming history."""
    
    def process(self, element):
        """Process a streaming history group for a user."""
        user_id, records = element
        
        try:
            # Calculate user stats
            unique_tracks = set()
            total_ms_played = 0
            total_plays = 0
            artist_plays = {}
            genre_plays = {}
            
            for record in records:
                track_id = record['track_id']
                unique_tracks.add(track_id)
                total_ms_played += record['ms_played']
                total_plays += 1
                
                # Track artist play counts
                artist_id = record['artist_id']
                artist_plays[artist_id] = artist_plays.get(artist_id, 0) + 1
                
                # Track genre play counts
                genres = record.get('genres', [])
                if genres:
                    for genre in genres:
                        genre_plays[genre] = genre_plays.get(genre, 0) + 1
            
            # Find favorite artist and genre
            favorite_artist_id = max(artist_plays.items(), key=lambda x: x[1])[0] if artist_plays else None
            favorite_genre = max(genre_plays.items(), key=lambda x: x[1])[0] if genre_plays else None
            
            # Convert to minutes for better readability
            total_minutes_played = total_ms_played / 60000.0
            
            # Build result record
            result = {
                'user_id': user_id,
                'unique_tracks_played': len(unique_tracks),
                'total_minutes_played': total_minutes_played,
                'total_plays': total_plays,
                'favorite_artist_id': favorite_artist_id,
                'favorite_genre': favorite_genre,
                'last_updated': datetime.datetime.now().isoformat()
            }
            
            yield result
        except Exception as e:
            logger.error(f"Error calculating user stats: {e}, user_id: {user_id}")


# ===============================
# Beam DoFn: CalcTrackPopularity
# ===============================
# This DoFn computes per-track popularity metrics, such as total plays, unique listeners, skip rate, and a trending score.
class CalcTrackPopularity(beam.DoFn):
    """Calculate track popularity metrics from streaming history."""
    
    def process(self, element):
        """Process a streaming history group for a track."""
        track_id, records = element
        
        try:
            total_plays = len(records)
            unique_listeners = len(set(r['user_id'] for r in records))
            
            # Calculate skip rate
            skip_count = sum(1 for r in records if r.get('skipped', False))
            skip_rate = skip_count / total_plays if total_plays > 0 else 0.0
            
            # Calculate average completion rate
            completion_rates = [r.get('completion_rate', 0.0) for r in records]
            avg_completion_rate = sum(completion_rates) / len(completion_rates) if completion_rates else 0.0
            
            # Calculate trending score (simple version)
            # A good completion rate and low skip rate are positive signals
            trending_score = total_plays * (1 - skip_rate) * avg_completion_rate
            
            result = {
                'track_id': track_id,
                'total_plays': total_plays,
                'unique_listeners': unique_listeners,
                'skip_rate': skip_rate,
                'avg_completion_rate': avg_completion_rate,
                'trending_score': trending_score,
                'last_updated': datetime.datetime.now().isoformat()
            }
            
            yield result
        except Exception as e:
            logger.error(f"Error calculating track popularity: {e}, track_id: {track_id}")


# ===============================
# Pipeline Construction and Execution
# ===============================
# The run_pipeline function builds and executes the Beam pipeline:
#   - Reads raw streaming history from the database
#   - Enriches records
#   - Writes enriched data to processed DB
#   - Samples data for debugging
#   - Computes user and track analytics
#   - Writes analytics to DB
def run_pipeline(start_date=YESTERDAY, end_date=TODAY, runner='DirectRunner'):
    """Build and run the data processing pipeline."""
    logger.info(f"Starting Beam pipeline for date range: {start_date} to {end_date}")
    
    # Define pipeline options
    options = PipelineOptions([
        '--runner', runner,
        '--project', 'spotify-pipeline',
        '--temp_location', 'data/beam-temp',
        '--max_num_workers', '4'
    ])
    
    # Query to fetch raw streaming history with track details
    streaming_history_query = f"""
    SELECT 
        sh.id, sh.user_id, sh.track_id, t.artist_id, t.album_id,
        sh.played_at, sh.ms_played, t.duration_ms,
        sh.skipped, sh.session_id, sh.reason_start, sh.reason_end,
        u.country, u.subscription_type,
        a.genres
    FROM 
        raw.streaming_history sh
    JOIN 
        raw.tracks t ON sh.track_id = t.track_id
    JOIN 
        raw.users u ON sh.user_id = u.user_id
    JOIN 
        raw.artists a ON t.artist_id = a.artist_id
    WHERE 
        DATE(sh.played_at) BETWEEN '{start_date}' AND '{end_date}'
    """
    
    # JDBC connection properties
    jdbc_properties = {
        'username': DB_USER,
        'password': DB_PASSWORD,
        'driver_class_name': JDBC_DRIVER
    }
    
    # Build the pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Read streaming history from database
        streaming_history = (
            pipeline
            | "Read Streaming History" >> ReadFromJdbc(
                jdbc_url=JDBC_URL,
                query=streaming_history_query,
                jdbc_driver_jar_path='path/to/postgresql-42.2.18.jar',  # This path needs to be configured
                **jdbc_properties
            )
        )
        
        # Process and enrich streaming history
        enriched_history = (
            streaming_history
            | "Enrich Streaming History" >> beam.ParDo(EnrichStreamingHistory())
        )
        
        # Write enriched data to processed.streaming_history table
        processed_schema = """
        id INTEGER, user_id VARCHAR, track_id VARCHAR, artist_id VARCHAR, album_id VARCHAR,
        played_at TIMESTAMP, ms_played INTEGER, completion_rate FLOAT, day_of_week INTEGER,
        hour_of_day INTEGER, skipped BOOLEAN, session_id VARCHAR, country VARCHAR,
        subscription_type VARCHAR, processed_at TIMESTAMP
        """
        
        processed_table = "processed.streaming_history"
        
        enriched_history | "Write Processed History" >> WriteToJdbc(
            jdbc_url=JDBC_URL,
            table_name=processed_table,
            statement="INSERT",
            schema=processed_schema,
            jdbc_driver_jar_path='path/to/postgresql-42.2.18.jar',  # This path needs to be configured
            **jdbc_properties
        )
        
        # For debugging: write sample of processed data to a text file
        (
            enriched_history
            | "Sample Processed Data" >> beam.Sample.FixedSizeGlobally(100)
            | "Format Sample Records" >> beam.Map(lambda x: str(x))
            | "Write Debug Sample" >> WriteToText(
                'data/processed/debug_sample',
                file_name_suffix='.txt',
                compression_type=CompressionTypes.GZIP
            )
        )
        
        # Calculate user listening statistics
        user_stats = (
            enriched_history
            | "Group By User" >> beam.GroupBy(lambda x: x['user_id'])
            | "Calculate User Stats" >> beam.ParDo(CalcUserListeningStats())
        )
        
        # Write user stats to analytics.user_listening_stats table
        user_stats_schema = """
        user_id VARCHAR, unique_tracks_played INTEGER, total_minutes_played FLOAT,
        total_plays INTEGER, favorite_artist_id VARCHAR, favorite_genre VARCHAR,
        last_updated TIMESTAMP
        """
        
        user_stats_table = "analytics.user_listening_stats"
        
        user_stats | "Write User Stats" >> WriteToJdbc(
            jdbc_url=JDBC_URL,
            table_name=user_stats_table,
            statement="UPSERT",
            schema=user_stats_schema,
            jdbc_driver_jar_path='path/to/postgresql-42.2.18.jar',  # This path needs to be configured
            **jdbc_properties
        )
        
        # Calculate track popularity
        track_popularity = (
            enriched_history
            | "Group By Track" >> beam.GroupBy(lambda x: x['track_id'])
            | "Calculate Track Popularity" >> beam.ParDo(CalcTrackPopularity())
        )
        
        # Write track popularity to analytics.track_popularity table
        track_popularity_schema = """
        track_id VARCHAR, total_plays INTEGER, unique_listeners INTEGER,
        skip_rate FLOAT, avg_completion_rate FLOAT, trending_score FLOAT,
        last_updated TIMESTAMP
        """
        
        track_popularity_table = "analytics.track_popularity"
        
        track_popularity | "Write Track Popularity" >> WriteToJdbc(
            jdbc_url=JDBC_URL,
            table_name=track_popularity_table,
            statement="UPSERT",
            schema=track_popularity_schema,
            jdbc_driver_jar_path='path/to/postgresql-42.2.18.jar',  # This path needs to be configured
            **jdbc_properties
        )
    
    logger.info("Beam pipeline completed successfully")


# ===============================
# Main Entrypoint
# ===============================
# Parses command-line arguments and launches the pipeline.
def main():
    """Main entry point for the pipeline."""
    parser = argparse.ArgumentParser(description="Spotify data processing pipeline")
    parser.add_argument('--start_date', type=str, default=YESTERDAY,
                        help='Start date for data processing (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, default=TODAY,
                        help='End date for data processing (YYYY-MM-DD)')
    parser.add_argument('--runner', type=str, default='DirectRunner',
                        help='Apache Beam runner to use')
    
    args = parser.parse_args()
    
    run_pipeline(args.start_date, args.end_date, args.runner)


if __name__ == "__main__":
    main() 