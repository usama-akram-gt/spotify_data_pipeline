#!/usr/bin/env python3
"""
Spotify Data Pipeline - Apache Beam Transformation for Streaming History

This script uses Apache Beam to transform streaming history data for analytics.
It performs the following transformations:
- Calculates play duration statistics by user, track, and artist
- Computes completion rates for tracks
- Identifies listening patterns by time of day
- Groups plays by platform and country
"""

import argparse
import datetime
import json
import logging
import os
import sys
from typing import Dict, List, Tuple, Any

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.io.jdbc import ReadFromJdbc, WriteToJdbc
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms import PTransform, DoFn, GroupByKey, CombinePerKey
from apache_beam.transforms.core import ParDo
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "spotify_raw")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_ANALYTICS = os.getenv("DB_ANALYTICS_NAME", "spotify_analytics")

# JDBC connection strings
JDBC_RAW_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_ANALYTICS_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_ANALYTICS}"
JDBC_DRIVER = "org.postgresql.Driver"

# SQL queries
STREAMING_HISTORY_QUERY = """
SELECT 
    event_id, user_id, track_id, timestamp, ms_played, 
    reason_start, reason_end, shuffle, skipped, platform, country
FROM raw.streaming_history
WHERE DATE(timestamp) = ?
"""

# Beam transforms
class ExtractStreamingData(DoFn):
    """Extract and parse streaming history data."""
    
    def process(self, element):
        """Process each streaming history record."""
        try:
            # Convert timestamp to datetime
            if isinstance(element['timestamp'], str):
                element['timestamp'] = datetime.datetime.fromisoformat(
                    element['timestamp'].replace('Z', '+00:00')
                )
            
            # Calculate hour of day for time-based analysis
            hour_of_day = element['timestamp'].hour
            
            # Determine time of day category
            if 5 <= hour_of_day < 12:
                time_of_day = 'morning'
            elif 12 <= hour_of_day < 17:
                time_of_day = 'afternoon'
            elif 17 <= hour_of_day < 22:
                time_of_day = 'evening'
            else:
                time_of_day = 'night'
            
            # Determine if weekend
            is_weekend = element['timestamp'].weekday() >= 5
            
            # Add derived fields
            element['time_of_day'] = time_of_day
            element['is_weekend'] = is_weekend
            element['date'] = element['timestamp'].date().isoformat()
            
            # Calculate completion ratio if we have duration data
            if 'ms_played' in element and element.get('ms_played') is not None:
                # Note: We would need track duration to calculate actual completion ratio
                # This is a placeholder estimation
                completion_ratio = min(1.0, element['ms_played'] / 180000)  # Assume 3 min avg
                element['completion_ratio'] = completion_ratio
            
            yield element
        
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            # Skip the record
            return


class CalculateUserListeningStats(DoFn):
    """Calculate listening statistics by user and date."""
    
    def process(self, element):
        """Process group of records for a user on a date."""
        (key, records) = element
        user_id, date = key
        
        # Initialize counters
        total_tracks = 0
        total_time_ms = 0
        track_durations = []
        artists = {}
        genres = {}
        time_of_day_counts = {
            'morning': 0,
            'afternoon': 0,
            'evening': 0,
            'night': 0
        }
        
        # Process records
        for record in records:
            total_tracks += 1
            
            # Accumulate play time
            if 'ms_played' in record and record['ms_played'] is not None:
                total_time_ms += record['ms_played']
                track_durations.append(record['ms_played'])
            
            # Count by time of day
            if 'time_of_day' in record:
                time_of_day_counts[record['time_of_day']] += 1
            
            # Track artist counts (for favorite artist calculation)
            if 'artist_id' in record:
                artists[record['artist_id']] = artists.get(record['artist_id'], 0) + 1
            
            # Track genre counts (would come from artist or track metadata)
            if 'genre' in record:
                genres[record['genre']] = genres.get(record['genre'], 0) + 1
        
        # Calculate averages and favorites
        avg_track_duration_ms = sum(track_durations) / len(track_durations) if track_durations else 0
        favorite_artist_id = max(artists.items(), key=lambda x: x[1])[0] if artists else None
        favorite_genre = max(genres.items(), key=lambda x: x[1])[0] if genres else None
        
        # Create result record
        result = {
            'date': date,
            'user_id': user_id,
            'total_tracks': total_tracks,
            'total_time_ms': total_time_ms,
            'avg_track_duration_ms': avg_track_duration_ms,
            'favorite_artist_id': favorite_artist_id,
            'favorite_genre': favorite_genre,
            'morning_tracks': time_of_day_counts['morning'],
            'afternoon_tracks': time_of_day_counts['afternoon'],
            'evening_tracks': time_of_day_counts['evening'],
            'night_tracks': time_of_day_counts['night']
        }
        
        yield result


class CalculateTrackPopularity(DoFn):
    """Calculate popularity statistics by track and date."""
    
    def process(self, element):
        """Process group of records for a track on a date."""
        (key, records) = element
        track_id, date = key
        
        # Initialize counters
        total_streams = 0
        unique_listeners = set()
        completion_rates = []
        skips = 0
        
        # Process records
        for record in records:
            total_streams += 1
            unique_listeners.add(record['user_id'])
            
            # Track completion rates
            if 'completion_ratio' in record:
                completion_rates.append(record['completion_ratio'])
            
            # Track skips
            if record.get('skipped'):
                skips += 1
        
        # Calculate statistics
        avg_completion_rate = sum(completion_rates) / len(completion_rates) if completion_rates else 0
        skip_rate = skips / total_streams if total_streams > 0 else 0
        
        # Create result record
        result = {
            'date': date,
            'track_id': track_id,
            'total_streams': total_streams,
            'unique_listeners': len(unique_listeners),
            'avg_completion_rate': avg_completion_rate,
            'skip_rate': skip_rate
        }
        
        yield result


def run_pipeline(date_str, output_path=None):
    """Run the Apache Beam pipeline for the specified date."""
    # Pipeline options
    options = PipelineOptions([
        '--runner=DirectRunner',
        f'--temp_location=/opt/beam/temp'
    ])
    options.view_as(SetupOptions).save_main_session = True
    
    # Parse the date
    try:
        date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        logger.error(f"Invalid date format: {date_str}. Use YYYY-MM-DD.")
        return
    
    # Create pipeline
    with beam.Pipeline(options=options) as pipeline:
        # Read streaming history from database
        streaming_data = (
            pipeline
            | "ReadFromJDBC" >> ReadFromJdbc(
                table_name="raw.streaming_history",
                driver_class_name=JDBC_DRIVER,
                jdbc_url=JDBC_RAW_URL,
                username=DB_USER,
                password=DB_PASSWORD,
                query=STREAMING_HISTORY_QUERY,
                statement_parameters=[date_str]
            )
            | "ExtractStreamingData" >> ParDo(ExtractStreamingData())
        )
        
        # Calculate daily listening stats by user
        (
            streaming_data
            | "KeyByUserAndDate" >> beam.Map(lambda x: ((x['user_id'], x['date']), x))
            | "GroupByUserAndDate" >> GroupByKey()
            | "CalculateUserListeningStats" >> ParDo(CalculateUserListeningStats())
            | "WriteDailyListeningStats" >> WriteToJdbc(
                table_name="analytics.daily_listening_stats",
                driver_class_name=JDBC_DRIVER,
                jdbc_url=JDBC_ANALYTICS_URL,
                username=DB_USER,
                password=DB_PASSWORD
            )
        )
        
        # Calculate track popularity
        (
            streaming_data
            | "KeyByTrackAndDate" >> beam.Map(lambda x: ((x['track_id'], x['date']), x))
            | "GroupByTrackAndDate" >> GroupByKey()
            | "CalculateTrackPopularity" >> ParDo(CalculateTrackPopularity())
            | "WriteTrackPopularity" >> WriteToJdbc(
                table_name="analytics.track_popularity",
                driver_class_name=JDBC_DRIVER,
                jdbc_url=JDBC_ANALYTICS_URL,
                username=DB_USER,
                password=DB_PASSWORD
            )
        )
        
        # If output path is specified, also write to file
        if output_path:
            (
                streaming_data
                | "FormatForOutput" >> beam.Map(lambda x: json.dumps(x))
                | "WriteToText" >> WriteToText(output_path)
            )


def main():
    """Main function to run the Apache Beam pipeline."""
    parser = argparse.ArgumentParser(description='Transform streaming history data using Apache Beam')
    parser.add_argument('--date', required=True, help='Date to process in YYYY-MM-DD format')
    parser.add_argument('--output', help='Optional output file path')
    args = parser.parse_args()
    
    # Run the pipeline
    run_pipeline(args.date, args.output)


if __name__ == "__main__":
    main() 