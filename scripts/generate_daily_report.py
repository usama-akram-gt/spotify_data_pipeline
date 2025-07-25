#!/usr/bin/env python
"""
Generate Daily Report Script
This script generates daily reports from the analytics database.
It creates CSV and JSON reports summarizing user listening patterns and track popularity.
"""

import os
import json
import argparse
import logging
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate daily reports from analytics data')
    parser.add_argument('--date', type=str, required=False, 
                        default=datetime.now().strftime('%Y-%m-%d'),
                        help='Report date in YYYY-MM-DD format')
    parser.add_argument('--output-dir', type=str, required=False, 
                        default='/opt/airflow/data/reports',
                        help='Directory to save reports')
    return parser.parse_args()

def get_db_connection():
    """Create a database connection."""
    return psycopg2.connect(
        host=os.environ.get('DB_HOST', 'postgres'),
        port=os.environ.get('DB_PORT', 5432),
        dbname='spotify_analytics',
        user=os.environ.get('DB_USER', 'spotify'),
        password=os.environ.get('DB_PASSWORD', 'spotify')
    )

def get_user_listening_summary(conn, report_date):
    """Generate user listening summary."""
    query = """
    SELECT 
        date,
        COUNT(DISTINCT user_id) AS active_users,
        SUM(total_tracks) AS total_tracks_played,
        SUM(total_time_ms) / 1000 / 60 / 60 AS total_hours_listened,
        AVG(total_tracks) AS avg_tracks_per_user,
        AVG(total_time_ms) / 1000 / 60 AS avg_minutes_per_user,
        SUM(morning_tracks) AS morning_tracks,
        SUM(afternoon_tracks) AS afternoon_tracks,
        SUM(evening_tracks) AS evening_tracks,
        SUM(night_tracks) AS night_tracks,
        SUM(weekday_tracks) AS weekday_tracks,
        SUM(weekend_tracks) AS weekend_tracks,
        AVG(completion_rate) AS avg_completion_rate
    FROM 
        spotify_analytics.user_listening_stats
    WHERE 
        date = %s
    GROUP BY 
        date
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, (report_date,))
        result = cursor.fetchone()
        return result if result else {}

def get_top_tracks(conn, report_date, limit=50):
    """Get top tracks by streams."""
    query = """
    SELECT 
        track_id,
        track_name,
        artist_name,
        total_streams,
        unique_listeners,
        avg_completion_rate,
        skip_rate
    FROM 
        spotify_analytics.track_popularity
    WHERE 
        date = %s
    ORDER BY 
        total_streams DESC
    LIMIT %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, (report_date, limit))
        return cursor.fetchall()

def get_time_distribution(conn, report_date):
    """Get listening time distribution."""
    query = """
    SELECT 
        SUM(morning_streams) AS morning_streams,
        SUM(afternoon_streams) AS afternoon_streams,
        SUM(evening_streams) AS evening_streams,
        SUM(night_streams) AS night_streams,
        SUM(weekday_streams) AS weekday_streams,
        SUM(weekend_streams) AS weekend_streams
    FROM 
        spotify_analytics.track_popularity
    WHERE 
        date = %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, (report_date,))
        result = cursor.fetchone()
        return result if result else {}

def save_report(data, filename, output_dir):
    """Save report data to CSV and JSON."""
    os.makedirs(output_dir, exist_ok=True)
    
    # Save as JSON
    json_path = os.path.join(output_dir, f"{filename}.json")
    with open(json_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    
    # Convert to DataFrame and save as CSV if it's a list or dict
    if isinstance(data, list) and data:
        df = pd.DataFrame(data)
        csv_path = os.path.join(output_dir, f"{filename}.csv")
        df.to_csv(csv_path, index=False)
    elif isinstance(data, dict) and data:
        df = pd.DataFrame([data])
        csv_path = os.path.join(output_dir, f"{filename}.csv")
        df.to_csv(csv_path, index=False)
    
    logger.info(f"Report saved to {json_path}")

def main():
    """Main function to generate reports."""
    args = parse_args()
    report_date = args.date
    output_dir = args.output_dir
    
    logger.info(f"Generating reports for date: {report_date}")
    
    # Create report directory for specific date
    date_output_dir = os.path.join(output_dir, report_date)
    os.makedirs(date_output_dir, exist_ok=True)
    
    try:
        conn = get_db_connection()
        
        # Generate user listening summary
        user_summary = get_user_listening_summary(conn, report_date)
        save_report(user_summary, "user_listening_summary", date_output_dir)
        
        # Generate top tracks report
        top_tracks = get_top_tracks(conn, report_date)
        save_report(top_tracks, "top_tracks", date_output_dir)
        
        # Generate time distribution report
        time_distribution = get_time_distribution(conn, report_date)
        save_report(time_distribution, "time_distribution", date_output_dir)
        
        # Generate combined report
        combined_report = {
            "report_date": report_date,
            "generated_at": datetime.now().isoformat(),
            "user_summary": user_summary,
            "top_tracks": top_tracks[:10],  # Include only top 10 in combined report
            "time_distribution": time_distribution
        }
        save_report(combined_report, "daily_summary", date_output_dir)
        
        logger.info(f"All reports generated successfully for {report_date}")
        
    except Exception as e:
        logger.error(f"Error generating reports: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main() 