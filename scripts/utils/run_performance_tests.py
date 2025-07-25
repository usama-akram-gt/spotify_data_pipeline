#!/usr/bin/env python3
"""
Performance testing script for the Spotify data pipeline.

This script runs performance tests on various components of the data pipeline:
- Database operations
- Kafka operations
- Data processing operations
- Scio pipeline operations

Usage:
  python run_performance_tests.py [--test-component COMPONENT] [--iterations ITERATIONS] [--report-file REPORT_FILE]

Options:
  --test-component COMPONENT  Component to test (db, kafka, processing, scio, all) [default: all]
  --iterations ITERATIONS     Number of test iterations to run [default: 10]
  --report-file REPORT_FILE   Path to save the performance report [default: performance_report.json]
"""

import argparse
import json
import logging
import os
import time
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to the path to import from scripts.utils
sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.utils import config, db_utils, kafka_utils, test_utils

# Configure logging
logger = config.configure_logging(__name__)

# Default test parameters
DEFAULT_ITERATIONS = 10
DEFAULT_COMPONENT = "all"
DEFAULT_REPORT_FILE = "performance_report.json"
DEFAULT_TEST_SIZE = "small"
DEFAULT_WARMUP = 3


class PerformanceTester:
    """Class for running performance tests on the Spotify data pipeline."""
    
    def __init__(self, component: str = DEFAULT_COMPONENT, 
                iterations: int = DEFAULT_ITERATIONS,
                test_size: str = DEFAULT_TEST_SIZE,
                warmup: int = DEFAULT_WARMUP):
        """
        Initialize the performance tester.
        
        Args:
            component: Component to test ('db', 'kafka', 'processing', 'scio', 'all')
            iterations: Number of test iterations to run
            test_size: Size of test data to use ('tiny', 'small', 'medium')
            warmup: Number of warmup iterations
        """
        self.component = component
        self.iterations = iterations
        self.test_size = test_size
        self.warmup = warmup
        self.results = {}
        
        # Set up test helpers
        self.db_helper = test_utils.DatabaseTestHelper(db_type="raw", test_schema="perf_test")
        self.kafka_helper = test_utils.KafkaTestHelper(topic_prefix="perf_test")
        self.data_generator = test_utils.TestDataGenerator(size=test_size)
        
        # Set up test data
        self.test_data = None
    
    def setup(self):
        """Set up the test environment."""
        logger.info("Setting up performance test environment...")
        
        # Set up database
        self.db_helper.setup_test_schema()
        
        # Create tables
        logger.info("Creating test tables...")
        for table in ["users", "artists", "albums", "tracks", "streaming_history"]:
            self.db_helper.create_table(table)
        
        # Generate test data
        logger.info(f"Generating test data (size: {self.test_size})...")
        self.test_data = self.data_generator.generate_all_test_data()
        
        # Set up Kafka if testing Kafka
        if self.component in ["kafka", "all"]:
            self.kafka_helper.setup()
    
    def teardown(self):
        """Clean up the test environment."""
        logger.info("Cleaning up performance test environment...")
        
        # Clean up Kafka
        if self.component in ["kafka", "all"]:
            self.kafka_helper.teardown()
        
        # Clean up database
        self.db_helper.teardown_test_schema()
        self.db_helper.disconnect()
    
    def run_tests(self):
        """Run the performance tests based on the selected component."""
        logger.info(f"Running performance tests for component: {self.component}")
        
        # Run appropriate tests based on component
        if self.component in ["db", "all"]:
            self.test_database_operations()
        
        if self.component in ["kafka", "all"]:
            self.test_kafka_operations()
        
        if self.component in ["processing", "all"]:
            self.test_data_processing()
        
        if self.component in ["scio", "all"]:
            self.test_scio_operations()
        
        return self.results
    
    def test_database_operations(self):
        """Test database operations performance."""
        logger.info("Testing database operations performance...")
        
        # Test insert performance
        insert_results = {}
        
        for table, data in self.test_data.items():
            logger.info(f"Testing insert performance for table: {table}")
            
            def test_insert():
                # Truncate the table before each iteration
                with self.db_helper.conn.cursor() as cursor:
                    cursor.execute(f"TRUNCATE TABLE {self.db_helper.test_schema}.{table}")
                    self.db_helper.conn.commit()
                
                # Insert the data
                self.db_helper.populate_test_data(table, data)
            
            # Run performance test
            metrics = test_utils.run_performance_test(
                test_insert, 
                iterations=self.iterations,
                warmup=self.warmup
            )
            
            insert_results[table] = {
                "records": len(data),
                "avg_time": metrics["avg_time"],
                "min_time": metrics["min_time"],
                "max_time": metrics["max_time"],
                "records_per_sec": len(data) / metrics["avg_time"]
            }
        
        # Test query performance
        query_results = {}
        
        # Ensure data is in all tables
        for table, data in self.test_data.items():
            with self.db_helper.conn.cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {self.db_helper.test_schema}.{table}")
                self.db_helper.conn.commit()
            self.db_helper.populate_test_data(table, data)
        
        # Test simple query
        logger.info("Testing simple query performance...")
        
        def test_simple_query():
            return self.db_helper.execute_query(
                f"SELECT * FROM {self.db_helper.test_schema}.users LIMIT 100"
            )
        
        simple_metrics = test_utils.run_performance_test(
            test_simple_query, 
            iterations=self.iterations,
            warmup=self.warmup
        )
        
        query_results["simple_query"] = {
            "avg_time": simple_metrics["avg_time"],
            "min_time": simple_metrics["min_time"],
            "max_time": simple_metrics["max_time"]
        }
        
        # Test complex join query
        logger.info("Testing complex join query performance...")
        
        def test_complex_query():
            return self.db_helper.execute_query(
                f"""
                SELECT 
                    u.user_id,
                    u.username,
                    COUNT(sh.event_id) as play_count,
                    SUM(sh.ms_played) as total_play_time,
                    t.title as most_played_track
                FROM 
                    {self.db_helper.test_schema}.users u
                JOIN 
                    {self.db_helper.test_schema}.streaming_history sh ON u.user_id = sh.user_id
                JOIN 
                    {self.db_helper.test_schema}.tracks t ON sh.track_id = t.track_id
                GROUP BY 
                    u.user_id, u.username, t.title
                ORDER BY 
                    play_count DESC
                LIMIT 100
                """
            )
        
        complex_metrics = test_utils.run_performance_test(
            test_complex_query, 
            iterations=self.iterations,
            warmup=self.warmup
        )
        
        query_results["complex_query"] = {
            "avg_time": complex_metrics["avg_time"],
            "min_time": complex_metrics["min_time"],
            "max_time": complex_metrics["max_time"]
        }
        
        # Store results
        self.results["database"] = {
            "insert": insert_results,
            "query": query_results
        }
    
    def test_kafka_operations(self):
        """Test Kafka operations performance."""
        logger.info("Testing Kafka operations performance...")
        
        # Test producer performance
        producer_results = {}
        
        # Generate some streaming history events for Kafka
        streaming_events = self.test_data["streaming_history"][:1000]
        
        logger.info("Testing Kafka producer performance...")
        
        def test_producer():
            self.kafka_helper.produce_test_messages("play_events", streaming_events, key_field="event_id")
        
        producer_metrics = test_utils.run_performance_test(
            test_producer, 
            iterations=self.iterations,
            warmup=self.warmup
        )
        
        producer_results["play_events"] = {
            "records": len(streaming_events),
            "avg_time": producer_metrics["avg_time"],
            "min_time": producer_metrics["min_time"],
            "max_time": producer_metrics["max_time"],
            "records_per_sec": len(streaming_events) / producer_metrics["avg_time"]
        }
        
        # Test consumer performance
        logger.info("Testing Kafka consumer performance...")
        
        # First produce a batch of messages
        self.kafka_helper.produce_test_messages("play_events", streaming_events, key_field="event_id")
        
        def test_consumer():
            # Create a new consumer for each test to ensure we're always reading from the beginning
            consumer = kafka_utils.get_kafka_consumer(
                group_id=f"perf_test_consumer_{int(time.time())}",
                auto_offset_reset="earliest"
            )
            
            # Consume messages
            messages = kafka_utils.consume_messages(
                consumer,
                [self.kafka_helper.get_test_topic("play_events")],
                timeout=5.0,
                num_messages=1000  # Limit to avoid reading indefinitely
            )
            
            consumer.close()
            return messages
        
        consumer_metrics = test_utils.run_performance_test(
            test_consumer, 
            iterations=self.iterations,
            warmup=self.warmup
        )
        
        consumer_results = {
            "play_events": {
                "avg_time": consumer_metrics["avg_time"],
                "min_time": consumer_metrics["min_time"],
                "max_time": consumer_metrics["max_time"]
            }
        }
        
        # Store results
        self.results["kafka"] = {
            "producer": producer_results,
            "consumer": consumer_results
        }
    
    def test_data_processing(self):
        """Test data processing performance."""
        logger.info("Testing data processing performance...")
        
        # Get streaming history data
        streaming_events = self.test_data["streaming_history"]
        
        # Test time of day classification
        logger.info("Testing time of day classification performance...")
        
        def test_time_classification():
            results = []
            for event in streaming_events:
                # Parse played_at datetime
                played_at = datetime.strptime(event["played_at"], "%Y-%m-%d %H:%M:%S")
                
                # Classify time of day
                hour = played_at.hour
                if 6 <= hour < 12:
                    time_of_day = "morning"
                elif 12 <= hour < 18:
                    time_of_day = "afternoon"
                elif 18 <= hour < 22:
                    time_of_day = "evening"
                else:
                    time_of_day = "night"
                
                # Check if weekend
                is_weekend = played_at.weekday() >= 5
                
                results.append({
                    "event_id": event["event_id"],
                    "time_of_day": time_of_day,
                    "is_weekend": is_weekend
                })
            
            return results
        
        time_metrics = test_utils.run_performance_test(
            test_time_classification, 
            iterations=self.iterations,
            warmup=self.warmup
        )
        
        # Test user listening stats calculation
        logger.info("Testing user listening stats calculation performance...")
        
        def test_listening_stats():
            # Group by user
            user_events = {}
            for event in streaming_events:
                user_id = event["user_id"]
                if user_id not in user_events:
                    user_events[user_id] = []
                user_events[user_id].append(event)
            
            # Calculate stats for each user
            user_stats = []
            for user_id, events in user_events.items():
                total_tracks = len(events)
                total_ms_played = sum(event["ms_played"] for event in events)
                avg_track_length = total_ms_played // total_tracks if total_tracks > 0 else 0
                
                # Count by time of day
                morning_plays = 0
                afternoon_plays = 0
                evening_plays = 0
                night_plays = 0
                weekday_plays = 0
                weekend_plays = 0
                
                for event in events:
                    played_at = datetime.strptime(event["played_at"], "%Y-%m-%d %H:%M:%S")
                    
                    # Time of day
                    hour = played_at.hour
                    if 6 <= hour < 12:
                        morning_plays += 1
                    elif 12 <= hour < 18:
                        afternoon_plays += 1
                    elif 18 <= hour < 22:
                        evening_plays += 1
                    else:
                        night_plays += 1
                    
                    # Weekday/weekend
                    if played_at.weekday() >= 5:
                        weekend_plays += 1
                    else:
                        weekday_plays += 1
                
                user_stats.append({
                    "user_id": user_id,
                    "tracks_played": total_tracks,
                    "total_ms_played": total_ms_played,
                    "avg_track_length": avg_track_length,
                    "morning_plays": morning_plays,
                    "afternoon_plays": afternoon_plays,
                    "evening_plays": evening_plays,
                    "night_plays": night_plays,
                    "weekday_plays": weekday_plays,
                    "weekend_plays": weekend_plays
                })
            
            return user_stats
        
        stats_metrics = test_utils.run_performance_test(
            test_listening_stats, 
            iterations=self.iterations,
            warmup=self.warmup
        )
        
        # Test track popularity calculation
        logger.info("Testing track popularity calculation performance...")
        
        def test_track_popularity():
            # Group by track
            track_events = {}
            for event in streaming_events:
                track_id = event["track_id"]
                if track_id not in track_events:
                    track_events[track_id] = []
                track_events[track_id].append(event)
            
            # Calculate stats for each track
            track_stats = []
            for track_id, events in track_events.items():
                total_streams = len(events)
                unique_listeners = len(set(event["user_id"] for event in events))
                
                # Get tracks data for completion calculation
                track_duration = None
                for track in self.test_data["tracks"]:
                    if track["track_id"] == track_id:
                        track_duration = track["duration_ms"]
                        break
                
                # Calculate completion and skip rates
                if track_duration:
                    completion_rates = [event["ms_played"] / track_duration for event in events]
                    avg_completion_rate = sum(completion_rates) / len(completion_rates)
                    skip_rate = sum(1 for rate in completion_rates if rate < 0.8) / len(completion_rates)
                else:
                    avg_completion_rate = 0
                    skip_rate = 0
                
                track_stats.append({
                    "track_id": track_id,
                    "total_streams": total_streams,
                    "unique_listeners": unique_listeners,
                    "avg_completion_rate": avg_completion_rate,
                    "skip_rate": skip_rate
                })
            
            return track_stats
        
        popularity_metrics = test_utils.run_performance_test(
            test_track_popularity, 
            iterations=self.iterations,
            warmup=self.warmup
        )
        
        # Store results
        self.results["data_processing"] = {
            "time_classification": {
                "records": len(streaming_events),
                "avg_time": time_metrics["avg_time"],
                "min_time": time_metrics["min_time"],
                "max_time": time_metrics["max_time"],
                "records_per_sec": len(streaming_events) / time_metrics["avg_time"]
            },
            "user_listening_stats": {
                "records": len(streaming_events),
                "avg_time": stats_metrics["avg_time"],
                "min_time": stats_metrics["min_time"],
                "max_time": stats_metrics["max_time"],
                "records_per_sec": len(streaming_events) / stats_metrics["avg_time"]
            },
            "track_popularity": {
                "records": len(streaming_events),
                "avg_time": popularity_metrics["avg_time"],
                "min_time": popularity_metrics["min_time"],
                "max_time": popularity_metrics["max_time"],
                "records_per_sec": len(streaming_events) / popularity_metrics["avg_time"]
            }
        }
    
    def test_scio_operations(self):
        """Test Scio operations performance."""
        logger.info("Testing Scio operations (simulation)...")
        
        # Since we can't directly run the Scio pipeline in Python,
        # we'll simulate its performance by running similar operations.
        
        # Test streaming history transformation (similar to what Scio does)
        logger.info("Testing streaming history transformation performance...")
        
        def test_streaming_transform():
            # Step 1: Get streaming history and related data
            streaming_events = self.test_data["streaming_history"]
            tracks = {track["track_id"]: track for track in self.test_data["tracks"]}
            
            # Step 2: Enrich streaming events
            enriched_events = []
            for event in streaming_events:
                track_id = event["track_id"]
                track = tracks.get(track_id)
                
                if not track:
                    continue
                
                # Parse played_at datetime
                played_at = datetime.strptime(event["played_at"], "%Y-%m-%d %H:%M:%S")
                
                # Classify time of day
                hour = played_at.hour
                if 6 <= hour < 12:
                    time_of_day = "morning"
                elif 12 <= hour < 18:
                    time_of_day = "afternoon"
                elif 18 <= hour < 22:
                    time_of_day = "evening"
                else:
                    time_of_day = "night"
                
                # Check if weekend
                is_weekend = played_at.weekday() >= 5
                
                # Calculate completion ratio
                completion_ratio = event["ms_played"] / track["duration_ms"] if track["duration_ms"] > 0 else 0
                
                # Create enriched event
                enriched_events.append({
                    "event_id": event["event_id"],
                    "user_id": event["user_id"],
                    "track_id": track_id,
                    "played_at": event["played_at"],
                    "date": played_at.strftime("%Y-%m-%d"),
                    "ms_played": event["ms_played"],
                    "track_duration": track["duration_ms"],
                    "completion_ratio": completion_ratio,
                    "time_of_day": time_of_day,
                    "is_weekend": is_weekend,
                    "device_type": event["device_type"],
                    "country": event["country"]
                })
            
            # Step 3: Calculate user listening stats
            # Group by date and user
            user_date_events = {}
            for event in enriched_events:
                date = event["date"]
                user_id = event["user_id"]
                key = (date, user_id)
                
                if key not in user_date_events:
                    user_date_events[key] = []
                
                user_date_events[key].append(event)
            
            # Calculate stats for each user/date
            user_stats = []
            for (date, user_id), events in user_date_events.items():
                total_tracks = len(events)
                total_ms_played = sum(event["ms_played"] for event in events)
                avg_track_length = total_ms_played // total_tracks if total_tracks > 0 else 0
                
                # Count by time of day
                morning_plays = sum(1 for event in events if event["time_of_day"] == "morning")
                afternoon_plays = sum(1 for event in events if event["time_of_day"] == "afternoon")
                evening_plays = sum(1 for event in events if event["time_of_day"] == "evening")
                night_plays = sum(1 for event in events if event["time_of_day"] == "night")
                
                # Count by weekday/weekend
                weekday_plays = sum(1 for event in events if not event["is_weekend"])
                weekend_plays = sum(1 for event in events if event["is_weekend"])
                
                # Find most played track and artist
                track_plays = {}
                for event in events:
                    track_id = event["track_id"]
                    if track_id not in track_plays:
                        track_plays[track_id] = 0
                    track_plays[track_id] += 1
                
                most_played_track = max(track_plays.items(), key=lambda x: x[1])[0] if track_plays else None
                
                user_stats.append({
                    "date": date,
                    "user_id": user_id,
                    "tracks_played": total_tracks,
                    "total_ms_played": total_ms_played,
                    "avg_track_length": avg_track_length,
                    "most_played_track": most_played_track,
                    "morning_plays": morning_plays,
                    "afternoon_plays": afternoon_plays,
                    "evening_plays": evening_plays,
                    "night_plays": night_plays,
                    "weekday_plays": weekday_plays,
                    "weekend_plays": weekend_plays
                })
            
            # Step 4: Calculate track popularity stats
            # Group by date and track
            track_date_events = {}
            for event in enriched_events:
                date = event["date"]
                track_id = event["track_id"]
                key = (date, track_id)
                
                if key not in track_date_events:
                    track_date_events[key] = []
                
                track_date_events[key].append(event)
            
            # Calculate stats for each track/date
            track_stats = []
            for (date, track_id), events in track_date_events.items():
                total_streams = len(events)
                unique_listeners = len(set(event["user_id"] for event in events))
                
                # Calculate completion and skip rates
                completion_rates = [event["completion_ratio"] for event in events]
                avg_completion_rate = sum(completion_rates) / len(completion_rates) if completion_rates else 0
                skip_rate = sum(1 for rate in completion_rates if rate < 0.8) / len(completion_rates) if completion_rates else 0
                
                track_stats.append({
                    "date": date,
                    "track_id": track_id,
                    "total_streams": total_streams,
                    "unique_listeners": unique_listeners,
                    "avg_completion_rate": avg_completion_rate,
                    "skip_rate": skip_rate
                })
            
            return {
                "enriched_events": enriched_events,
                "user_stats": user_stats,
                "track_stats": track_stats
            }
        
        transform_metrics = test_utils.run_performance_test(
            test_streaming_transform, 
            iterations=self.iterations,
            warmup=self.warmup
        )
        
        # Store results
        self.results["scio_simulation"] = {
            "streaming_transform": {
                "records": len(self.test_data["streaming_history"]),
                "avg_time": transform_metrics["avg_time"],
                "min_time": transform_metrics["min_time"],
                "max_time": transform_metrics["max_time"],
                "records_per_sec": len(self.test_data["streaming_history"]) / transform_metrics["avg_time"]
            }
        }
    
    def save_results(self, report_file: str = DEFAULT_REPORT_FILE):
        """
        Save the performance test results to a file.
        
        Args:
            report_file: Path to save the performance report
        """
        logger.info(f"Saving performance test results to {report_file}...")
        
        # Add metadata to results
        report = {
            "timestamp": datetime.now().isoformat(),
            "component": self.component,
            "iterations": self.iterations,
            "test_size": self.test_size,
            "warmup": self.warmup,
            "results": self.results
        }
        
        # Save to file
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)
        
        logger.info("Performance test results saved successfully.")


def main():
    """Main function to run the performance tests."""
    parser = argparse.ArgumentParser(description="Run performance tests on the Spotify data pipeline")
    
    parser.add_argument("--test-component", dest="component", type=str, default=DEFAULT_COMPONENT,
                        choices=["db", "kafka", "processing", "scio", "all"],
                        help=f"Component to test (db, kafka, processing, scio, all) [default: {DEFAULT_COMPONENT}]")
    
    parser.add_argument("--iterations", type=int, default=DEFAULT_ITERATIONS,
                        help=f"Number of test iterations to run [default: {DEFAULT_ITERATIONS}]")
    
    parser.add_argument("--test-size", type=str, default=DEFAULT_TEST_SIZE,
                        choices=["tiny", "small", "medium"],
                        help=f"Size of test data to use [default: {DEFAULT_TEST_SIZE}]")
    
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP,
                        help=f"Number of warmup iterations [default: {DEFAULT_WARMUP}]")
    
    parser.add_argument("--report-file", type=str, default=DEFAULT_REPORT_FILE,
                        help=f"Path to save the performance report [default: {DEFAULT_REPORT_FILE}]")
    
    args = parser.parse_args()
    
    # Run the performance tests
    try:
        tester = PerformanceTester(
            component=args.component,
            iterations=args.iterations,
            test_size=args.test_size,
            warmup=args.warmup
        )
        
        tester.setup()
        tester.run_tests()
        tester.save_results(args.report_file)
        
    except Exception as e:
        logger.error(f"Error running performance tests: {e}")
        return 1
    finally:
        try:
            tester.teardown()
        except:
            pass
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 