#!/usr/bin/env python3
"""
Testing utilities for the Spotify data pipeline.

This module provides functions for testing different components of the pipeline:
- Database testing
- Kafka testing
- Data validation
- Performance testing
"""

import json
import logging
import os
import time
import unittest
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Union
import random
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer, Consumer, KafkaException

from scripts.utils import db_utils, kafka_utils, config

# Configure logging
logger = logging.getLogger(__name__)
logger = config.configure_logging(__name__)

# Test data generation sizes
TEST_SIZES = {
    "tiny": {
        "users": 10,
        "artists": 5,
        "albums_per_artist": 2,
        "tracks_per_album": 5,
        "streaming_history_per_user": 20
    },
    "small": {
        "users": 50,
        "artists": 10,
        "albums_per_artist": 3,
        "tracks_per_album": 8,
        "streaming_history_per_user": 50
    },
    "medium": {
        "users": 100,
        "artists": 20,
        "albums_per_artist": 3,
        "tracks_per_album": 10,
        "streaming_history_per_user": 100
    }
}


class DatabaseTestHelper:
    """Helper class for database testing."""
    
    def __init__(self, db_type: str = "raw", test_schema: str = "test"):
        """
        Initialize the database test helper.
        
        Args:
            db_type: Database type ('raw' or 'analytics')
            test_schema: Schema to use for testing
        """
        self.db_config = config.get_db_config(db_type)
        self.test_schema = test_schema
        self.conn = None
        self.setup_complete = False
    
    def connect(self) -> None:
        """Connect to the database."""
        self.conn = db_utils.get_db_connection(self.db_config)
    
    def disconnect(self) -> None:
        """Disconnect from the database."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def setup_test_schema(self) -> None:
        """Create a test schema for isolation."""
        if not self.conn:
            self.connect()
        
        with self.conn.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {self.test_schema} CASCADE")
            cursor.execute(f"CREATE SCHEMA {self.test_schema}")
            self.conn.commit()
        
        self.setup_complete = True
    
    def teardown_test_schema(self) -> None:
        """Drop the test schema."""
        if not self.conn:
            self.connect()
        
        with self.conn.cursor() as cursor:
            cursor.execute(f"DROP SCHEMA IF EXISTS {self.test_schema} CASCADE")
            self.conn.commit()
        
        self.setup_complete = False
    
    def create_table(self, table_name: str) -> None:
        """
        Create a table in the test schema.
        
        Args:
            table_name: Table name
        """
        if not self.setup_complete:
            self.setup_test_schema()
        
        schema = config.get_table_schema(table_name)
        create_sql = config.get_create_table_sql(table_name)
        create_sql = create_sql.replace(f"CREATE TABLE IF NOT EXISTS {table_name}",
                                        f"CREATE TABLE IF NOT EXISTS {self.test_schema}.{table_name}")
        
        with self.conn.cursor() as cursor:
            cursor.execute(create_sql)
            self.conn.commit()
    
    def populate_test_data(self, table_name: str, data: List[Dict]) -> None:
        """
        Insert test data into a table.
        
        Args:
            table_name: Table name
            data: List of dictionaries with data to insert
        """
        if not data:
            return
        
        columns = list(data[0].keys())
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {self.test_schema}.{table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        values = [[row.get(col) for col in columns] for row in data]
        
        with self.conn.cursor() as cursor:
            db_utils.execute_batch_insert(cursor, insert_sql, values)
            self.conn.commit()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """
        Execute a query and return the results as a list of dictionaries.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            List of dictionaries with query results
        """
        if not self.conn:
            self.connect()
        
        # Replace table references with schema-qualified ones
        for table in config.SCHEMAS.keys():
            query = query.replace(f" {table} ", f" {self.test_schema}.{table} ")
            query = query.replace(f"FROM {table}", f"FROM {self.test_schema}.{table}")
            query = query.replace(f"JOIN {table}", f"JOIN {self.test_schema}.{table}")
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return [dict(row) for row in results]


class KafkaTestHelper:
    """Helper class for Kafka testing."""
    
    def __init__(self, topic_prefix: str = "test"):
        """
        Initialize the Kafka test helper.
        
        Args:
            topic_prefix: Prefix for test topics
        """
        self.topic_prefix = topic_prefix
        self.producer = None
        self.consumer = None
        self.test_topics = {}
        
        for key, topic in config.KAFKA_CONFIG["topics"].items():
            self.test_topics[key] = f"{self.topic_prefix}.{topic}"
    
    def get_test_topic(self, topic_key: str) -> str:
        """
        Get the test topic name for a given key.
        
        Args:
            topic_key: Topic key
            
        Returns:
            Test topic name
        """
        if topic_key not in self.test_topics:
            raise ValueError(f"Invalid topic key: {topic_key}")
        return self.test_topics[topic_key]
    
    def setup(self) -> None:
        """Set up Kafka test environment."""
        self.producer = kafka_utils.get_kafka_producer(client_id=f"{self.topic_prefix}-producer")
        self.consumer = kafka_utils.get_kafka_consumer(group_id=f"{self.topic_prefix}-consumer")
        
        # Create test topics
        kafka_utils.create_topics_if_not_exist(
            config.KAFKA_CONFIG["bootstrap_servers"],
            list(self.test_topics.values()),
            num_partitions=1,
            replication_factor=1
        )
    
    def teardown(self) -> None:
        """Clean up Kafka test environment."""
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        
        if self.producer:
            self.producer.flush()
            self.producer = None
    
    def produce_test_messages(self, topic_key: str, messages: List[Dict], key_field: str = None) -> None:
        """
        Produce test messages to a Kafka topic.
        
        Args:
            topic_key: Topic key
            messages: List of message dictionaries
            key_field: Field to use as message key (if any)
        """
        if not self.producer:
            self.setup()
        
        topic = self.get_test_topic(topic_key)
        
        for message in messages:
            key = message.get(key_field) if key_field else None
            kafka_utils.produce_message(self.producer, topic, message, key)
        
        self.producer.flush()
    
    def consume_test_messages(self, topic_key: str, timeout: float = 5.0, 
                             num_messages: int = -1) -> List[Dict]:
        """
        Consume messages from a test topic.
        
        Args:
            topic_key: Topic key
            timeout: Timeout in seconds
            num_messages: Number of messages to consume (-1 for all available)
            
        Returns:
            List of message dictionaries
        """
        if not self.consumer:
            self.setup()
        
        topic = self.get_test_topic(topic_key)
        return kafka_utils.consume_messages(
            self.consumer, 
            [topic], 
            timeout=timeout,
            num_messages=num_messages
        )


class TestDataGenerator:
    """Helper class for generating test data."""
    
    def __init__(self, size: str = "tiny"):
        """
        Initialize the test data generator.
        
        Args:
            size: Test data size ('tiny', 'small', 'medium')
        """
        if size not in TEST_SIZES:
            raise ValueError(f"Invalid test size: {size}, must be one of {list(TEST_SIZES.keys())}")
        
        self.size_config = TEST_SIZES[size]
    
    def generate_users(self, count: int = None) -> List[Dict]:
        """
        Generate test user data.
        
        Args:
            count: Number of users to generate (defaults to size config)
            
        Returns:
            List of user dictionaries
        """
        count = count or self.size_config["users"]
        users = []
        
        subscription_tiers = ["free", "premium", "family", "student"]
        countries = ["US", "GB", "CA", "DE", "FR", "JP", "BR", "AU", "MX", "ES"]
        
        for i in range(count):
            user_id = f"test_user_{i}"
            users.append({
                "user_id": user_id,
                "username": f"test_username_{i}",
                "email": f"test_user_{i}@example.com",
                "date_of_birth": (datetime.now() - timedelta(days=random.randint(6570, 25000))).strftime("%Y-%m-%d"),
                "gender": random.choice(["M", "F", "NB", "Other"]),
                "country": random.choice(countries),
                "created_at": (datetime.now() - timedelta(days=random.randint(0, 1000))).strftime("%Y-%m-%d %H:%M:%S"),
                "subscription_tier": random.choice(subscription_tiers)
            })
        
        return users
    
    def generate_artists(self, count: int = None) -> List[Dict]:
        """
        Generate test artist data.
        
        Args:
            count: Number of artists to generate (defaults to size config)
            
        Returns:
            List of artist dictionaries
        """
        count = count or self.size_config["artists"]
        artists = []
        
        genres = ["pop", "rock", "hip-hop", "jazz", "classical", "electronic", "country", "metal"]
        countries = ["US", "GB", "CA", "DE", "FR", "JP", "BR", "AU", "MX", "ES"]
        
        for i in range(count):
            artist_id = f"test_artist_{i}"
            artists.append({
                "artist_id": artist_id,
                "name": f"Test Artist {i}",
                "genre": random.choice(genres),
                "popularity": random.randint(1, 100),
                "founded_year": random.randint(1950, 2020),
                "country": random.choice(countries)
            })
        
        return artists
    
    def generate_albums(self, artists: List[Dict], albums_per_artist: int = None) -> List[Dict]:
        """
        Generate test album data.
        
        Args:
            artists: List of artist dictionaries
            albums_per_artist: Number of albums per artist (defaults to size config)
            
        Returns:
            List of album dictionaries
        """
        albums_per_artist = albums_per_artist or self.size_config["albums_per_artist"]
        albums = []
        
        genres = ["pop", "rock", "hip-hop", "jazz", "classical", "electronic", "country", "metal"]
        
        for artist in artists:
            for i in range(albums_per_artist):
                album_id = f"test_album_{artist['artist_id']}_{i}"
                albums.append({
                    "album_id": album_id,
                    "artist_id": artist["artist_id"],
                    "title": f"Test Album {i} by {artist['name']}",
                    "release_date": (datetime.now() - timedelta(days=random.randint(0, 3650))).strftime("%Y-%m-%d"),
                    "genre": random.choice(genres),
                    "total_tracks": random.randint(5, 15)
                })
        
        return albums
    
    def generate_tracks(self, albums: List[Dict], artists: List[Dict], 
                       tracks_per_album: int = None) -> List[Dict]:
        """
        Generate test track data.
        
        Args:
            albums: List of album dictionaries
            artists: List of artist dictionaries
            tracks_per_album: Number of tracks per album (defaults to size config)
            
        Returns:
            List of track dictionaries
        """
        tracks_per_album = tracks_per_album or self.size_config["tracks_per_album"]
        tracks = []
        
        # Create artist lookup
        artist_map = {artist["artist_id"]: artist for artist in artists}
        
        for album in albums:
            artist = artist_map.get(album["artist_id"])
            if not artist:
                continue
                
            for i in range(tracks_per_album):
                track_id = f"test_track_{album['album_id']}_{i}"
                tracks.append({
                    "track_id": track_id,
                    "album_id": album["album_id"],
                    "artist_id": artist["artist_id"],
                    "title": f"Test Track {i} on {album['title']}",
                    "duration_ms": random.randint(120000, 360000),
                    "explicit": random.choice([True, False]),
                    "popularity": random.randint(1, 100),
                    "track_number": i + 1
                })
        
        return tracks
    
    def generate_streaming_history(self, users: List[Dict], tracks: List[Dict],
                                 events_per_user: int = None) -> List[Dict]:
        """
        Generate test streaming history data.
        
        Args:
            users: List of user dictionaries
            tracks: List of track dictionaries
            events_per_user: Number of streaming events per user (defaults to size config)
            
        Returns:
            List of streaming history dictionaries
        """
        events_per_user = events_per_user or self.size_config["streaming_history_per_user"]
        streaming_history = []
        
        reason_starts = ["trackdone", "clickrow", "playbtn", "backbtn", "remote", "appload"]
        reason_ends = ["trackdone", "endplay", "forward", "remote", "backbtn"]
        device_types = ["desktop", "mobile", "tablet", "speaker", "tv", "car"]
        
        for user in users:
            for i in range(events_per_user):
                track = random.choice(tracks)
                played_at = datetime.now() - timedelta(days=random.randint(0, 30), 
                                                     hours=random.randint(0, 23),
                                                     minutes=random.randint(0, 59))
                
                # Sometimes tracks are not played completely
                completion_ratio = random.uniform(0.1, 1.0)
                ms_played = int(track["duration_ms"] * completion_ratio)
                
                streaming_history.append({
                    "event_id": f"test_event_{user['user_id']}_{i}",
                    "user_id": user["user_id"],
                    "track_id": track["track_id"],
                    "played_at": played_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "ms_played": ms_played,
                    "reason_start": random.choice(reason_starts),
                    "reason_end": random.choice(reason_ends),
                    "device_type": random.choice(device_types),
                    "country": user["country"]
                })
        
        return streaming_history
    
    def generate_all_test_data(self) -> Dict[str, List[Dict]]:
        """
        Generate all test data.
        
        Returns:
            Dictionary with all generated test data
        """
        users = self.generate_users()
        artists = self.generate_artists()
        albums = self.generate_albums(artists)
        tracks = self.generate_tracks(albums, artists)
        streaming_history = self.generate_streaming_history(users, tracks)
        
        return {
            "users": users,
            "artists": artists,
            "albums": albums,
            "tracks": tracks,
            "streaming_history": streaming_history
        }


class DataPipelineTestCase(unittest.TestCase):
    """Base class for data pipeline integration tests."""
    
    def setUp(self):
        """Set up the test environment."""
        self.db_helper = DatabaseTestHelper()
        self.kafka_helper = KafkaTestHelper()
        self.data_generator = TestDataGenerator()
        
        # Set up test environment
        self.db_helper.setup_test_schema()
        self.kafka_helper.setup()
        
        # Create required tables
        self.db_helper.create_table("users")
        self.db_helper.create_table("artists")
        self.db_helper.create_table("albums")
        self.db_helper.create_table("tracks")
        self.db_helper.create_table("streaming_history")
        self.db_helper.create_table("user_listening_stats")
        self.db_helper.create_table("track_popularity")
    
    def tearDown(self):
        """Clean up the test environment."""
        self.kafka_helper.teardown()
        self.db_helper.teardown_test_schema()
        self.db_helper.disconnect()
    
    def load_test_data(self):
        """Load test data into the database."""
        test_data = self.data_generator.generate_all_test_data()
        
        for table_name, data in test_data.items():
            self.db_helper.populate_test_data(table_name, data)
        
        return test_data


def run_performance_test(func: Callable, iterations: int = 10, 
                         warmup: int = 1) -> Dict[str, float]:
    """
    Run a performance test on a function.
    
    Args:
        func: Function to test
        iterations: Number of iterations to run
        warmup: Number of warmup iterations
        
    Returns:
        Dictionary with performance metrics
    """
    # Run warmup iterations
    for _ in range(warmup):
        func()
    
    # Measure performance
    times = []
    start_total = time.time()
    
    for _ in range(iterations):
        start = time.time()
        func()
        end = time.time()
        times.append(end - start)
    
    end_total = time.time()
    
    # Calculate metrics
    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)
    total_time = end_total - start_total
    
    return {
        "avg_time": avg_time,
        "min_time": min_time,
        "max_time": max_time,
        "total_time": total_time,
        "iterations": iterations
    }


def validate_data_integrity(expected: List[Dict], actual: List[Dict], 
                          key_field: str) -> Dict[str, Any]:
    """
    Validate data integrity between expected and actual data.
    
    Args:
        expected: Expected data
        actual: Actual data
        key_field: Field to use as key for comparison
        
    Returns:
        Dictionary with validation results
    """
    expected_map = {item[key_field]: item for item in expected}
    actual_map = {item[key_field]: item for item in actual}
    
    missing = set(expected_map.keys()) - set(actual_map.keys())
    extra = set(actual_map.keys()) - set(expected_map.keys())
    common = set(expected_map.keys()) & set(actual_map.keys())
    
    # Check for differences in common records
    differences = {}
    for key in common:
        expected_item = expected_map[key]
        actual_item = actual_map[key]
        
        field_diffs = {}
        for field in expected_item:
            if field in actual_item and expected_item[field] != actual_item[field]:
                field_diffs[field] = {
                    "expected": expected_item[field],
                    "actual": actual_item[field]
                }
        
        if field_diffs:
            differences[key] = field_diffs
    
    return {
        "total_expected": len(expected),
        "total_actual": len(actual),
        "missing": list(missing),
        "extra": list(extra),
        "differences": differences,
        "is_valid": len(missing) == 0 and len(differences) == 0
    }


if __name__ == "__main__":
    """Run the tests."""
    unittest.main() 