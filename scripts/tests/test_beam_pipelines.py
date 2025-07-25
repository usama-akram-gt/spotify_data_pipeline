#!/usr/bin/env python
"""
Test Beam Pipelines
This module contains tests for the Apache Beam pipelines.
"""

import os
import sys
import unittest
import tempfile
import pandas as pd
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

# Add scripts directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from beam_pipelines.streaming_history_transform import (
    ParseStreamingHistory,
    ComputeUserStats,
    ComputeTrackPopularity
)


class TestStreamingHistoryTransform(unittest.TestCase):
    """Tests for the streaming history transform pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample streaming history data
        self.streaming_data = [
            {
                'event_id': '1',
                'user_id': 'user1',
                'track_id': 'track1',
                'artist_id': 'artist1',
                'played_at': '2023-01-01T10:30:00',
                'ms_played': 180000,
                'track_name': 'Test Track 1',
                'artist_name': 'Test Artist 1',
                'completed': True,
                'duration_ms': 200000,
                'explicit': False,
                'track_popularity': 80,
                'genres': '["pop", "dance"]',
                'artist_popularity': 85
            },
            {
                'event_id': '2',
                'user_id': 'user1',
                'track_id': 'track2',
                'artist_id': 'artist2',
                'played_at': '2023-01-01T14:45:00',
                'ms_played': 120000,
                'track_name': 'Test Track 2',
                'artist_name': 'Test Artist 2',
                'completed': False,
                'duration_ms': 240000,
                'explicit': True,
                'track_popularity': 70,
                'genres': '["rock", "alternative"]',
                'artist_popularity': 75
            },
            {
                'event_id': '3',
                'user_id': 'user2',
                'track_id': 'track1',
                'artist_id': 'artist1',
                'played_at': '2023-01-01T20:15:00',
                'ms_played': 190000,
                'track_name': 'Test Track 1',
                'artist_name': 'Test Artist 1',
                'completed': True,
                'duration_ms': 200000,
                'explicit': False,
                'track_popularity': 80,
                'genres': '["pop", "dance"]',
                'artist_popularity': 85
            }
        ]
        
        # Create a temporary CSV file with sample data
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_file = os.path.join(self.temp_dir.name, 'streaming_history.csv')
        pd.DataFrame(self.streaming_data).to_csv(self.temp_file, index=False)

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    @pytest.mark.parametrize("input_data,expected_data", [
        (
            # Single event
            [
                {
                    'event_id': '1',
                    'user_id': 'user1',
                    'track_id': 'track1',
                    'played_at': '2023-01-01T10:30:00',
                    'ms_played': 180000,
                    'duration_ms': 200000,
                    'completed': True,
                }
            ],
            # Expected parsed result
            [
                {
                    'event_id': '1',
                    'user_id': 'user1',
                    'track_id': 'track1',
                    'played_at': '2023-01-01T10:30:00',
                    'date': '2023-01-01',
                    'hour': 10,
                    'time_of_day': 'morning',
                    'is_weekend': False,
                    'ms_played': 180000,
                    'duration_ms': 200000,
                    'completion_ratio': 0.9,
                    'completed': True,
                }
            ]
        )
    ])
    @pytest.mark.dependency
    def test_parse_streaming_history(self, input_data, expected_data):
        """Test parsing of streaming history records."""
        with TestPipeline() as p:
            input_pcoll = p | "Create test input" >> beam.Create(input_data)
            output_pcoll = input_pcoll | "Parse streaming history" >> beam.ParDo(ParseStreamingHistory())
            assert_that(output_pcoll, equal_to(expected_data))

    @pytest.mark.production
    def test_compute_user_stats_production(self):
        """Test computation of user statistics with larger dataset (production test)."""
        # Generate a larger dataset for production testing
        large_data = []
        for i in range(1000):
            # Copy and modify sample data to create a large dataset
            for record in self.streaming_data:
                new_record = record.copy()
                new_record['event_id'] = f"{record['event_id']}-{i}"
                new_record['played_at'] = f"2023-01-{(i % 30) + 1:02d}T{i % 24:02d}:30:00"
                large_data.append(new_record)
        
        # Process with the pipeline and verify results
        # This is a simplified test that just checks if the pipeline runs without error
        # In a real production test, you would verify the results more thoroughly
        with TestPipeline() as p:
            parsed_data = (
                p 
                | "Create test input" >> beam.Create(large_data)
                | "Parse streaming history" >> beam.ParDo(ParseStreamingHistory())
            )
            
            user_stats = parsed_data | "Compute user stats" >> beam.ParDo(ComputeUserStats())
            
            # Just check that we get some output without errors
            def count_outputs(elements):
                return len(list(elements))
            
            result = user_stats | "Count outputs" >> beam.CombineGlobally(count_outputs)
            assert_that(result, lambda x: x[0] > 0)

    @pytest.mark.dependency
    def test_compute_track_popularity(self):
        """Test computation of track popularity metrics."""
        # Parse the sample data
        parsed_data = [ParseStreamingHistory().process(record) for record in self.streaming_data]
        
        # Expected output after computing track popularity
        expected_track_popularity = [
            {
                'date': '2023-01-01',
                'track_id': 'track1',
                'track_name': 'Test Track 1',
                'artist_id': 'artist1',
                'artist_name': 'Test Artist 1',
                'total_streams': 2,
                'unique_listeners': 2,
                'total_time_ms': 370000,
                'avg_completion_rate': 0.95,
                'skip_rate': 0.0,
                'morning_streams': 1,
                'afternoon_streams': 0,
                'evening_streams': 1,
                'night_streams': 0,
                'weekday_streams': 2,
                'weekend_streams': 0
            }
        ]
        
        with TestPipeline() as p:
            input_pcoll = p | "Create test input" >> beam.Create([e for e in parsed_data if e['track_id'] == 'track1'])
            
            output_pcoll = (
                input_pcoll 
                | "Group by track" >> beam.GroupBy(
                    lambda x: (x['date'], x['track_id'])
                ).aggregate_field(
                    lambda x: x,
                    lambda elements: ComputeTrackPopularity().process(list(elements)),
                    'track_popularity'
                )
                | "Extract values" >> beam.Values()
            )
            
            # Check if the output matches expected for track1
            assert_that(output_pcoll, equal_to(expected_track_popularity))


if __name__ == '__main__':
    pytest.main() 