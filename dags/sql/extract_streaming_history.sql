-- SQL script to extract streaming history data from raw database
-- This script extracts streaming history data for the previous day
-- and prepares it for transformation and loading into analytics tables

-- Extract streaming history data
INSERT INTO spotify_analytics.staging.streaming_history_extract
(
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
    processed_date
)
SELECT 
    sh.event_id,
    sh.user_id,
    sh.track_id,
    sh.timestamp,
    sh.ms_played,
    sh.reason_start,
    sh.reason_end,
    sh.shuffle,
    sh.skipped,
    sh.platform,
    sh.country,
    sh.ip_address,
    CURRENT_DATE as processed_date
FROM 
    spotify_raw.raw.streaming_history sh
WHERE 
    DATE(sh.timestamp) = CURRENT_DATE - INTERVAL '1 day'
    AND NOT EXISTS (
        -- Avoid duplicate processing
        SELECT 1 
        FROM spotify_analytics.staging.streaming_history_extract she
        WHERE she.event_id = sh.event_id
    )
ON CONFLICT (event_id) DO NOTHING;

-- Log the extraction metrics
INSERT INTO spotify_analytics.analytics.etl_logs
(
    etl_task,
    table_name,
    start_datetime,
    end_datetime,
    records_processed,
    status,
    log_message
)
SELECT
    'extract_streaming_history',
    'streaming_history',
    CURRENT_TIMESTAMP - INTERVAL '5 minutes', -- approximate start time
    CURRENT_TIMESTAMP,
    COUNT(*),
    'SUCCESS',
    'Extracted streaming history for ' || (CURRENT_DATE - INTERVAL '1 day')::TEXT
FROM
    spotify_analytics.staging.streaming_history_extract
WHERE
    processed_date = CURRENT_DATE;

-- Create staging table if it doesn't exist
CREATE TABLE IF NOT EXISTS spotify_analytics.staging.streaming_history_extract (
    event_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    track_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    ms_played INT,
    reason_start VARCHAR(50),
    reason_end VARCHAR(50),
    shuffle BOOLEAN,
    skipped BOOLEAN,
    platform VARCHAR(50),
    country VARCHAR(50),
    ip_address VARCHAR(50),
    processed_date DATE NOT NULL,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create ETL logs table if it doesn't exist
CREATE TABLE IF NOT EXISTS spotify_analytics.analytics.etl_logs (
    log_id SERIAL PRIMARY KEY,
    etl_task VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    start_datetime TIMESTAMP NOT NULL,
    end_datetime TIMESTAMP NOT NULL,
    records_processed INT,
    status VARCHAR(20) NOT NULL,
    log_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Extract streaming history data from raw database
-- This query extracts the streaming events from the raw database for processing by the Scio pipeline
-- The date parameters will be interpolated by Airflow

WITH date_range AS (
    SELECT 
        '{{ ds }}' AS process_date,
        '{{ macros.ds_add(ds, -1) }}' AS prev_date
)
SELECT 
    sh.event_id,
    sh.user_id,
    sh.track_id,
    sh.artist_id,
    sh.played_at,
    sh.ms_played,
    sh.track_name,
    sh.artist_name,
    CASE 
        WHEN sh.ms_played >= t.duration_ms * 0.9 THEN TRUE
        ELSE FALSE
    END AS completed,
    t.duration_ms,
    t.explicit,
    t.popularity AS track_popularity,
    a.genres,
    a.popularity AS artist_popularity
FROM 
    spotify_raw.streaming_history sh
JOIN 
    spotify_raw.tracks t ON sh.track_id = t.track_id
JOIN 
    spotify_raw.artists a ON sh.artist_id = a.artist_id
CROSS JOIN 
    date_range
WHERE 
    DATE(sh.played_at) = date_range.process_date
ORDER BY 
    sh.played_at; 