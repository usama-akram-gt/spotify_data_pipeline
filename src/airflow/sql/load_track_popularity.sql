-- Load track popularity metrics into analytics database
-- This query loads the processed track popularity data from the Scio pipeline
-- The date parameters will be interpolated by Airflow

INSERT INTO spotify_analytics.track_popularity (
    date,
    track_id,
    track_name,
    artist_id,
    artist_name,
    total_streams,
    unique_listeners,
    total_time_ms,
    avg_completion_rate,
    skip_rate,
    morning_streams,
    afternoon_streams,
    evening_streams,
    night_streams,
    weekday_streams,
    weekend_streams,
    process_date
)
SELECT 
    tp.date,
    tp.track_id,
    tp.track_name,
    tp.artist_id,
    tp.artist_name,
    tp.total_streams,
    tp.unique_listeners,
    tp.total_time_ms,
    tp.avg_completion_rate,
    tp.skip_rate,
    tp.morning_streams,
    tp.afternoon_streams,
    tp.evening_streams,
    tp.night_streams,
    tp.weekday_streams,
    tp.weekend_streams,
    '{{ ds }}' AS process_date
FROM 
    spotify_analytics.staging_track_popularity tp
WHERE 
    tp.date = '{{ ds }}';

-- After successful insertion, clean up staging table
DELETE FROM spotify_analytics.staging_track_popularity 
WHERE date = '{{ ds }}'; 