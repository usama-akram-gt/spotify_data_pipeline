-- Load user listening statistics into analytics database
-- This query loads the processed user listening statistics from the Scio pipeline
-- The date parameters will be interpolated by Airflow

INSERT INTO spotify_analytics.user_listening_stats (
    date,
    user_id,
    total_tracks,
    total_time_ms,
    avg_track_duration,
    morning_tracks,
    afternoon_tracks,
    evening_tracks,
    night_tracks,
    weekday_tracks,
    weekend_tracks,
    distinct_artists,
    top_artist_id,
    top_genre,
    explicit_tracks,
    completion_rate,
    process_date
)
SELECT 
    uls.date,
    uls.user_id,
    uls.total_tracks,
    uls.total_time_ms,
    uls.avg_track_duration,
    uls.morning_tracks,
    uls.afternoon_tracks,
    uls.evening_tracks,
    uls.night_tracks,
    uls.weekday_tracks,
    uls.weekend_tracks,
    uls.distinct_artists,
    uls.top_artist_id,
    uls.top_genre,
    uls.explicit_tracks,
    uls.completion_rate,
    '{{ ds }}' AS process_date
FROM 
    spotify_analytics.staging_user_listening_stats uls
WHERE 
    uls.date = '{{ ds }}';

-- After successful insertion, clean up staging table
DELETE FROM spotify_analytics.staging_user_listening_stats 
WHERE date = '{{ ds }}'; 