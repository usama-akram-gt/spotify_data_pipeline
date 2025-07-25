{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH user_play_counts AS (
    SELECT
        user_id,
        COUNT(*) AS total_plays,
        COUNT(DISTINCT track_id) AS unique_tracks_played,
        SUM(CAST(ms_played AS FLOAT) / 60000.0) AS total_minutes_played
    FROM {{ source('raw', 'streaming_history') }}
    GROUP BY user_id
),

user_favorite_artists AS (
    SELECT
        u.user_id,
        a.artist_id AS favorite_artist_id,
        a.name AS favorite_artist_name,
        COUNT(*) AS play_count,
        ROW_NUMBER() OVER (PARTITION BY u.user_id ORDER BY COUNT(*) DESC) AS artist_rank
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    JOIN {{ source('raw', 'artists') }} a ON t.artist_id = a.artist_id
    GROUP BY u.user_id, a.artist_id, a.name
),

user_favorite_genres AS (
    SELECT 
        u.user_id,
        UNNEST(a.genres) AS genre,
        COUNT(*) AS plays,
        ROW_NUMBER() OVER (PARTITION BY u.user_id ORDER BY COUNT(*) DESC) AS genre_rank
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    JOIN {{ source('raw', 'artists') }} a ON t.artist_id = a.artist_id
    WHERE a.genres IS NOT NULL AND array_length(a.genres, 1) > 0
    GROUP BY u.user_id, UNNEST(a.genres)
),

daily_listening AS (
    SELECT
        user_id,
        DATE(played_at) AS activity_date,
        SUM(CAST(ms_played AS FLOAT) / 60000.0) AS daily_minutes
    FROM {{ source('raw', 'streaming_history') }}
    WHERE played_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY user_id, DATE(played_at)
),

daily_stats AS (
    SELECT
        user_id,
        COUNT(DISTINCT activity_date) AS active_days_last_month,
        AVG(daily_minutes) AS avg_daily_minutes
    FROM daily_listening
    GROUP BY user_id
),

consecutive_days AS (
    SELECT 
        user_id,
        activity_date,
        activity_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date) AS date_group
    FROM (
        SELECT DISTINCT user_id, activity_date
        FROM daily_listening
    ) AS distinct_dates
),

streak_calculation AS (
    SELECT
        user_id,
        COUNT(*) AS streak_length,
        MIN(activity_date) AS streak_start,
        MAX(activity_date) AS streak_end,
        CASE
            WHEN MAX(activity_date) = CURRENT_DATE - INTERVAL '1 day' THEN TRUE
            ELSE FALSE
        END AS is_current_streak
    FROM consecutive_days
    GROUP BY user_id, date_group
)

SELECT
    upc.user_id,
    upc.unique_tracks_played,
    upc.total_minutes_played,
    upc.total_plays,
    ufa.favorite_artist_id,
    ufg.genre AS favorite_genre,
    ds.active_days_last_month,
    ds.avg_daily_minutes,
    COALESCE(MAX(CASE WHEN sc.is_current_streak THEN sc.streak_length ELSE 0 END), 0) AS listening_streak,
    CURRENT_TIMESTAMP AS last_updated
FROM user_play_counts upc
LEFT JOIN user_favorite_artists ufa ON upc.user_id = ufa.user_id AND ufa.artist_rank = 1
LEFT JOIN user_favorite_genres ufg ON upc.user_id = ufg.user_id AND ufg.genre_rank = 1
LEFT JOIN daily_stats ds ON upc.user_id = ds.user_id
LEFT JOIN streak_calculation sc ON upc.user_id = sc.user_id
GROUP BY
    upc.user_id,
    upc.unique_tracks_played,
    upc.total_minutes_played,
    upc.total_plays,
    ufa.favorite_artist_id,
    ufg.genre,
    ds.active_days_last_month,
    ds.avg_daily_minutes 