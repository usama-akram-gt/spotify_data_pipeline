{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH artist_play_counts AS (
    SELECT
        a.artist_id,
        a.name AS artist_name,
        COUNT(*) AS total_plays,
        COUNT(DISTINCT u.user_id) AS unique_listeners
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    JOIN {{ source('raw', 'artists') }} a ON t.artist_id = a.artist_id
    GROUP BY a.artist_id, a.name
),

artist_track_popularity AS (
    SELECT
        a.artist_id,
        t.track_id,
        t.name AS track_name,
        COUNT(*) AS play_count,
        ROW_NUMBER() OVER (PARTITION BY a.artist_id ORDER BY COUNT(*) DESC) AS track_rank
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    JOIN {{ source('raw', 'artists') }} a ON t.artist_id = a.artist_id
    GROUP BY a.artist_id, t.track_id, t.name
),

last_30_days_plays AS (
    SELECT
        a.artist_id,
        COUNT(*) AS recent_plays
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    JOIN {{ source('raw', 'artists') }} a ON t.artist_id = a.artist_id
    WHERE u.played_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY a.artist_id
),

previous_30_days_plays AS (
    SELECT
        a.artist_id,
        COUNT(*) AS previous_plays
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    JOIN {{ source('raw', 'artists') }} a ON t.artist_id = a.artist_id
    WHERE u.played_at BETWEEN CURRENT_DATE - INTERVAL '60 days' AND CURRENT_DATE - INTERVAL '30 days'
    GROUP BY a.artist_id
)

SELECT
    apc.artist_id,
    apc.artist_name,
    apc.total_plays,
    apc.unique_listeners,
    atp.track_id AS most_popular_track,
    atp.track_name AS most_popular_track_name,
    COALESCE(last30.recent_plays, 0) AS plays_last_30_days,
    COALESCE(prev30.previous_plays, 0) AS plays_previous_30_days,
    CASE
        WHEN COALESCE(prev30.previous_plays, 0) = 0 THEN 0
        ELSE (COALESCE(last30.recent_plays, 0) - COALESCE(prev30.previous_plays, 0))::FLOAT / COALESCE(prev30.previous_plays, 1) * 100
    END AS growth_percent,
    -- Calculate trending score: combination of recent plays and growth
    (
        COALESCE(last30.recent_plays, 0) * 0.7 +
        CASE
            WHEN COALESCE(prev30.previous_plays, 0) = 0 THEN 0
            ELSE (COALESCE(last30.recent_plays, 0) - COALESCE(prev30.previous_plays, 0))::FLOAT / COALESCE(prev30.previous_plays, 1) * 100
        END * 0.3
    ) AS trending_score,
    CURRENT_TIMESTAMP AS last_updated
FROM artist_play_counts apc
LEFT JOIN artist_track_popularity atp ON apc.artist_id = atp.artist_id AND atp.track_rank = 1
LEFT JOIN last_30_days_plays last30 ON apc.artist_id = last30.artist_id
LEFT JOIN previous_30_days_plays prev30 ON apc.artist_id = prev30.artist_id 