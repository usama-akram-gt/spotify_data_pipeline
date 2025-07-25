{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

WITH track_play_counts AS (
    SELECT
        t.track_id,
        t.name AS track_name,
        a.artist_id,
        a.name AS artist_name,
        COUNT(*) AS total_plays,
        COUNT(DISTINCT u.user_id) AS unique_listeners
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    JOIN {{ source('raw', 'artists') }} a ON t.artist_id = a.artist_id
    GROUP BY t.track_id, t.name, a.artist_id, a.name
),

track_skips AS (
    SELECT
        t.track_id,
        SUM(CASE WHEN u.skipped = TRUE THEN 1 ELSE 0 END) AS skip_count,
        COUNT(*) AS total_count
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    GROUP BY t.track_id
),

track_completion AS (
    SELECT
        t.track_id,
        AVG(u.ms_played::FLOAT / t.duration_ms) AS avg_completion_rate
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    GROUP BY t.track_id
),

last_30_days_plays AS (
    SELECT
        t.track_id,
        COUNT(*) AS recent_plays
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    WHERE u.played_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY t.track_id
),

previous_30_days_plays AS (
    SELECT
        t.track_id,
        COUNT(*) AS previous_plays
    FROM {{ source('raw', 'streaming_history') }} u
    JOIN {{ source('raw', 'tracks') }} t ON u.track_id = t.track_id
    WHERE u.played_at BETWEEN CURRENT_DATE - INTERVAL '60 days' AND CURRENT_DATE - INTERVAL '30 days'
    GROUP BY t.track_id
)

SELECT
    tpc.track_id,
    tpc.track_name,
    tpc.artist_id,
    tpc.artist_name,
    tpc.total_plays,
    tpc.unique_listeners,
    COALESCE(ts.skip_count, 0)::FLOAT / NULLIF(ts.total_count, 0) AS skip_rate,
    COALESCE(tc.avg_completion_rate, 0) AS avg_completion_rate,
    COALESCE(last30.recent_plays, 0) AS plays_last_30_days,
    COALESCE(prev30.previous_plays, 0) AS plays_previous_30_days,
    CASE
        WHEN COALESCE(prev30.previous_plays, 0) = 0 THEN 0
        ELSE (COALESCE(last30.recent_plays, 0) - COALESCE(prev30.previous_plays, 0))::FLOAT / COALESCE(prev30.previous_plays, 1) * 100
    END AS growth_percent,
    -- Calculate trending score: combination of recent plays, low skip rate, and high completion
    (
        COALESCE(last30.recent_plays, 0) * 0.6 +
        (1 - COALESCE(ts.skip_count, 0)::FLOAT / NULLIF(ts.total_count, 0)) * 100 * 0.2 +
        COALESCE(tc.avg_completion_rate, 0) * 100 * 0.2
    ) AS trending_score,
    CURRENT_TIMESTAMP AS last_updated
FROM track_play_counts tpc
LEFT JOIN track_skips ts ON tpc.track_id = ts.track_id
LEFT JOIN track_completion tc ON tpc.track_id = tc.track_id
LEFT JOIN last_30_days_plays last30 ON tpc.track_id = last30.track_id
LEFT JOIN previous_30_days_plays prev30 ON tpc.track_id = prev30.track_id 