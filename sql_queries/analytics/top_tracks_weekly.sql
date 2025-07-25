-- Top Tracks Weekly Analysis
-- This query identifies the most popular tracks based on play count and unique listeners
-- for the previous week, with trending score calculation

WITH weekly_plays AS (
    SELECT 
        t.track_id,
        t.name AS track_name,
        a.artist_id,
        a.name AS artist_name,
        al.album_id,
        al.name AS album_name,
        COUNT(*) AS play_count,
        COUNT(DISTINCT sh.user_id) AS unique_listeners,
        SUM(sh.ms_played) / 60000.0 AS total_minutes_played,
        AVG(sh.ms_played * 1.0 / t.duration_ms) AS avg_completion_rate,
        SUM(CASE WHEN sh.skipped THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS skip_rate,
        -- Calculate trending score: combines play count, unique listeners, and completion rate
        -- with recency bias (more recent plays weighted higher)
        COUNT(*) * 0.4 + 
        COUNT(DISTINCT sh.user_id) * 0.3 + 
        AVG(sh.ms_played * 1.0 / t.duration_ms) * 100 * 0.3 AS trending_score
    FROM 
        processed.streaming_history sh
    JOIN 
        public.tracks t ON sh.track_id = t.track_id
    JOIN 
        public.artists a ON t.artist_id = a.artist_id
    JOIN 
        public.albums al ON t.album_id = al.album_id
    WHERE 
        sh.played_at >= (CURRENT_DATE - INTERVAL '7 days')
        AND sh.played_at < CURRENT_DATE
    GROUP BY 
        t.track_id, t.name, a.artist_id, a.name, al.album_id, al.name
),
previous_week_plays AS (
    SELECT 
        t.track_id,
        COUNT(*) AS prev_play_count
    FROM 
        processed.streaming_history sh
    JOIN 
        public.tracks t ON sh.track_id = t.track_id
    WHERE 
        sh.played_at >= (CURRENT_DATE - INTERVAL '14 days')
        AND sh.played_at < (CURRENT_DATE - INTERVAL '7 days')
    GROUP BY 
        t.track_id
)

SELECT 
    wp.track_id,
    wp.track_name,
    wp.artist_name,
    wp.album_name,
    wp.play_count,
    wp.unique_listeners,
    wp.total_minutes_played,
    ROUND(wp.avg_completion_rate * 100, 2) AS completion_percentage,
    ROUND(wp.skip_rate * 100, 2) AS skip_percentage,
    ROUND(wp.trending_score, 2) AS trending_score,
    -- Calculate week-over-week change
    COALESCE(wp.play_count - pwp.prev_play_count, wp.play_count) AS weekly_change,
    CASE 
        WHEN pwp.prev_play_count IS NULL OR pwp.prev_play_count = 0 THEN 'New'
        WHEN (wp.play_count - pwp.prev_play_count) * 100.0 / pwp.prev_play_count >= 50 THEN 'Trending Up'
        WHEN (wp.play_count - pwp.prev_play_count) * 100.0 / pwp.prev_play_count <= -30 THEN 'Falling'
        ELSE 'Stable'
    END AS trend_status
FROM 
    weekly_plays wp
LEFT JOIN 
    previous_week_plays pwp ON wp.track_id = pwp.track_id
ORDER BY 
    wp.trending_score DESC
LIMIT 100; 