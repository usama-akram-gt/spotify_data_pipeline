-- User Listening Patterns Analysis
-- This query analyzes user listening patterns including time of day preferences,
-- genre preferences, session length, and completion rates

WITH user_sessions AS (
    -- Group plays into sessions when they occur within 30 minutes of each other
    SELECT
        user_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS tracks_in_session,
        SUM(ms_played) / 60000.0 AS session_minutes,
        AVG(completion_rate) AS avg_completion_rate
    FROM
        processed.streaming_history
    WHERE
        played_at >= (CURRENT_DATE - INTERVAL '30 days')
    GROUP BY
        user_id, session_id
),
genre_preferences AS (
    -- Calculate genre preferences based on artist genres
    SELECT
        sh.user_id,
        UNNEST(a.genres) AS genre,
        COUNT(*) AS genre_plays
    FROM
        processed.streaming_history sh
    JOIN
        public.tracks t ON sh.track_id = t.track_id
    JOIN
        public.artists a ON t.artist_id = a.artist_id
    WHERE
        sh.played_at >= (CURRENT_DATE - INTERVAL '30 days')
        AND a.genres IS NOT NULL
    GROUP BY
        sh.user_id, genre
),
time_preferences AS (
    -- Analyze preferred listening times
    SELECT
        user_id,
        hour_of_day,
        COUNT(*) AS hour_plays,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY user_id) AS hour_percentage
    FROM
        processed.streaming_history
    WHERE
        played_at >= (CURRENT_DATE - INTERVAL '30 days')
    GROUP BY
        user_id, hour_of_day
),
day_preferences AS (
    -- Analyze preferred listening days
    SELECT
        user_id,
        day_of_week,
        COUNT(*) AS day_plays,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY user_id) AS day_percentage
    FROM
        processed.streaming_history
    WHERE
        played_at >= (CURRENT_DATE - INTERVAL '30 days')
    GROUP BY
        user_id, day_of_week
),
top_genres_by_user AS (
    -- Get top 3 genres per user
    SELECT
        user_id,
        genre,
        genre_plays,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY genre_plays DESC) AS genre_rank
    FROM
        genre_preferences
),
user_summary AS (
    -- Calculate summary statistics by user
    SELECT
        u.user_id,
        u.username,
        u.country,
        u.subscription_type,
        COUNT(DISTINCT sh.played_at::date) AS active_days,
        COUNT(DISTINCT sh.track_id) AS unique_tracks,
        AVG(sh.completion_rate) AS avg_completion_rate,
        AVG(us.session_minutes) AS avg_session_minutes,
        AVG(us.tracks_in_session) AS avg_tracks_per_session,
        COUNT(DISTINCT us.session_id) AS total_sessions
    FROM
        processed.streaming_history sh
    JOIN
        public.users u ON sh.user_id = u.user_id
    LEFT JOIN
        user_sessions us ON sh.user_id = us.user_id AND sh.session_id = us.session_id
    WHERE
        sh.played_at >= (CURRENT_DATE - INTERVAL '30 days')
    GROUP BY
        u.user_id, u.username, u.country, u.subscription_type
)

-- Final result combining all the analytics
SELECT
    us.user_id,
    us.username,
    us.country,
    us.subscription_type,
    us.active_days,
    us.unique_tracks,
    ROUND(us.avg_completion_rate * 100, 2) AS avg_completion_percentage,
    ROUND(us.avg_session_minutes, 2) AS avg_session_minutes,
    ROUND(us.avg_tracks_per_session, 2) AS avg_tracks_per_session,
    us.total_sessions,
    -- Preferred genres
    STRING_AGG(
        CASE WHEN tg.genre_rank <= 3 THEN tg.genre || ' (' || tg.genre_plays || ')' END, 
        ', ' 
        ORDER BY tg.genre_rank
    ) AS top_genres,
    -- Preferred time of day (morning, afternoon, evening, night)
    CASE
        WHEN MAX(CASE WHEN tp.hour_of_day BETWEEN 5 AND 11 THEN tp.hour_percentage ELSE 0 END) >
             MAX(CASE WHEN tp.hour_of_day BETWEEN 12 AND 17 THEN tp.hour_percentage ELSE 0 END) AND
             MAX(CASE WHEN tp.hour_of_day BETWEEN 5 AND 11 THEN tp.hour_percentage ELSE 0 END) >
             MAX(CASE WHEN tp.hour_of_day BETWEEN 18 AND 23 THEN tp.hour_percentage ELSE 0 END) AND
             MAX(CASE WHEN tp.hour_of_day BETWEEN 5 AND 11 THEN tp.hour_percentage ELSE 0 END) >
             MAX(CASE WHEN tp.hour_of_day BETWEEN 0 AND 4 THEN tp.hour_percentage ELSE 0 END)
        THEN 'Morning (5AM-11AM)'
        WHEN MAX(CASE WHEN tp.hour_of_day BETWEEN 12 AND 17 THEN tp.hour_percentage ELSE 0 END) >
             MAX(CASE WHEN tp.hour_of_day BETWEEN 18 AND 23 THEN tp.hour_percentage ELSE 0 END) AND
             MAX(CASE WHEN tp.hour_of_day BETWEEN 12 AND 17 THEN tp.hour_percentage ELSE 0 END) >
             MAX(CASE WHEN tp.hour_of_day BETWEEN 0 AND 4 THEN tp.hour_percentage ELSE 0 END)
        THEN 'Afternoon (12PM-5PM)'
        WHEN MAX(CASE WHEN tp.hour_of_day BETWEEN 18 AND 23 THEN tp.hour_percentage ELSE 0 END) >
             MAX(CASE WHEN tp.hour_of_day BETWEEN 0 AND 4 THEN tp.hour_percentage ELSE 0 END)
        THEN 'Evening (6PM-11PM)'
        ELSE 'Night (12AM-4AM)'
    END AS preferred_time_of_day,
    -- Preferred day of week (weekday vs weekend)
    CASE
        WHEN SUM(CASE WHEN dp.day_of_week IN (1,2,3,4,5) THEN dp.day_plays ELSE 0 END) >
             SUM(CASE WHEN dp.day_of_week IN (0,6) THEN dp.day_plays ELSE 0 END)
        THEN 'Weekday Listener'
        ELSE 'Weekend Listener'
    END AS weekday_weekend_preference,
    -- Listening pattern characterization
    CASE
        WHEN us.avg_session_minutes > 60 AND us.active_days > 20 THEN 'Heavy Listener'
        WHEN us.avg_session_minutes BETWEEN 30 AND 60 AND us.active_days BETWEEN 10 AND 20 THEN 'Regular Listener'
        WHEN us.avg_session_minutes < 30 AND us.active_days < 10 THEN 'Casual Listener'
        ELSE 'Mixed Pattern'
    END AS listener_type
FROM
    user_summary us
LEFT JOIN
    top_genres_by_user tg ON us.user_id = tg.user_id
LEFT JOIN
    time_preferences tp ON us.user_id = tp.user_id
LEFT JOIN
    day_preferences dp ON us.user_id = dp.user_id
GROUP BY
    us.user_id, us.username, us.country, us.subscription_type, 
    us.active_days, us.unique_tracks, us.avg_completion_rate,
    us.avg_session_minutes, us.avg_tracks_per_session, us.total_sessions
ORDER BY
    us.active_days DESC, us.avg_session_minutes DESC; 