-- Main tables in public schema

-- Users table
CREATE TABLE IF NOT EXISTS public.users (
    user_id VARCHAR(255) PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    country VARCHAR(2) NOT NULL,
    joined_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    birth_year INTEGER,
    gender VARCHAR(50),
    subscription_type VARCHAR(50) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_active TIMESTAMP WITH TIME ZONE
);

-- Artists table
CREATE TABLE IF NOT EXISTS public.artists (
    artist_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    popularity INTEGER CHECK (popularity >= 0 AND popularity <= 100),
    genres TEXT[],
    followers INTEGER DEFAULT 0,
    external_url VARCHAR(255)
);

-- Albums table
CREATE TABLE IF NOT EXISTS public.albums (
    album_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    artist_id VARCHAR(255) NOT NULL REFERENCES public.artists(artist_id),
    release_date DATE,
    total_tracks INTEGER CHECK (total_tracks > 0),
    album_type VARCHAR(50) NOT NULL,
    popularity INTEGER CHECK (popularity >= 0 AND popularity <= 100),
    genres TEXT[]
);

-- Tracks table
CREATE TABLE IF NOT EXISTS public.tracks (
    track_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    album_id VARCHAR(255) NOT NULL REFERENCES public.albums(album_id),
    artist_id VARCHAR(255) NOT NULL REFERENCES public.artists(artist_id),
    duration_ms INTEGER NOT NULL CHECK (duration_ms > 0),
    explicit BOOLEAN NOT NULL DEFAULT FALSE,
    track_number INTEGER CHECK (track_number > 0),
    popularity INTEGER CHECK (popularity >= 0 AND popularity <= 100),
    danceability FLOAT CHECK (danceability >= 0 AND danceability <= 1),
    energy FLOAT CHECK (energy >= 0 AND energy <= 1),
    key INTEGER CHECK (key >= -1 AND key <= 11),
    loudness FLOAT,
    mode INTEGER CHECK (mode IN (0, 1)),
    speechiness FLOAT CHECK (speechiness >= 0 AND speechiness <= 1),
    acousticness FLOAT CHECK (acousticness >= 0 AND acousticness <= 1),
    instrumentalness FLOAT CHECK (instrumentalness >= 0 AND instrumentalness <= 1),
    liveness FLOAT CHECK (liveness >= 0 AND liveness <= 1),
    valence FLOAT CHECK (valence >= 0 AND valence <= 1),
    tempo FLOAT CHECK (tempo > 0),
    time_signature INTEGER CHECK (time_signature > 0)
);

-- User Playlists
CREATE TABLE IF NOT EXISTS public.playlists (
    playlist_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL REFERENCES public.users(user_id),
    description TEXT,
    is_public BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_modified TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    follower_count INTEGER NOT NULL DEFAULT 0,
    track_count INTEGER NOT NULL DEFAULT 0
);

-- Playlist Tracks Junction Table
CREATE TABLE IF NOT EXISTS public.playlist_tracks (
    playlist_id VARCHAR(255) NOT NULL REFERENCES public.playlists(playlist_id),
    track_id VARCHAR(255) NOT NULL REFERENCES public.tracks(track_id),
    added_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    added_by VARCHAR(255) NOT NULL REFERENCES public.users(user_id),
    position INTEGER NOT NULL,
    PRIMARY KEY (playlist_id, track_id)
);

-- Raw streaming history
CREATE TABLE IF NOT EXISTS public.streaming_history (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL REFERENCES public.users(user_id),
    track_id VARCHAR(255) NOT NULL REFERENCES public.tracks(track_id),
    played_at TIMESTAMP WITH TIME ZONE NOT NULL,
    ms_played INTEGER NOT NULL CHECK (ms_played >= 0),
    session_id VARCHAR(255),
    device VARCHAR(100),
    reason_start VARCHAR(100),
    reason_end VARCHAR(100),
    shuffle BOOLEAN,
    skipped BOOLEAN,
    offline BOOLEAN,
    incognito_mode BOOLEAN
);

-- Tables in the processed schema

-- Processed streaming history
CREATE TABLE IF NOT EXISTS processed.streaming_history (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    track_id VARCHAR(255) NOT NULL,
    artist_id VARCHAR(255) NOT NULL,
    album_id VARCHAR(255) NOT NULL,
    played_at TIMESTAMP WITH TIME ZONE NOT NULL,
    ms_played INTEGER NOT NULL CHECK (ms_played >= 0),
    completion_rate FLOAT CHECK (completion_rate >= 0 AND completion_rate <= 1),
    day_of_week INTEGER CHECK (day_of_week >= 0 AND day_of_week <= 6),
    hour_of_day INTEGER CHECK (hour_of_day >= 0 AND hour_of_day <= 23),
    skipped BOOLEAN,
    session_id VARCHAR(255),
    country VARCHAR(2),
    subscription_type VARCHAR(50),
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Tables in the analytics schema

-- User listening stats
CREATE TABLE IF NOT EXISTS analytics.user_listening_stats (
    user_id VARCHAR(255) PRIMARY KEY,
    unique_tracks_played INTEGER NOT NULL DEFAULT 0,
    total_minutes_played FLOAT NOT NULL DEFAULT 0,
    total_plays INTEGER NOT NULL DEFAULT 0,
    favorite_artist_id VARCHAR(255),
    favorite_genre VARCHAR(100),
    active_days_last_month INTEGER NOT NULL DEFAULT 0,
    avg_daily_minutes FLOAT NOT NULL DEFAULT 0,
    listening_streak INTEGER NOT NULL DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Track popularity stats
CREATE TABLE IF NOT EXISTS analytics.track_popularity (
    track_id VARCHAR(255) PRIMARY KEY,
    total_plays INTEGER NOT NULL DEFAULT 0,
    unique_listeners INTEGER NOT NULL DEFAULT 0,
    skip_rate FLOAT NOT NULL DEFAULT 0,
    avg_completion_rate FLOAT NOT NULL DEFAULT 0,
    trending_score FLOAT NOT NULL DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Artist popularity stats
CREATE TABLE IF NOT EXISTS analytics.artist_popularity (
    artist_id VARCHAR(255) PRIMARY KEY,
    total_plays INTEGER NOT NULL DEFAULT 0,
    unique_listeners INTEGER NOT NULL DEFAULT 0,
    most_popular_track VARCHAR(255),
    trending_score FLOAT NOT NULL DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Hourly listening trends
CREATE TABLE IF NOT EXISTS analytics.hourly_listening_trends (
    hour_of_day INTEGER PRIMARY KEY CHECK (hour_of_day >= 0 AND hour_of_day <= 23),
    total_plays INTEGER NOT NULL DEFAULT 0,
    unique_listeners INTEGER NOT NULL DEFAULT 0,
    most_played_genre VARCHAR(100),
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- Tables in the monitoring schema

-- Pipeline executions log
CREATE TABLE IF NOT EXISTS monitoring.pipeline_executions (
    execution_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    duration_seconds INTEGER,
    records_processed INTEGER,
    error_message TEXT,
    parameters JSONB
);

-- Create indexes for performance

-- Streaming history indexes
CREATE INDEX IF NOT EXISTS idx_streaming_history_user_id ON public.streaming_history(user_id);
CREATE INDEX IF NOT EXISTS idx_streaming_history_track_id ON public.streaming_history(track_id);
CREATE INDEX IF NOT EXISTS idx_streaming_history_played_at ON public.streaming_history(played_at);
CREATE INDEX IF NOT EXISTS idx_streaming_history_session_id ON public.streaming_history(session_id);

-- Processed streaming history indexes
CREATE INDEX IF NOT EXISTS idx_processed_streaming_user_id ON processed.streaming_history(user_id);
CREATE INDEX IF NOT EXISTS idx_processed_streaming_track_id ON processed.streaming_history(track_id);
CREATE INDEX IF NOT EXISTS idx_processed_streaming_artist_id ON processed.streaming_history(artist_id);
CREATE INDEX IF NOT EXISTS idx_processed_streaming_played_at ON processed.streaming_history(played_at);
CREATE INDEX IF NOT EXISTS idx_processed_streaming_day_week ON processed.streaming_history(day_of_week);
CREATE INDEX IF NOT EXISTS idx_processed_streaming_hour_day ON processed.streaming_history(hour_of_day); 