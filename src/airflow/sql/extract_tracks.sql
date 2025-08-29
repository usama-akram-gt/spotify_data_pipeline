-- Extract track data from raw database
-- This query extracts track information for the ETL pipeline
-- The date parameters will be interpolated by Airflow

SELECT 
    t.track_id,
    t.name AS track_name,
    t.artist_id,
    a.name AS artist_name,
    t.album_id,
    al.name AS album_name,
    t.duration_ms,
    t.explicit,
    t.track_number,
    t.disc_number,
    t.popularity,
    t.release_date,
    a.genres,
    a.popularity AS artist_popularity,
    al.total_tracks,
    al.album_type
FROM 
    spotify_raw.tracks t
JOIN 
    spotify_raw.artists a ON t.artist_id = a.artist_id
JOIN 
    spotify_raw.albums al ON t.album_id = al.album_id; 