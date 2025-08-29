-- Extract user data from raw database
-- This query extracts user information for the ETL pipeline
-- The date parameters will be interpolated by Airflow

SELECT 
    u.user_id,
    u.username,
    u.email,
    u.country,
    u.birthdate,
    u.gender,
    u.subscription_type,
    u.registration_date,
    u.active,
    u.last_login
FROM 
    spotify_raw.users u
WHERE 
    u.active = TRUE; 