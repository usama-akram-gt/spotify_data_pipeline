-- Create necessary schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS processed;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Create users with appropriate permissions
CREATE USER pipeline_user WITH PASSWORD 'pipeline_password';
CREATE USER analytics_user WITH PASSWORD 'analytics_password';
CREATE USER monitoring_user WITH PASSWORD 'monitoring_password';

-- Grant privileges
GRANT USAGE ON SCHEMA public TO pipeline_user, analytics_user, monitoring_user;
GRANT USAGE ON SCHEMA staging TO pipeline_user, monitoring_user;
GRANT USAGE ON SCHEMA processed TO pipeline_user, analytics_user, monitoring_user;
GRANT USAGE ON SCHEMA analytics TO analytics_user, monitoring_user;
GRANT USAGE ON SCHEMA monitoring TO monitoring_user;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO pipeline_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA processed TO pipeline_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_user;
GRANT SELECT ON ALL TABLES IN SCHEMA processed TO analytics_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO analytics_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA monitoring TO monitoring_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public, staging, processed, analytics TO monitoring_user;

-- Set default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA processed GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA processed GRANT SELECT ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO analytics_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA monitoring GRANT ALL ON TABLES TO monitoring_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public, staging, processed, analytics GRANT SELECT ON TABLES TO monitoring_user;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Set search path for convenience
SET search_path TO public, staging, processed, analytics, monitoring; 