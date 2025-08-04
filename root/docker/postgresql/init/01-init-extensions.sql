-- PostgreSQL Extensions and Optimization Setup for TabulaRasa BI Core
-- This script runs during container initialization to set up extensions and optimizations

-- Create extensions for enhanced functionality
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";           -- UUID generation
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";  -- Query statistics
CREATE EXTENSION IF NOT EXISTS "pg_trgm";            -- Trigram matching for text search
CREATE EXTENSION IF NOT EXISTS "btree_gin";          -- GIN indexes for btree types
CREATE EXTENSION IF NOT EXISTS "btree_gist";         -- GIST indexes for btree types

-- Create a dedicated schema for monitoring and utilities
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Create a function to get database size information
CREATE OR REPLACE FUNCTION monitoring.get_database_stats()
RETURNS TABLE (
    database_name TEXT,
    size_bytes BIGINT,
    size_pretty TEXT,
    active_connections INTEGER,
    total_connections INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        datname::TEXT,
        pg_database_size(datname) as size_bytes,
        pg_size_pretty(pg_database_size(datname)) as size_pretty,
        (SELECT count(*) FROM pg_stat_activity WHERE datname = pg_stat_activity.datname AND state = 'active')::INTEGER,
        (SELECT count(*) FROM pg_stat_activity WHERE datname = pg_stat_activity.datname)::INTEGER
    FROM pg_database 
    WHERE datistemplate = false;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get table statistics
CREATE OR REPLACE FUNCTION monitoring.get_table_stats(schema_name TEXT DEFAULT 'public')
RETURNS TABLE (
    table_name TEXT,
    row_count BIGINT,
    size_bytes BIGINT,
    size_pretty TEXT,
    last_vacuum TIMESTAMP WITH TIME ZONE,
    last_analyze TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.tablename::TEXT,
        COALESCE(s.n_tup_ins + s.n_tup_upd - s.n_tup_del, 0) as row_count,
        pg_total_relation_size(quote_ident(schema_name)||'.'||quote_ident(t.tablename)) as size_bytes,
        pg_size_pretty(pg_total_relation_size(quote_ident(schema_name)||'.'||quote_ident(t.tablename))) as size_pretty,
        s.last_vacuum,
        s.last_analyze
    FROM pg_tables t
    LEFT JOIN pg_stat_user_tables s ON t.tablename = s.relname AND t.schemaname = s.schemaname
    WHERE t.schemaname = schema_name
    ORDER BY pg_total_relation_size(quote_ident(schema_name)||'.'||quote_ident(t.tablename)) DESC;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get slow queries
CREATE OR REPLACE FUNCTION monitoring.get_slow_queries(min_duration_ms INTEGER DEFAULT 1000)
RETURNS TABLE (
    query TEXT,
    calls BIGINT,
    total_time_ms DOUBLE PRECISION,
    mean_time_ms DOUBLE PRECISION,
    max_time_ms DOUBLE PRECISION,
    stddev_time_ms DOUBLE PRECISION,
    rows_affected BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        pgs.query,
        pgs.calls,
        pgs.total_exec_time as total_time_ms,
        pgs.mean_exec_time as mean_time_ms,
        pgs.max_exec_time as max_time_ms,
        pgs.stddev_exec_time as stddev_time_ms,
        pgs.rows as rows_affected
    FROM pg_stat_statements pgs
    WHERE pgs.mean_exec_time > min_duration_ms
    ORDER BY pgs.mean_exec_time DESC
    LIMIT 20;
END;
$$ LANGUAGE plpgsql;

-- Create indexes for common query patterns (will be created by Flyway migrations)
-- These are commented out since they should be created by application migrations
-- but are documented here for reference

-- Example indexes for the aggregated_campaign_stats table:
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_acs_campaign_time 
--     ON aggregated_campaign_stats (campaign_id, window_start_time);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_acs_event_type_time 
--     ON aggregated_campaign_stats (event_type, window_start_time);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_acs_updated_at 
--     ON aggregated_campaign_stats (updated_at) WHERE updated_at > NOW() - INTERVAL '1 day';

-- Set default configuration for new connections
ALTER SYSTEM SET log_statement_stats = off;
ALTER SYSTEM SET log_parser_stats = off;
ALTER SYSTEM SET log_planner_stats = off;
ALTER SYSTEM SET log_executor_stats = off;

-- Reload configuration
SELECT pg_reload_conf();

-- Log initialization completion
DO $$
BEGIN
    RAISE NOTICE 'TabulaRasa BI Core database initialization completed successfully';
    RAISE NOTICE 'Extensions created: uuid-ossp, pg_stat_statements, pg_trgm, btree_gin, btree_gist';
    RAISE NOTICE 'Monitoring schema and functions created';
    RAISE NOTICE 'Database ready for application deployment';
END
$$;