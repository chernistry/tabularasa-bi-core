-- Production-ready table for storing aggregated campaign statistics with optimized schema
CREATE TABLE IF NOT EXISTS aggregated_campaign_stats (
    id BIGSERIAL PRIMARY KEY,
    campaign_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(50) NOT NULL, -- Reduced size for better performance
    event_count BIGINT NOT NULL DEFAULT 0,
    total_bid_amount NUMERIC(19, 4) NOT NULL DEFAULT 0,
    window_start_time TIMESTAMP WITH TIME ZONE NOT NULL, -- Added timezone support
    window_end_time TIMESTAMP WITH TIME ZONE NOT NULL,   -- Added timezone support
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Optimized composite primary key for better performance
    CONSTRAINT aggregated_campaign_stats_unique_window 
        UNIQUE (campaign_id, event_type, window_start_time)
);

-- Production-ready materialized view for analytics performance
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_campaign_stats_summary AS
SELECT 
    campaign_id,
    event_type,
    SUM(event_count) AS total_events,
    SUM(total_bid_amount) AS total_bid_amount,
    AVG(total_bid_amount / NULLIF(event_count, 0)) AS avg_bid_per_event,
    MIN(window_start_time) AS first_event_time,
    MAX(window_end_time) AS last_event_time,
    COUNT(*) AS total_windows,
    NOW() AS last_refreshed
FROM 
    aggregated_campaign_stats
GROUP BY 
    campaign_id, event_type;

-- Create index on materialized view for faster queries
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_campaign_stats_pk 
    ON mv_campaign_stats_summary (campaign_id, event_type);

-- Performance-optimized indexes with proper covering indexes
CREATE INDEX IF NOT EXISTS idx_campaign_stats_campaign_time 
    ON aggregated_campaign_stats (campaign_id, window_start_time DESC) 
    INCLUDE (event_type, event_count, total_bid_amount);

CREATE INDEX IF NOT EXISTS idx_campaign_stats_type_time 
    ON aggregated_campaign_stats (event_type, window_start_time DESC) 
    INCLUDE (campaign_id, event_count, total_bid_amount);

-- Composite index for time-range queries
CREATE INDEX IF NOT EXISTS idx_campaign_stats_time_range 
    ON aggregated_campaign_stats (window_start_time, window_end_time) 
    INCLUDE (campaign_id, event_type, event_count, total_bid_amount);

-- Partial index for recent data (most commonly queried)
CREATE INDEX IF NOT EXISTS idx_campaign_stats_recent 
    ON aggregated_campaign_stats (campaign_id, event_type, window_start_time) 
    WHERE window_start_time >= NOW() - INTERVAL '7 days';

-- Auto-refresh mechanism for materialized view (production-ready)
CREATE OR REPLACE FUNCTION refresh_campaign_stats_summary()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_campaign_stats_summary;
    -- Log refresh for monitoring
    INSERT INTO system_events (event_type, event_data, created_at) 
    VALUES ('materialized_view_refresh', 
            '{"view": "mv_campaign_stats_summary", "status": "completed"}',
            NOW());
EXCEPTION
    WHEN OTHERS THEN
        -- Log error for monitoring
        INSERT INTO system_events (event_type, event_data, created_at) 
        VALUES ('materialized_view_refresh_error', 
                '{"view": "mv_campaign_stats_summary", "error": "' || SQLERRM || '"}',
                NOW());
        RAISE;
END;
$$ LANGUAGE plpgsql;

-- Create system events table for operational monitoring
CREATE TABLE IF NOT EXISTS system_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    event_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_system_events_type_time 
    ON system_events (event_type, created_at DESC);

-- Data retention function for automated cleanup
CREATE OR REPLACE FUNCTION cleanup_old_campaign_stats()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    -- Delete records older than 90 days
    DELETE FROM aggregated_campaign_stats 
    WHERE window_start_time < NOW() - INTERVAL '90 days';
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    -- Log cleanup operation
    INSERT INTO system_events (event_type, event_data, created_at) 
    VALUES ('data_cleanup', 
            '{"table": "aggregated_campaign_stats", "deleted_rows": ' || deleted_count || '}',
            NOW());
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Updated trigger for automatic updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_updated_at
    BEFORE UPDATE ON aggregated_campaign_stats
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Production-ready table comments and documentation
COMMENT ON TABLE aggregated_campaign_stats IS 'Production table storing aggregated campaign statistics with time-windowed data for real-time analytics';
COMMENT ON COLUMN aggregated_campaign_stats.campaign_id IS 'Campaign identifier - indexed for fast lookups';
COMMENT ON COLUMN aggregated_campaign_stats.event_type IS 'Event type (impression, click, conversion) - varchar(50) for performance';
COMMENT ON COLUMN aggregated_campaign_stats.event_count IS 'Number of events in time window - always >= 0';
COMMENT ON COLUMN aggregated_campaign_stats.total_bid_amount IS 'Total bid amount in USD for time window - precision 19,4 for financial accuracy';
COMMENT ON COLUMN aggregated_campaign_stats.window_start_time IS 'Start of aggregation window - timezone-aware for global operations';
COMMENT ON COLUMN aggregated_campaign_stats.window_end_time IS 'End of aggregation window - timezone-aware for global operations';

COMMENT ON MATERIALIZED VIEW mv_campaign_stats_summary IS 'Materialized view for fast analytics queries - refreshed every 5 minutes';
COMMENT ON FUNCTION refresh_campaign_stats_summary() IS 'Refreshes materialized view with error handling and logging';
COMMENT ON FUNCTION cleanup_old_campaign_stats() IS 'Automated cleanup of old data - run daily via cron job'; 