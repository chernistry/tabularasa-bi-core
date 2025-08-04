-- V1__Create_aggregated_campaign_stats_table.sql
-- Initial schema creation for aggregated campaign statistics

CREATE TABLE IF NOT EXISTS aggregated_campaign_stats (
    id BIGSERIAL PRIMARY KEY,
    campaign_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    window_start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    window_end_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    total_bid_amount DECIMAL(18, 6) NOT NULL DEFAULT 0.00,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Additional fields for comprehensive analytics
    total_sales_amount_euro DECIMAL(18, 6) DEFAULT 0.00,
    spend_usd DECIMAL(18, 6) DEFAULT 0.00,
    device_type VARCHAR(50),
    product_category_1 INTEGER,
    product_brand VARCHAR(100),
    product_age_group VARCHAR(50),
    
    -- Ensure data integrity with unique constraint
    CONSTRAINT aggregated_campaign_stats_unique 
        UNIQUE (campaign_id, event_type, window_start_time)
);

-- Create indexes for optimal query performance
CREATE INDEX idx_aggregated_campaign_stats_campaign_id 
    ON aggregated_campaign_stats (campaign_id);

CREATE INDEX idx_aggregated_campaign_stats_event_type 
    ON aggregated_campaign_stats (event_type);

CREATE INDEX idx_aggregated_campaign_stats_window 
    ON aggregated_campaign_stats (window_start_time, window_end_time);

CREATE INDEX idx_aggregated_campaign_stats_updated_at 
    ON aggregated_campaign_stats (updated_at);

-- Composite indexes for common query patterns
CREATE INDEX idx_aggregated_campaign_stats_campaign_event 
    ON aggregated_campaign_stats (campaign_id, event_type);

CREATE INDEX idx_aggregated_campaign_stats_window_campaign 
    ON aggregated_campaign_stats (window_start_time, campaign_id);

-- Add table comment for documentation
COMMENT ON TABLE aggregated_campaign_stats IS 'Stores aggregated campaign performance metrics in time windows';
COMMENT ON COLUMN aggregated_campaign_stats.campaign_id IS 'Unique identifier for the advertising campaign';
COMMENT ON COLUMN aggregated_campaign_stats.event_type IS 'Type of event: impression, click, or conversion';
COMMENT ON COLUMN aggregated_campaign_stats.window_start_time IS 'Start time of the aggregation window';
COMMENT ON COLUMN aggregated_campaign_stats.window_end_time IS 'End time of the aggregation window';
COMMENT ON COLUMN aggregated_campaign_stats.event_count IS 'Number of events in this window';
COMMENT ON COLUMN aggregated_campaign_stats.total_bid_amount IS 'Sum of bid amounts in this window';
COMMENT ON COLUMN aggregated_campaign_stats.updated_at IS 'Last update timestamp for this record';