-- ==== AGGREGATED CAMPAIGN STATS TABLE DEFINITION ====
CREATE TABLE IF NOT EXISTS aggregated_campaign_stats (
    campaign_id VARCHAR NOT NULL,
    event_type VARCHAR NOT NULL,
    window_start_time TIMESTAMP NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    total_bid_amount DECIMAL(18, 4) NOT NULL DEFAULT 0.0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (campaign_id, event_type, window_start_time)
);

-- ==== INDEXES FOR QUERY PERFORMANCE ====
CREATE INDEX IF NOT EXISTS idx_agg_stats_campaign_id ON aggregated_campaign_stats (campaign_id);
CREATE INDEX IF NOT EXISTS idx_agg_stats_event_type ON aggregated_campaign_stats (event_type);
CREATE INDEX IF NOT EXISTS idx_agg_stats_window_start_time ON aggregated_campaign_stats (window_start_time); 