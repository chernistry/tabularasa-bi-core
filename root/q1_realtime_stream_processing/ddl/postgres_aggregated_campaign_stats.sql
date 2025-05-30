-- Create the aggregated campaign stats table if it does not exist
CREATE TABLE IF NOT EXISTS aggregated_campaign_stats (
    campaign_id BIGINT NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    impressions BIGINT NOT NULL DEFAULT 0,
    clicks BIGINT NOT NULL DEFAULT 0,
    conversions BIGINT NOT NULL DEFAULT 0,
    total_bid_amount_usd DECIMAL(12, 4) NOT NULL DEFAULT 0.0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (campaign_id, window_start, window_end)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_aggregated_campaign_stats_campaign_id ON aggregated_campaign_stats (campaign_id);
CREATE INDEX IF NOT EXISTS idx_aggregated_campaign_stats_window_start ON aggregated_campaign_stats (window_start);
CREATE INDEX IF NOT EXISTS idx_aggregated_campaign_stats_window_end ON aggregated_campaign_stats (window_end); 