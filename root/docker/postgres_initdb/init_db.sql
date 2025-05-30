CREATE TABLE IF NOT EXISTS ad_campaign_summary (
    campaign_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_count BIGINT DEFAULT 0,
    total_bid_amount DOUBLE PRECISION DEFAULT 0.0,
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (campaign_id, event_type)
);

-- Optional: Add an index for querying by last_updated if needed for dashboards/reports
-- CREATE INDEX IF NOT EXISTS idx_ad_campaign_summary_last_updated ON ad_campaign_summary(last_updated DESC);

-- Grant permissions if a specific user is used by the Spark application (other than the default postgres user)
-- Example: GRANT ALL ON ad_campaign_summary TO my_spark_user; 