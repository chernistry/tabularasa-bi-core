-- ==== AGGREGATED CAMPAIGN STATS TABLE DEFINITION ====
CREATE TABLE aggregated_campaign_stats (
    id SERIAL PRIMARY KEY,
    campaign_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    window_start_time TIMESTAMP NOT NULL,
    window_end_time TIMESTAMP NOT NULL,
    event_count BIGINT NOT NULL,
    total_bid_amount DECIMAL(18, 6) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT aggregated_campaign_stats_unique UNIQUE (campaign_id, event_type, window_start_time)
);

-- ==== INDEXES FOR QUERY PERFORMANCE ====
CREATE INDEX idx_aggregated_campaign_stats_campaign_id ON aggregated_campaign_stats (campaign_id);
CREATE INDEX idx_aggregated_campaign_stats_event_type ON aggregated_campaign_stats (event_type);
CREATE INDEX idx_aggregated_campaign_stats_window ON aggregated_campaign_stats (window_start_time, window_end_time);

-- ==== VIEW FOR DASHBOARD COMPATIBILITY ====
DROP VIEW IF EXISTS v_aggregated_campaign_stats;

CREATE OR REPLACE VIEW v_aggregated_campaign_stats AS
SELECT
  campaign_id,
  event_type,
  window_start_time,
  'Desktop' as device_type,
  'US' as country_code,
  'Brand' as product_brand,
  '25-34' as product_age_group,
  1 as product_category_1,
  NULL as product_category_2,
  NULL as product_category_3,
  NULL as product_category_4,
  NULL as product_category_5,
  NULL as product_category_6,
  NULL as product_category_7,
  event_count,
  total_bid_amount as spend_usd,
  total_bid_amount * 0.85 as total_sales_amount_euro,
  CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END as total_sales_count,
  updated_at
FROM aggregated_campaign_stats;

-- Grant permissions if needed
-- GRANT ALL PRIVILEGES ON TABLE aggregated_campaign_stats TO tabulauser; 