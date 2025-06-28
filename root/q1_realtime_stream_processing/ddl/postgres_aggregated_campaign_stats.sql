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

-- ==== VIEW FOR DASHBOARD COMPATIBILITY ====
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