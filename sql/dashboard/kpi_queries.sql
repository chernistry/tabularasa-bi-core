-- ============================================================================
-- DASHBOARD KPI QUERIES (ENHANCED FOR ADVANCED BI)
-- ============================================================================
-- This file contains SQL queries for the TabulaRasa dashboards API.
-- Queries are written for PostgreSQL and are designed to work with the
-- enriched `aggregated_campaign_stats` table, which includes Criteo data.
-- ============================================================================

-- NAME: KPI Query
-- Provides high-level, platform-wide KPIs.
SELECT  COALESCE(SUM(event_count) FILTER (WHERE event_type = 'impression'), 0) AS impressions,
        COALESCE(SUM(event_count) FILTER (WHERE event_type = 'click'), 0)       AS clicks,
        COALESCE(SUM(event_count) FILTER (WHERE event_type = 'conversion'), 0)  AS conversions,
        COALESCE(SUM(total_bid_amount), 0)                                      AS spend_usd
FROM    aggregated_campaign_stats;

-- NAME: ROI Trend Query
-- Calculates daily Return on Investment (ROI) trend.
-- ROI = Revenue / Spend where Revenue = conversions * 50 EUR (fallback)
SELECT
    DATE_TRUNC('day', window_start_time)::date AS day,
    CASE
        WHEN SUM(total_bid_amount) > 0
        THEN (SUM(event_count) FILTER (WHERE event_type = 'conversion')) * 50.0 / SUM(total_bid_amount)
        ELSE 0
    END AS roi
FROM
    aggregated_campaign_stats
GROUP BY
    day
ORDER BY
    day;

-- NAME: Freshness Query
-- Gets the timestamp of the most recent data point.
SELECT MAX(window_start_time) as latest_ts 
FROM aggregated_campaign_stats;

-- NAME: Campaign Performance Query
-- Aggregates performance metrics for each campaign. Used for bubble charts and matrices.
SELECT
    campaign_id,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'impression'), 0) AS impressions,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'click'), 0) AS clicks,
    COALESCE(SUM(event_count) FILTER (WHERE event_type = 'conversion'), 0) AS conversions,
    COALESCE(SUM(total_bid_amount), 0) AS spend_usd,
    -- Calculate CTR safely
    CASE
        WHEN SUM(event_count) FILTER (WHERE event_type = 'impression') > 0
        THEN ROUND((SUM(event_count) FILTER (WHERE event_type = 'click')::NUMERIC /
                     SUM(event_count) FILTER (WHERE event_type = 'impression')) * 100, 2)
        ELSE 0
    END AS ctr
FROM
    aggregated_campaign_stats
GROUP BY
    campaign_id;

-- NAME: Performance by Device Type
-- Aggregates sales and conversion rate by device type.
SELECT
    device_type,
    COALESCE(SUM(sales_amount_euro), 0) AS total_revenue,
    CASE
        WHEN SUM(event_count) FILTER (WHERE event_type = 'click') > 0
        THEN (SUM(event_count) FILTER (WHERE event_type = 'conversion')::NUMERIC /
              SUM(event_count) FILTER (WHERE event_type = 'click'))
        ELSE 0
    END as conversion_rate
FROM
    aggregated_campaign_stats
WHERE
    device_type IS NOT NULL
GROUP BY
    device_type
ORDER BY
    total_revenue DESC;

-- NAME: Revenue by Product Category
-- Aggregates sales revenue by the top-level product category.
SELECT
    product_category_1,
    COALESCE(SUM(sales_amount_euro), 0) AS total_revenue
FROM
    aggregated_campaign_stats
WHERE
    product_category_1 IS NOT NULL
GROUP BY
    product_category_1
ORDER BY
    total_revenue DESC; 