-- ============================================================================
-- DASHBOARD KPI QUERIES (ENHANCED FOR ADVANCED BI)
-- ============================================================================
-- This file contains SQL queries for the TabulaRasa dashboards API.
-- Queries are written for PostgreSQL and are designed to work with the
-- enriched `aggregated_campaign_stats` table, which includes Criteo data.
-- ============================================================================

-- NAME: KPI Query
-- Provides high-level, platform-wide KPIs.
SELECT  COALESCE(SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END), 0) AS impressions,
        COALESCE(SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END), 0) AS clicks,
        COALESCE(SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END), 0) AS conversions,
        COALESCE(SUM(total_spend_usd), 0) AS total_spend_usd,
        COALESCE(SUM(total_sales_amount_euro), 0) AS total_revenue_euro,
        CASE
            WHEN COALESCE(SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END), 0) > 0
            THEN COALESCE(SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END), 0)::NUMERIC / COALESCE(SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END), 0)
            ELSE 0
        END AS ctr,
        CASE
            WHEN COALESCE(SUM(total_spend_usd), 0) > 0
            THEN COALESCE(SUM(total_sales_amount_euro), 0) / COALESCE(SUM(total_spend_usd), 0)
            ELSE 0
        END AS roas
FROM    aggregated_campaign_stats;

-- NAME: ROI Trend Query
-- Calculates daily Return on Investment (ROI) trend.
-- ROI = Revenue / Spend
SELECT
    DATE_TRUNC('day', window_start_time)::date AS day,
    CASE
        WHEN SUM(total_spend_usd) > 0
        THEN SUM(total_sales_amount_euro) / SUM(total_spend_usd)
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

-- NAME: Performance by Product Brand
-- Aggregates sales revenue by product brand.
SELECT
    product_brand,
    COALESCE(SUM(sales_amount_euro), 0) AS total_revenue
FROM
    aggregated_campaign_stats
WHERE
    product_brand IS NOT NULL
GROUP BY
    product_brand
ORDER BY
    total_revenue DESC
LIMIT 15;

-- NAME: Performance by Product Age Group
-- Aggregates sales revenue by product age group.
SELECT
    product_age_group,
    COALESCE(SUM(sales_amount_euro), 0) AS total_revenue
FROM
    aggregated_campaign_stats
WHERE
    product_age_group IS NOT NULL
GROUP BY
    product_age_group
ORDER BY
    total_revenue DESC; 