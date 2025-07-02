-- Q2: Advanced SQL for Ad Performance Analysis
-- Identifies the top 3 performing campaign_ids for each country_code on '2024-07-15'
-- based on Click-Through Rate (CTR = clicks / impressions).
-- Includes total_spend_usd and total_conversions for these top campaigns.

WITH CampaignDailyPerformance AS (
    -- Aggregate performance metrics per campaign, country, and day
    SELECT
        campaign_id,
        country_code,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(conversions) AS total_conversions,
        SUM(spend_usd) AS total_spend_usd
    FROM
        ad_performance_hourly
    WHERE
        DATE(hour_timestamp) = '2024-07-15' -- Filter for the specific day
    GROUP BY
        campaign_id,
        country_code
),
CampaignCTR AS (
    -- Calculate CTR for each campaign, handling division by zero
    SELECT
        campaign_id,
        country_code,
        total_impressions,
        total_clicks,
        total_conversions,
        total_spend_usd,
        CASE
            WHEN total_impressions = 0 THEN 0.0
            ELSE (total_clicks * 1.0 / total_impressions) -- Multiply by 1.0 for decimal division
        END AS ctr
    FROM
        CampaignDailyPerformance
),
RankedCampaigns AS (
    -- Rank campaigns within each country by CTR
    SELECT
        campaign_id,
        country_code,
        ctr,
        total_spend_usd,
        total_conversions,
        ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY ctr DESC, total_spend_usd DESC) AS rank_in_country
        -- Secondary sort by total_spend_usd in case of CTR ties, can be adjusted based on business rule
    FROM
        CampaignCTR
)
-- Final selection of top 3 campaigns per country
SELECT
    country_code,
    campaign_id,
    rank_in_country,
    ctr,
    total_spend_usd,
    total_conversions
FROM
    RankedCampaigns
WHERE
    rank_in_country <= 3
ORDER BY
    country_code ASC,
    rank_in_country ASC; 