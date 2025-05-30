# Q2 SQL Query Explanation

The provided SQL query identifies the top 3 performing `campaign_id`s for each `country_code` on a specific day ('2024-07-15'), based on the highest Click-Through Rate (CTR). It also includes the total `spend_usd` and total `conversions` for these top campaigns on that day.

The query uses a series of Common Table Expressions (CTEs) to break down the problem:

1.  **`CampaignDailyPerformance` CTE:**
    *   This CTE first filters the `ad_performance_hourly` table for records matching the specified date (`2024-07-15`) by extracting the date part from `hour_timestamp`.
    *   It then aggregates key metrics (`impressions`, `clicks`, `conversions`, `spend_usd`) by `campaign_id` and `country_code` for that entire day. This provides the daily totals needed for CTR calculation and final output.

2.  **`CampaignCTR` CTE:**
    *   This CTE takes the daily aggregated data from `CampaignDailyPerformance`.
    *   It calculates the Click-Through Rate (CTR) using the formula `total_clicks / total_impressions`.
    *   **Handling Division by Zero:** A `CASE` statement is used to prevent division-by-zero errors. If `total_impressions` is 0, CTR is set to 0.0. Otherwise, the CTR is calculated. Multiplying `total_clicks` by `1.0` ensures floating-point division for an accurate CTR value.

3.  **`RankedCampaigns` CTE:**
    *   This CTE is responsible for ranking the campaigns within each country based on their CTR.
    *   It uses the `ROW_NUMBER()` window function, partitioned by `country_code`.
    *   Campaigns are ordered in descending order of `ctr` (`ORDER BY ctr DESC`). This means campaigns with higher CTRs get lower rank numbers (e.g., rank 1 is the best).
    *   A secondary sort key, `total_spend_usd DESC`, is added to the `ORDER BY` clause within the window function. This helps to deterministically rank campaigns that might have the same CTR. The choice of `total_spend_usd` as a tie-breaker is a common business consideration, but could be adapted (e.g., highest conversions, or simply a campaign_id for pure deterministic ordering if no business rule for ties exist).

4.  **Final `SELECT` Statement:**
    *   The final query selects the required columns: `country_code`, `campaign_id`, `rank_in_country`, `ctr`, `total_spend_usd`, and `total_conversions` from the `RankedCampaigns` CTE.
    *   It filters these results to include only campaigns where `rank_in_country` is less than or equal to 3, effectively giving the top 3 campaigns per country.
    *   The results are ordered by `country_code` and then by `rank_in_country` for clear and structured output.

This approach ensures that aggregations are performed correctly at the daily level before CTR calculation and ranking, and that edge cases like zero impressions are handled gracefully. 