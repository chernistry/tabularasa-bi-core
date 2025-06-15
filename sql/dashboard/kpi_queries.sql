-- ============================================================================
-- DASHBOARD KPI QUERIES
-- ============================================================================
-- Файл содержит SQL-запросы, используемые в API дашбордов TabulaRasa.
-- Все запросы написаны для PostgreSQL и работают с таблицей aggregated_campaign_stats.
-- ============================================================================

-- KPI Query: Основные метрики для дашборда Executive Pulse
-- Используется в: /api/kpis
-- Описание: Возвращает агрегированные метрики по всем кампаниям
-- ============================================================================
SELECT  COALESCE(SUM(event_count) FILTER (WHERE event_type = 'impression'), 0) AS impressions,
        COALESCE(SUM(event_count) FILTER (WHERE event_type = 'click'), 0)       AS clicks,
        COALESCE(SUM(event_count) FILTER (WHERE event_type = 'conversion'), 0)  AS conversions,
        COALESCE(SUM(total_bid_amount), 0)                                      AS spend_usd
FROM    aggregated_campaign_stats;

-- ROI Trend Query: Тренд ROI по дням
-- Используется в: /api/roi_trend
-- Описание: Возвращает ежедневный ROI, рассчитанный как отношение
-- стоимости конверсий к стоимости показов
-- ============================================================================
SELECT  window_start_time::date AS day,
        COALESCE(SUM(total_bid_amount) FILTER (WHERE event_type = 'conversion'), 0) /
        NULLIF(COALESCE(SUM(total_bid_amount) FILTER (WHERE event_type = 'impression'), 0), 0) AS roi
FROM    aggregated_campaign_stats
GROUP BY day
ORDER BY day;

-- Freshness Query: Последний timestamp для проверки свежести данных
-- Используется в: /api/pipeline_health
-- Описание: Возвращает самый свежий timestamp из таблицы для расчета
-- времени задержки данных
-- ============================================================================
SELECT MAX(window_start_time) as latest_ts FROM aggregated_campaign_stats;

-- Campaign Performance Query: Метрики по отдельным кампаниям
-- Используется в: /api/campaign_performance (будущий эндпоинт)
-- Описание: Возвращает метрики эффективности по отдельным кампаниям
-- ============================================================================
SELECT  campaign_id,
        SUM(event_count) FILTER (WHERE event_type = 'impression') AS impressions,
        SUM(event_count) FILTER (WHERE event_type = 'click') AS clicks,
        SUM(event_count) FILTER (WHERE event_type = 'conversion') AS conversions,
        SUM(total_bid_amount) AS spend_usd,
        CASE 
            WHEN SUM(event_count) FILTER (WHERE event_type = 'impression') > 0 
            THEN ROUND((SUM(event_count) FILTER (WHERE event_type = 'click')::NUMERIC / 
                 SUM(event_count) FILTER (WHERE event_type = 'impression')) * 100, 2)
            ELSE 0 
        END AS ctr
FROM    aggregated_campaign_stats
GROUP BY campaign_id
ORDER BY spend_usd DESC
LIMIT 10; 