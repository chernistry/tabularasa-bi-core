-- Таблица для хранения агрегированных статистик по рекламным кампаниям
CREATE TABLE IF NOT EXISTS aggregated_campaign_stats (
    id BIGSERIAL PRIMARY KEY,
    campaign_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    event_count BIGINT NOT NULL,
    total_bid_amount NUMERIC(19, 4) DEFAULT 0,
    window_start_time TIMESTAMP NOT NULL,
    window_end_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT aggregated_campaign_stats_unique_window 
        UNIQUE (campaign_id, event_type, window_start_time)
);

-- Создаем представление для удобного анализа данных
CREATE OR REPLACE VIEW v_aggregated_campaign_stats AS
SELECT 
    campaign_id,
    event_type,
    SUM(event_count) AS total_events,
    SUM(total_bid_amount) AS total_bid_amount,
    MIN(window_start_time) AS min_time,
    MAX(window_end_time) AS max_time
FROM 
    aggregated_campaign_stats
GROUP BY 
    campaign_id, event_type;

-- Добавляем индексы для улучшения производительности запросов
CREATE INDEX IF NOT EXISTS idx_campaign_id ON aggregated_campaign_stats (campaign_id);
CREATE INDEX IF NOT EXISTS idx_event_type ON aggregated_campaign_stats (event_type);
CREATE INDEX IF NOT EXISTS idx_window_time ON aggregated_campaign_stats (window_start_time, window_end_time);

-- Комментарии к таблице и колонкам для документации
COMMENT ON TABLE aggregated_campaign_stats IS 'Хранит агрегированную статистику по рекламным кампаниям с разбивкой по типам событий и временным окнам';
COMMENT ON COLUMN aggregated_campaign_stats.campaign_id IS 'Идентификатор рекламной кампании';
COMMENT ON COLUMN aggregated_campaign_stats.event_type IS 'Тип события (impression, click, etc)';
COMMENT ON COLUMN aggregated_campaign_stats.event_count IS 'Количество событий данного типа в указанном временном окне';
COMMENT ON COLUMN aggregated_campaign_stats.total_bid_amount IS 'Суммарная ставка в USD за события данного типа в указанном временном окне';
COMMENT ON COLUMN aggregated_campaign_stats.window_start_time IS 'Начало временного окна агрегации';
COMMENT ON COLUMN aggregated_campaign_stats.window_end_time IS 'Конец временного окна агрегации'; 