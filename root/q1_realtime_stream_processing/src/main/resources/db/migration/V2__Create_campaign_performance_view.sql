-- V2__Create_campaign_performance_view.sql
-- Create materialized view for campaign performance analytics

CREATE OR REPLACE VIEW v_aggregated_campaign_stats AS
SELECT 
    id,
    campaign_id,
    event_type,
    window_start_time,
    window_end_time,
    event_count,
    total_bid_amount,
    total_sales_amount_euro,
    spend_usd,
    device_type,
    product_category_1,
    product_brand,
    product_age_group,
    updated_at,
    -- Calculated fields for analytics
    CASE 
        WHEN event_count > 0 THEN total_bid_amount / event_count
        ELSE 0
    END AS avg_bid_amount,
    CASE 
        WHEN spend_usd > 0 THEN total_sales_amount_euro / spend_usd
        ELSE 0
    END AS roi
FROM aggregated_campaign_stats;

-- Grant appropriate permissions
GRANT SELECT ON v_aggregated_campaign_stats TO PUBLIC;

-- Create additional analytics table for pipeline health metrics
CREATE TABLE IF NOT EXISTS bi_pipeline_metrics (
    id BIGSERIAL PRIMARY KEY,
    process_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    pipeline_name VARCHAR(255) NOT NULL,
    job_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('success', 'failed', 'running', 'pending')),
    processing_time_sec DECIMAL(10, 3),
    records_processed BIGINT DEFAULT 0,
    errors_count BIGINT DEFAULT 0,
    data_freshness_minutes INTEGER,
    quality_score DECIMAL(5, 2) CHECK (quality_score >= 0 AND quality_score <= 100),
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure uniqueness for job tracking
    CONSTRAINT bi_pipeline_metrics_unique UNIQUE (pipeline_name, job_id)
);

-- Create indexes for pipeline metrics
CREATE INDEX idx_bi_pipeline_metrics_process_time 
    ON bi_pipeline_metrics (process_time DESC);

CREATE INDEX idx_bi_pipeline_metrics_pipeline_name 
    ON bi_pipeline_metrics (pipeline_name);

CREATE INDEX idx_bi_pipeline_metrics_status 
    ON bi_pipeline_metrics (status);

CREATE INDEX idx_bi_pipeline_metrics_created_at 
    ON bi_pipeline_metrics (created_at DESC);

-- Add comments for documentation
COMMENT ON TABLE bi_pipeline_metrics IS 'Tracks data pipeline execution metrics and health';
COMMENT ON COLUMN bi_pipeline_metrics.pipeline_name IS 'Name of the data pipeline';
COMMENT ON COLUMN bi_pipeline_metrics.job_id IS 'Unique identifier for the pipeline job';
COMMENT ON COLUMN bi_pipeline_metrics.status IS 'Current status of the pipeline job';
COMMENT ON COLUMN bi_pipeline_metrics.processing_time_sec IS 'Time taken to process the job in seconds';
COMMENT ON COLUMN bi_pipeline_metrics.data_freshness_minutes IS 'Age of the data being processed in minutes';
COMMENT ON COLUMN bi_pipeline_metrics.quality_score IS 'Data quality score from 0-100';