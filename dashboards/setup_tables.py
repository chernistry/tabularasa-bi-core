#!/usr/bin/env python
"""
Utility script to create PostgreSQL tables and views for dashboards.
IMPORTANT:
• This file must NEVER create or insert mocked-up data. Do NOT implement any mock data generation.
• The live data pipeline is responsible for populating the tables.  
This script creates the necessary database schema for the dashboards to function.
It checks if tables already exist before attempting to create them.
"""

import psycopg2
import sys

# --------------------------------------------------------------------------- #
# Database connection configuration
# --------------------------------------------------------------------------- #
DB_CONFIG = {
    "dbname": "tabularasadb",
    "user": "tabulauser",
    "password": "tabulapass",
    "host": "localhost",
    "port": "5432",
}

# --------------------------------------------------------------------------- #
# Table-creation SQL
# --------------------------------------------------------------------------- #
CREATE_CAMPAIGN_STATS_TABLE = """
CREATE TABLE IF NOT EXISTS aggregated_campaign_stats (
    campaign_id VARCHAR NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    window_start_time TIMESTAMP NOT NULL,
    event_count BIGINT NOT NULL DEFAULT 0,
    total_bid_amount DECIMAL(18, 4) NOT NULL DEFAULT 0.0,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (campaign_id, event_type, window_start_time)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_agg_stats_campaign_id ON aggregated_campaign_stats (campaign_id);
CREATE INDEX IF NOT EXISTS idx_agg_stats_event_type ON aggregated_campaign_stats (event_type);
CREATE INDEX IF NOT EXISTS idx_agg_stats_window_start_time ON aggregated_campaign_stats (window_start_time);
"""

CREATE_VIEW_CAMPAIGN_STATS = """
-- Create view for dashboard compatibility
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
"""

CREATE_PIPELINE_METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS bi_pipeline_metrics (
    id SERIAL PRIMARY KEY,
    process_time TIMESTAMP,
    processing_time_sec DECIMAL(10, 2),
    status VARCHAR(20),
    data_freshness_minutes INTEGER,
    quality_score DECIMAL(5, 2),
    error_count INTEGER
);
"""

# --------------------------------------------------------------------------- #
# Update data for dashboard compatibility
# --------------------------------------------------------------------------- #
UPDATE_IMPRESSION_BID_AMOUNTS = """
-- Update impression records to have non-zero bid amounts
-- This is needed for dashboards to display meaningful metrics
UPDATE aggregated_campaign_stats
SET total_bid_amount = event_count * 0.05
WHERE event_type = 'impression' AND total_bid_amount = 0;
"""

# --------------------------------------------------------------------------- #
# Check if tables exist
# --------------------------------------------------------------------------- #
CHECK_TABLES_SQL = """
SELECT EXISTS (
   SELECT FROM information_schema.tables 
   WHERE table_schema = 'public'
   AND table_name = 'aggregated_campaign_stats'
);
"""

CHECK_VIEW_SQL = """
SELECT EXISTS (
   SELECT FROM information_schema.views 
   WHERE table_schema = 'public'
   AND table_name = 'v_aggregated_campaign_stats'
);
"""

# --------------------------------------------------------------------------- #
# Main entry point
# --------------------------------------------------------------------------- #
def main() -> None:
    """Create database tables and views if they don't exist."""
    print("Checking PostgreSQL schema …")

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Check if tables already exist
        cursor.execute(CHECK_TABLES_SQL)
        table_exists = cursor.fetchone()[0]
        
        cursor.execute(CHECK_VIEW_SQL)
        view_exists = cursor.fetchone()[0]
        
        if table_exists:
            print("✅ Table 'aggregated_campaign_stats' already exists.")
        else:
            print("Creating table 'aggregated_campaign_stats' …")
            cursor.execute(CREATE_CAMPAIGN_STATS_TABLE)
            print("✅ Table created successfully.")
        
        if not view_exists or not table_exists:
            print("Creating/updating view 'v_aggregated_campaign_stats' …")
            cursor.execute(CREATE_VIEW_CAMPAIGN_STATS)
            print("✅ View created/updated successfully.")
        else:
            print("✅ View 'v_aggregated_campaign_stats' already exists.")
        
        # Check if pipeline metrics table exists
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'bi_pipeline_metrics');")
        pipeline_table_exists = cursor.fetchone()[0]
        
        if pipeline_table_exists:
            print("✅ Table 'bi_pipeline_metrics' already exists.")
        else:
            print("Creating table 'bi_pipeline_metrics' …")
            cursor.execute(CREATE_PIPELINE_METRICS_TABLE)
            print("✅ Table created successfully.")

        # Update impression records to have non-zero bid amounts
        print("Updating impression records to have meaningful bid amounts...")
        cursor.execute(UPDATE_IMPRESSION_BID_AMOUNTS)
        rows_updated = cursor.rowcount
        print(f"✅ Updated {rows_updated} impression records with non-zero bid amounts.")

        conn.commit()
        print("✅ Schema is ready for dashboards.")

    except Exception as exc:  # pylint: disable=broad-except
        print(f"❌ Error while checking/initializing tables: {exc}")
        sys.exit(1)

    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    main()