#!/usr/bin/env python
"""
Utility script to create (and optionally truncate) PostgreSQL tables.

IMPORTANT:
• This file must NEVER create or insert mocked-up data.  
• The live data pipeline is responsible for populating the tables.  
"""

import psycopg2
# execute_batch is kept for possible future bulk *real* inserts,
# but no mocked data should be generated here.
from psycopg2.extras import execute_batch  

# NOTE: The following imports were previously used for mock generators.
# They are kept commented out to make it clear that no mocks should be created.
# import random
# from datetime import datetime, timedelta


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
    campaign_id INTEGER NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    window_start_time TIMESTAMP NOT NULL,
    device_type VARCHAR(50),
    country_code VARCHAR(2),
    product_brand VARCHAR(100),
    product_age_group VARCHAR(20),
    product_category_1 INTEGER,
    product_category_2 INTEGER,
    product_category_3 INTEGER,
    product_category_4 INTEGER,
    product_category_5 INTEGER,
    product_category_6 INTEGER,
    product_category_7 INTEGER,
    event_count BIGINT,
    spend_usd DECIMAL(10, 2),
    total_sales_amount_euro DECIMAL(10, 2),
    total_sales_count BIGINT,
    updated_at TIMESTAMP,
    PRIMARY KEY (
        campaign_id, event_type, window_start_time, device_type, country_code, 
        product_brand, product_age_group, product_category_1, product_category_2, 
        product_category_3, product_category_4, product_category_5, product_category_6, 
        product_category_7
    )
);
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
# Maintenance SQL
# --------------------------------------------------------------------------- #
TRUNCATE_TABLES = """
TRUNCATE TABLE aggregated_campaign_stats;
TRUNCATE TABLE bi_pipeline_metrics;
"""

# In a safer variant:
DROP_TABLES_IF_EXISTS = """
DROP TABLE IF EXISTS aggregated_campaign_stats;
DROP TABLE IF EXISTS bi_pipeline_metrics;
"""

# --------------------------------------------------------------------------- #
# DEPRECATED MOCK GENERATORS
# --------------------------------------------------------------------------- #
# The following functions previously generated synthetic data.
# They are kept entirely commented-out to emphasize that *no* mock
# generation should happen in this file.
#
def generate_campaign_data(start_date, days=30):
    """Generates test data for campaigns for the specified period"""
    data = []
    campaign_ids = list(range(1, 16))
    devices = ['Desktop', 'Mobile', 'Tablet', 'Smart TV', 'Other']
    countries = ['US', 'DE', 'JP', 'UK', 'FR', 'CA', 'AU']
    product_categories = list(range(1, 101))
    brands = ['Nike', 'Adidas', 'Puma', 'Apple', 'Samsung', 'Sony', 'Gucci', 'Prada']
    age_groups = ['18-24', '25-34', '35-44', '45-54', '55+']
    event_types = ['impression', 'click', 'conversion']

    current_date = start_date

    for day in range(days):
        for hour in range(24):
            timestamp = current_date + timedelta(days=day, hours=hour)
            
            for campaign_id in campaign_ids:
                # Generate one set of attributes per campaign hour
                device = random.choice(devices)
                country = random.choice(countries)
                brand = random.choice(brands)
                age_group = random.choice(age_groups)
                categories = random.sample(product_categories, 7)

                base_impressions = random.randint(5000, 20000)
                ctr = random.uniform(0.005, 0.02)
                conv_rate = random.uniform(0.02, 0.05)
                
                clicks = int(base_impressions * ctr)
                conversions = int(clicks * conv_rate)

                # For each event type, create its own record
                for event_type in event_types:
                    count = 0
                    spend = 0.0
                    sales_amount = 0.0
                    sales_count = 0

                    if event_type == 'impression':
                        count = base_impressions
                        spend = float(count * random.uniform(0.001, 0.003))
                    elif event_type == 'click':
                        count = clicks
                    elif event_type == 'conversion':
                        count = conversions
                        sales_count = conversions
                        sales_amount = round(conversions * random.uniform(10.0, 500.0) * random.uniform(0.8, 1.2), 2)

                    data.append((
                        campaign_id,
                        event_type,
                        timestamp,
                        device,
                        country,
                        brand,
                        age_group,
                        categories[0],
                        categories[1],
                        categories[2],
                        categories[3],
                        categories[4],
                        categories[5],
                        categories[6],
                        count,
                        spend,
                        sales_amount,
                        sales_count,
                        datetime.now()
                    ))
    
    return data

def generate_pipeline_metrics(start_date, days=30):

        # Insert campaign data
        print(f"Inserting {len(campaign_data)} campaign records...")
        insert_campaign_sql = """
        INSERT INTO aggregated_campaign_stats 
        (campaign_id, event_type, window_start_time, device_type, country_code, 
         product_brand, product_age_group, product_category_1, product_category_2, 
         product_category_3, product_category_4, product_category_5, product_category_6, 
         product_category_7, event_count, spend_usd, total_sales_amount_euro, 
         total_sales_count, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_batch(cursor, insert_campaign_sql, campaign_data)
        
        # Insert pipeline metrics data



# --------------------------------------------------------------------------- #
# Main entry point
# --------------------------------------------------------------------------- #
def main() -> None:
    """Create database tables (and optionally truncate them)."""
    print("Initializing PostgreSQL tables …")

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create tables
        print("Creating tables …")
        cursor.execute(CREATE_CAMPAIGN_STATS_TABLE)
        cursor.execute(CREATE_PIPELINE_METRICS_TABLE)

        # Truncate or recreate tables
        print("Recreating tables for fresh data …")
        try:
            cursor.execute(DROP_TABLES_IF_EXISTS)
            # Re-create tables after dropping
            cursor.execute(CREATE_CAMPAIGN_STATS_TABLE)
            cursor.execute(CREATE_PIPELINE_METRICS_TABLE)
            print("Tables successfully recreated.")
        except Exception as exc:
            print(f"Warning: Could not drop/recreate tables: {exc}")
            print("Trying to truncate instead...")
            try:
                cursor.execute(TRUNCATE_TABLES)
                print("Tables successfully truncated.")
            except Exception as trunc_exc:
                print(f"Warning: Could not truncate tables: {trunc_exc}")
                print("Tables will keep existing data.")

        # ---------------------------------------------------------------
        # NOTE: No mock data insertion here. Real data will be ingested
        #       by the production pipeline.
        # ---------------------------------------------------------------

        conn.commit()
        print("Done! Schema is ready for real data.")

    except Exception as exc:  # pylint: disable=broad-except
        print(f"Error while initializing tables: {exc}")

    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    main()