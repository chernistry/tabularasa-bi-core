from flask import Flask, jsonify, send_from_directory
import os
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# PostgreSQL connection configuration
DB_CONFIG = {
    'dbname': 'tabularasadb',
    'user': 'tabulauser',
    'password': 'tabulapass',
    'host': 'localhost',
    'port': '5432'
}

# Helper to obtain a database connection
def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"PostgreSQL connection error: {e}")
        return None

# Execute SQL query with optional empty fallback
def execute_query(query, fallback_func=None):
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result
        except Exception as e:
            print(f"Database query error: {e}")
    
    # WARNING: Database unavailable. Returning empty results instead of real metrics.
    if fallback_func:
        return fallback_func()
    return []

# Static file handler
@app.route('/', defaults={'path': 'index.html'})
@app.route('/<path:path>')
def serve_static(path):
    if path == "" or path == "/":
        path = "index.html"
    
    # Try to find file in project root first
    if os.path.exists(path):
        return send_from_directory('.', path)
    
    # Fallback directories to search for the file
    possible_dirs = ['.', 'dashboards', 'dashboards/shared', 
                    'dashboards/ceo_executive_pulse', 
                    'dashboards/advertiser_campaign_performance',
                    'dashboards/bi_pipeline_health_data_trust']
    
    for directory in possible_dirs:
        file_path = os.path.join(directory, path)
        if os.path.exists(file_path):
            return send_from_directory(directory, os.path.basename(path))
    
    return "File not found", 404

# API endpoints backed by PostgreSQL metrics
@app.route('/api/campaign_performance')
def campaign_performance():
    query = """
    SELECT
      campaign_id,
      SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END) AS impressions,
      SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END) AS clicks,
      SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END) AS conversions,
      SUM(spend_usd) AS spend_usd,
      CASE WHEN SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END) > 0
        THEN (SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END)::float / SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END)) * 100
        ELSE 0 END AS ctr
    FROM v_aggregated_campaign_stats
    GROUP BY campaign_id
    ORDER BY spend_usd DESC
    LIMIT 15;
    """
    
    def empty_campaigns():
        print("WARNING: Returning empty campaign data.")
        return []
    
    return jsonify(execute_query(query, empty_campaigns))

@app.route('/api/performance/by_device')
def performance_by_device():
    query = """
    SELECT
      COALESCE(device_type, 'Unknown') as device_type,
      SUM(total_sales_amount_euro) as total_revenue,
      CASE WHEN SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END) > 0
        THEN (SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END)::float / SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END)) * 100
        ELSE 0 END as conversion_rate
    FROM v_aggregated_campaign_stats
    GROUP BY device_type
    ORDER BY total_revenue DESC;
    """
    
    def empty_device_data():
        print("WARNING: Returning empty device breakdown.")
        return []
    
    return jsonify(execute_query(query, empty_device_data))

@app.route('/api/performance/by_category')
def performance_by_category():
    query = """
    SELECT
      COALESCE(product_category_1, 0) as product_category_1,
      SUM(total_sales_amount_euro) as total_revenue
    FROM v_aggregated_campaign_stats
    WHERE product_category_1 IS NOT NULL
    GROUP BY product_category_1
    ORDER BY total_revenue DESC
    LIMIT 7;
    """
    
    def empty_categories():
        print("WARNING: Returning empty category breakdown.")
        return []
    
    return jsonify(execute_query(query, empty_categories))

@app.route('/api/kpis')
def kpis():
    query = """
    SELECT
      SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END) AS impressions,
      SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END) AS clicks,
      SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END) AS conversions,
      CASE WHEN SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END) > 0
        THEN (SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END)::float / SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END)) * 100
        ELSE 0 END AS ctr,
      SUM(spend_usd) AS spend_usd
    FROM v_aggregated_campaign_stats;
    """
    
    def empty_kpis():
        print("WARNING: Returning empty KPI set.")
        return {
            'impressions': 0,
            'clicks': 0,
            'conversions': 0,
            'ctr': 0,
            'spend_usd': 0
        }
    
    result = execute_query(query, empty_kpis)
    # If the query result is a list of dictionaries, take the first element
    if isinstance(result, list) and result:
        return jsonify(result[0])
    return jsonify(result)

@app.route('/api/roi_trend')
def roi_trend():
    query = """
    WITH daily_metrics AS (
        SELECT
            DATE(window_start_time) as day,
            SUM(total_sales_amount_euro) as total_revenue,
            SUM(spend_usd) as total_spend
        FROM v_aggregated_campaign_stats
        WHERE window_start_time > NOW() - INTERVAL '90 days'
        GROUP BY DATE(window_start_time)
    )
    SELECT
        day as window_start_time,
        CASE 
            WHEN total_spend > 0 THEN total_revenue / total_spend
            ELSE 0
        END AS roi
    FROM daily_metrics
    ORDER BY day;
    """
    
    def empty_roi_trend():
        print("WARNING: Returning empty ROI trend.")
        return []
    
    result = execute_query(query, empty_roi_trend)
    print(f"DEBUG ROI trend: Retrieved {len(result)} data points")
    if len(result) == 0:
        print("DEBUG ROI trend: No data found, checking for any data without date filter")
        # Try without date filter if no data found
        alt_query = """
        WITH daily_metrics AS (
            SELECT
                DATE(window_start_time) as day,
                SUM(total_sales_amount_euro) as total_revenue,
                SUM(spend_usd) as total_spend
            FROM v_aggregated_campaign_stats
            GROUP BY DATE(window_start_time)
        )
        SELECT
            day as window_start_time,
            CASE 
                WHEN total_spend > 0 THEN total_revenue / total_spend
                ELSE 0
            END AS roi
        FROM daily_metrics
        ORDER BY day;
        """
        result = execute_query(alt_query, empty_roi_trend)
        print(f"DEBUG ROI trend: Retrieved {len(result)} data points with alternative query")
    
    return jsonify(result)

@app.route('/api/pipeline_health')
def pipeline_health():
    query = """
    SELECT 
        date_trunc('day', process_time) as day,
        AVG(processing_time_sec) as avg_processing_time,
        COUNT(*) as job_count,
        SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as success_rate,
        MAX(data_freshness_minutes) / 60 as data_freshness_hours,
        AVG(quality_score) as quality_score
    FROM bi_pipeline_metrics
    WHERE process_time > NOW() - INTERVAL '30 days'
    GROUP BY date_trunc('day', process_time)
    ORDER BY day
    """
    
    def empty_pipeline_health():
        print("WARNING: Returning empty pipeline health data.")
        return []
    
    return jsonify(execute_query(query, empty_pipeline_health))

@app.route('/api/performance/by_brand')
def performance_by_brand():
    query = """
    SELECT
      COALESCE(product_brand, 'Unknown') as product_brand,
      SUM(total_sales_amount_euro) as total_revenue
    FROM v_aggregated_campaign_stats
    GROUP BY product_brand
    ORDER BY total_revenue DESC
    LIMIT 10;
    """
    
    def empty_brand_data():
        print("WARNING: Returning empty brand breakdown.")
        return []
    
    return jsonify(execute_query(query, empty_brand_data))

@app.route('/api/performance/by_age_group')
def performance_by_age_group():
    query = """
    SELECT
      COALESCE(product_age_group, 'Unknown') as product_age_group,
      SUM(total_sales_amount_euro) as total_revenue,
      CASE WHEN SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END) > 0
        THEN (SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END)::float / SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END)) * 100
        ELSE 0 END AS ctr
    FROM v_aggregated_campaign_stats
    GROUP BY product_age_group
    ORDER BY product_age_group;
    """

    def empty_age_group_data():
        print("WARNING: Returning empty age-group breakdown.")
        return []

    return jsonify(execute_query(query, empty_age_group_data))

@app.route('/api/advertiser_metrics')
def advertiser_metrics():
    query = """
    SELECT
      -- Cost Per Acquisition (CPA): Total spend divided by number of conversions
      CASE 
          WHEN SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END) > 0 
          THEN SUM(spend_usd) / SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END)
          ELSE 0 
      END AS cpa,
      
      -- Return On Ad Spend (ROAS): Total revenue divided by total spend
      CASE 
          WHEN SUM(spend_usd) > 0 
          THEN SUM(total_sales_amount_euro) / SUM(spend_usd)
          ELSE 0 
      END AS roas,
      
      -- Conversion Rate: Conversions divided by clicks
      CASE 
          WHEN SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END) > 0
          THEN (SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END)::float / 
                SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END)) * 100
          ELSE 0 
      END AS conversion_rate
    FROM v_aggregated_campaign_stats;
    """
    
    def empty_metrics():
        print("WARNING: Returning empty advertiser metrics.")
        return {
            'cpa': 0,
            'roas': 0,
            'conversion_rate': 0
        }
    
    result = execute_query(query, empty_metrics)
    # If the query result is a list of dictionaries, take the first element
    if isinstance(result, list) and result:
        return jsonify(result[0])
    return jsonify(result)

@app.route('/api/campaign_efficiency')
def campaign_efficiency():
    query = """
    WITH campaign_metrics AS (
        SELECT
            campaign_id,
            SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END) AS impressions,
            SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END) AS clicks,
            SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END) AS conversions,
            SUM(spend_usd) AS spend,
            SUM(total_sales_amount_euro) AS revenue
        FROM v_aggregated_campaign_stats
        GROUP BY campaign_id
    )
    SELECT
        campaign_id,
        spend,
        CASE 
            WHEN spend > 0 THEN revenue / spend
            ELSE 0
        END AS roi,
        conversions,
        CASE 
            WHEN impressions > 0 THEN (clicks::float / impressions) * 100
            ELSE 0
        END AS ctr,
        CASE 
            WHEN clicks > 0 THEN (conversions::float / clicks) * 100
            ELSE 0
        END AS conversion_rate
    FROM campaign_metrics
    WHERE spend > 0
    ORDER BY spend DESC
    LIMIT 20;
    """
    
    def empty_efficiency():
        print("WARNING: Returning empty campaign efficiency data.")
        return []
    
    return jsonify(execute_query(query, empty_efficiency))

if __name__ == '__main__':
    # Validate database connectivity on server start-up
    conn = get_db_connection()
    if conn:
        print("Successfully connected to PostgreSQL.")
        conn.close()
    else:
        print("WARNING: Unable to connect to PostgreSQL. Empty data will be served.")
    
    app.run(debug=True, port=8080) 