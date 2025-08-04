import os
import logging
import time
from contextlib import contextmanager
from typing import Optional, Dict, List, Any, Union
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from flask import Flask, jsonify, send_from_directory, Response, request
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_caching import Cache
from werkzeug.exceptions import HTTPException
import prometheus_client
from prometheus_client import Counter, Histogram, Gauge

# Configure structured logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('dashboards.log')
    ]
)
logger = logging.getLogger(__name__)

# Initialize Flask app with production configuration
app = Flask(__name__)
app.config.update(
    SECRET_KEY=os.getenv('SECRET_KEY', 'dev-key-change-in-production'),
    MAX_CONTENT_LENGTH=16 * 1024 * 1024,  # 16MB max request size
    JSON_SORT_KEYS=False,
    JSONIFY_PRETTYPRINT_REGULAR=False,
)

# Configure CORS for production
CORS(app, resources={
    r"/api/*": {
        "origins": os.getenv('ALLOWED_ORIGINS', '*').split(','),
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Configure rate limiting
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["1000 per hour", "100 per minute"]
)

# Configure caching
cache = Cache(app, config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': int(os.getenv('CACHE_TIMEOUT', '300'))
})

# Prometheus metrics
request_count = Counter('dashboard_requests_total', 'Total requests', ['method', 'endpoint', 'status'])
request_duration = Histogram('dashboard_request_duration_seconds', 'Request duration', ['method', 'endpoint'])
db_connection_errors = Counter('dashboard_db_errors_total', 'Database connection errors')
active_connections = Gauge('dashboard_db_active_connections', 'Active database connections')

# PostgreSQL connection configuration with environment variables
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME', 'tabularasadb'),
    'user': os.getenv('DB_USER', 'tabulauser'),
    'password': os.getenv('DB_PASSWORD', 'tabulapass'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'connect_timeout': int(os.getenv('DB_CONNECT_TIMEOUT', '10')),
    'options': '-c statement_timeout=30000'  # 30 second statement timeout
}

# Thread-safe connection pool
connection_pool: Optional[ThreadedConnectionPool] = None

def init_connection_pool(minconn: int = 1, maxconn: int = 10):
    """Initialize the database connection pool."""
    global connection_pool
    try:
        connection_pool = ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            **DB_CONFIG
        )
        logger.info(f"Database connection pool initialized with {minconn}-{maxconn} connections")
    except Exception as e:
        logger.error(f"Failed to initialize connection pool: {e}")
        db_connection_errors.inc()
        raise

@contextmanager
def get_db_connection():
    """Get a database connection from the pool with proper cleanup."""
    conn = None
    start_time = time.time()
    try:
        if not connection_pool:
            init_connection_pool()
        
        conn = connection_pool.getconn()
        active_connections.inc()
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        db_connection_errors.inc()
        raise
    finally:
        if conn and connection_pool:
            connection_pool.putconn(conn)
            active_connections.dec()
        duration = time.time() - start_time
        logger.debug(f"Database operation completed in {duration:.3f}s")

def execute_query(query: str, params: Optional[tuple] = None, fallback_func: Optional[callable] = None) -> Union[List[Dict], Dict]:
    """Execute SQL query with proper error handling and connection management."""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
                logger.debug(f"Query returned {len(result)} rows")
                return result
    except Exception as e:
        logger.error(f"Database query error: {e}")
        if fallback_func:
            logger.warning("Using fallback data due to database error")
            return fallback_func()
        raise

@app.errorhandler(Exception)
def handle_exception(e: Exception) -> tuple:
    """Global error handler for all exceptions."""
    if isinstance(e, HTTPException):
        response = e.get_response()
        response.data = jsonify({
            'error': e.name,
            'message': e.description,
            'status_code': e.code
        }).data
        response.content_type = "application/json"
        return response, e.code
    
    logger.exception("Unhandled exception occurred")
    return jsonify({
        'error': 'Internal Server Error',
        'message': 'An unexpected error occurred',
        'status_code': 500
    }), 500

@app.before_request
def before_request():
    """Log and track incoming requests."""
    logger.debug(f"Incoming request: {request.method} {request.path}")

@app.after_request
def after_request(response: Response) -> Response:
    """Track request metrics and add security headers."""
    request_count.labels(
        method=request.method,
        endpoint=request.endpoint or 'unknown',
        status=response.status_code
    ).inc()
    
    # Add security headers
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    
    return response

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

@app.route('/health')
def health_check():
    """Health check endpoint for monitoring."""
    try:
        # Test database connectivity
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT 1')
        return jsonify({'status': 'healthy', 'database': 'connected'}), 200
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy', 'database': 'disconnected', 'error': str(e)}), 503

@app.route('/metrics')
def metrics():
    """Expose Prometheus metrics."""
    return Response(prometheus_client.generate_latest(), mimetype='text/plain')

# API endpoints backed by PostgreSQL metrics
@app.route('/api/campaign_performance')
@limiter.limit("100 per minute")
@cache.cached(timeout=60)
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
        logger.warning("Returning empty campaign data due to database unavailability")
        return []
    
    return jsonify(execute_query(query, empty_campaigns))

@app.route('/api/performance/by_device')
@limiter.limit("100 per minute")
@cache.cached(timeout=60)
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
        logger.warning("Returning empty device breakdown due to database unavailability")
        return []
    
    return jsonify(execute_query(query, empty_device_data))

@app.route('/api/performance/by_category')
@limiter.limit("100 per minute")
@cache.cached(timeout=60)
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
        logger.warning("Returning empty category breakdown due to database unavailability")
        return []
    
    return jsonify(execute_query(query, empty_categories))

@app.route('/api/kpis')
@limiter.limit("100 per minute")
@cache.cached(timeout=30)
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
        logger.warning("Returning empty KPI set due to database unavailability")
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
@limiter.limit("100 per minute")
@cache.cached(timeout=120)
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
        logger.warning("Returning empty ROI trend due to database unavailability")
        return []
    
    result = execute_query(query, empty_roi_trend)
    logger.debug(f"ROI trend: Retrieved {len(result)} data points")
    if len(result) == 0:
        logger.debug("ROI trend: No data found, checking for any data without date filter")
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
        logger.debug(f"ROI trend: Retrieved {len(result)} data points with alternative query")
    
    return jsonify(result)

@app.route('/api/pipeline_health')
@limiter.limit("100 per minute")
@cache.cached(timeout=60)
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
        logger.warning("Returning empty pipeline health data due to database unavailability")
        return []
    
    return jsonify(execute_query(query, empty_pipeline_health))

@app.route('/api/performance/by_brand')
@limiter.limit("100 per minute")
@cache.cached(timeout=60)
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
        logger.warning("Returning empty brand breakdown due to database unavailability")
        return []
    
    return jsonify(execute_query(query, empty_brand_data))

@app.route('/api/performance/by_age_group')
@limiter.limit("100 per minute")
@cache.cached(timeout=60)
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
        logger.warning("Returning empty age-group breakdown due to database unavailability")
        return []

    return jsonify(execute_query(query, empty_age_group_data))

@app.route('/api/advertiser_metrics')
@limiter.limit("100 per minute")
@cache.cached(timeout=60)
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
        logger.warning("Returning empty advertiser metrics due to database unavailability")
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
@limiter.limit("100 per minute")
@cache.cached(timeout=60)
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
        logger.warning("Returning empty campaign efficiency data due to database unavailability")
        return []
    
    return jsonify(execute_query(query, empty_efficiency))

def init_app():
    """Initialize application resources."""
    try:
        # Initialize connection pool
        init_connection_pool(
            minconn=int(os.getenv('DB_POOL_MIN', '2')),
            maxconn=int(os.getenv('DB_POOL_MAX', '10'))
        )
        
        # Test database connectivity
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT 1')
        
        logger.info("Application initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize application: {e}")
        raise

if __name__ == '__main__':
    init_app()
    
    # Production server configuration
    port = int(os.getenv('PORT', '8080'))
    debug = os.getenv('FLASK_ENV') == 'development'
    
    if debug:
        logger.warning("Running in development mode")
        app.run(debug=True, port=port)
    else:
        # For production, use a proper WSGI server like gunicorn
        logger.info(f"Starting production server on port {port}")
        app.run(host='0.0.0.0', port=port, debug=False) 