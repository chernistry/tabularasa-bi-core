from flask import Flask, jsonify, send_from_directory
import os
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

# Конфигурация подключения к PostgreSQL
DB_CONFIG = {
    'dbname': 'airflow',
    'user': 'tabulauser',
    'password': 'tabulapass',
    'host': 'localhost',
    'port': '5432'
}

# Функция для подключения к БД
def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        return None

# Функция для запроса данных с поддержкой fallback на моковые данные
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
            print(f"Ошибка запроса к БД: {e}")
    
    # Fallback на моковые данные если БД недоступна
    print("Используем fallback на моковые данные")
    if fallback_func:
        return fallback_func()
    return []

# Статические файлы
@app.route('/', defaults={'path': 'index.html'})
@app.route('/<path:path>')
def serve_static(path):
    if path == "" or path == "/":
        path = "index.html"
    
    # Пробуем найти файл в корне проекта
    if os.path.exists(path):
        return send_from_directory('.', path)
    
    # Возможные директории для поиска файла
    possible_dirs = ['.', 'dashboards', 'dashboards/shared', 
                    'dashboards/ceo_executive_pulse', 
                    'dashboards/advertiser_campaign_performance',
                    'dashboards/bi_pipeline_health_data_trust']
    
    for directory in possible_dirs:
        file_path = os.path.join(directory, path)
        if os.path.exists(file_path):
            return send_from_directory(directory, os.path.basename(path))
    
    return "Файл не найден", 404

# API эндпоинты с реальными данными из PostgreSQL
@app.route('/api/campaign_performance')
def campaign_performance():
    query = """
    SELECT
      campaign_id,
      SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END) AS impressions,
      SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END) AS clicks,
      SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END) AS conversions,
      SUM(total_spend_usd) AS spend_usd,
      CASE WHEN SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END) > 0
        THEN (SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END)::float / SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END)) * 100
        ELSE 0 END AS ctr
    FROM aggregated_campaign_stats
    GROUP BY campaign_id
    ORDER BY spend_usd DESC
    LIMIT 15;
    """
    
    def mock_campaigns():
        campaigns = []
        for i in range(1, 16):
            campaigns.append({
                'campaign_id': i,
                'spend_usd': random.uniform(1000, 50000),
                'impressions': random.randint(100000, 1000000),
                'clicks': random.randint(5000, 50000),
                'conversions': random.randint(50, 500),
                'ctr': random.uniform(0.5, 5.0)
            })
        return campaigns
    
    return jsonify(execute_query(query, mock_campaigns))

@app.route('/api/performance/by_device')
def performance_by_device():
    query = """
    SELECT
      COALESCE(device_type, 'Unknown') as device_type,
      SUM(total_sales_amount_euro) as total_revenue,
      CASE WHEN SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END) > 0
        THEN (SUM(CASE WHEN event_type = 'conversion' THEN event_count ELSE 0 END)::float / SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END)) * 100
        ELSE 0 END as conversion_rate
    FROM aggregated_campaign_stats
    GROUP BY device_type
    ORDER BY total_revenue DESC;
    """
    
    def mock_device_data():
        devices = ['Mobile', 'Desktop', 'Tablet', 'Smart TV', 'Other']
        device_data = []
        for device in devices:
            device_data.append({
                'device_type': device,
                'total_revenue': random.uniform(10000, 100000),
                'conversion_rate': random.uniform(1.0, 8.0)
            })
        return device_data
    
    return jsonify(execute_query(query, mock_device_data))

@app.route('/api/performance/by_category')
def performance_by_category():
    query = """
    SELECT
      COALESCE(product_category_1, 'Unknown') as product_category_1,
      SUM(total_sales_amount_euro) as total_revenue
    FROM aggregated_campaign_stats
    GROUP BY product_category_1
    ORDER BY total_revenue DESC
    LIMIT 7;
    """
    
    def mock_categories():
        categories = []
        for i in range(1, 8):
            categories.append({
                'product_category_1': i,
                'total_revenue': random.uniform(5000, 80000)
            })
        return categories
    
    return jsonify(execute_query(query, mock_categories))

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
      SUM(total_spend_usd) AS spend_usd
    FROM aggregated_campaign_stats;
    """
    
    def mock_kpis():
        return {
            'impressions': random.randint(5000000, 10000000),
            'clicks': random.randint(250000, 500000),
            'conversions': random.randint(10000, 50000),
            'ctr': random.uniform(1.5, 5.0),
            'spend_usd': random.uniform(1000000, 5000000)
        }
    
    result = execute_query(query, mock_kpis)
    # Если результат запроса - список словарей, берем первый элемент
    if isinstance(result, list) and result:
        return jsonify(result[0])
    return jsonify(result)

@app.route('/api/roi_trend')
def roi_trend():
    query = """
    SELECT
      window_start_time,
      SUM(total_sales_amount_euro) / NULLIF(SUM(total_spend_usd),0) AS roi
    FROM aggregated_campaign_stats
    WHERE window_start_time > NOW() - INTERVAL '30 days'
    GROUP BY window_start_time
    ORDER BY window_start_time;
    """
    
    def mock_roi_trend():
        trend_data = []
        start_date = datetime.now() - timedelta(days=30)
        
        for i in range(30):
            current_date = start_date + timedelta(days=i)
            trend_data.append({
                'window_start_time': current_date.strftime('%Y-%m-%d'),
                'roi': random.uniform(1.5, 4.5)
            })
        
        return trend_data
    
    return jsonify(execute_query(query, mock_roi_trend))

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
    
    def mock_pipeline_health():
        health_data = []
        start_date = datetime.now() - timedelta(days=30)
        
        for i in range(30):
            current_date = start_date + timedelta(days=i)
            # Создаем аномалию в середине месяца
            is_anomaly = i == 15
            
            health_data.append({
                'day': current_date.strftime('%Y-%m-%d'),
                'avg_processing_time': random.uniform(100, 180) if not is_anomaly else random.uniform(400, 600),
                'job_count': random.randint(950, 1050),
                'success_rate': random.uniform(98, 100) if not is_anomaly else random.uniform(80, 90),
                'data_freshness_hours': random.uniform(0.8, 1.2) if not is_anomaly else random.uniform(4, 6),
                'quality_score': random.uniform(98, 100) if not is_anomaly else random.uniform(90, 95)
            })
        
        return health_data
    
    return jsonify(execute_query(query, mock_pipeline_health))

@app.route('/api/performance/by_brand')
def performance_by_brand():
    query = """
    SELECT
      COALESCE(product_brand, 'Unknown') as product_brand,
      SUM(total_sales_amount_euro) as total_revenue
    FROM aggregated_campaign_stats
    GROUP BY product_brand
    ORDER BY total_revenue DESC
    LIMIT 10;
    """
    
    def mock_brand_data():
        brands = ['Nike', 'Adidas', 'Puma', 'Apple', 'Samsung', 'Sony', 'Gucci', 'Prada', 'Other', 'Unknown']
        brand_data = []
        for brand in brands:
            brand_data.append({
                'product_brand': brand,
                'total_revenue': random.uniform(20000, 150000)
            })
        return brand_data
    
    return jsonify(execute_query(query, mock_brand_data))

@app.route('/api/performance/by_age_group')
def performance_by_age_group():
    query = """
    SELECT
      COALESCE(product_age_group, 'Unknown') as product_age_group,
      SUM(total_sales_amount_euro) as total_revenue,
      CASE WHEN SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END) > 0
        THEN (SUM(CASE WHEN event_type = 'click' THEN event_count ELSE 0 END)::float / SUM(CASE WHEN event_type = 'impression' THEN event_count ELSE 0 END)) * 100
        ELSE 0 END AS ctr
    FROM aggregated_campaign_stats
    GROUP BY product_age_group
    ORDER BY product_age_group;
    """

    def mock_age_group_data():
        age_groups = ['18-24', '25-34', '35-44', '45-54', '55+']
        age_group_data = []
        for age_group in age_groups:
            age_group_data.append({
                'product_age_group': age_group,
                'total_revenue': random.uniform(50000, 200000),
                'ctr': random.uniform(1.0, 5.0)
            })
        return age_group_data

    return jsonify(execute_query(query, mock_age_group_data))

if __name__ == '__main__':
    # Проверка подключения к БД при запуске
    conn = get_db_connection()
    if conn:
        print("Успешное подключение к PostgreSQL!")
        conn.close()
    else:
        print("ВНИМАНИЕ: Не удалось подключиться к PostgreSQL. Будут использованы моковые данные.")
    
    app.run(debug=True, port=8080) 