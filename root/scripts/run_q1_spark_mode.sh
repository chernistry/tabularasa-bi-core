#!/bin/bash

# Script to build, run and test Q1 with Spark Structured Streaming

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT_DIR="$( cd "${SCRIPT_DIR}/.." &> /dev/null && pwd )"
Q1_MODULE_DIR="${PROJECT_ROOT_DIR}/q1_realtime_stream_processing"
Q1_JAR_NAME="q1_realtime_stream_processing-0.0.1-SNAPSHOT-spring-boot.jar"
Q1_JAR_PATH="${Q1_MODULE_DIR}/target/${Q1_JAR_NAME}"
APP_LOG_DIR="${PROJECT_ROOT_DIR}/logs"
APP_LOG_FILE="${APP_LOG_DIR}/q1_spark_processor.log"
KAFKA_SAMPLE_DATA="${PROJECT_ROOT_DIR}/docker/spark_data/sample_ad_events.jsonl"

echo "=== Tabula Rasa BI - Q1 Spark Mode Test Helper ==="

# --- 0. Ensure logs directory exists ---
mkdir -p "$APP_LOG_DIR"
echo "[INFO] Log file will be: ${APP_LOG_FILE}"

# --- 1. Stop Previous Instance (if any) ---
echo ""
echo "[STEP 1] Stopping previous application instance (if any)..."
echo "Attempting to find and kill previous instance..."
EXISTING_PID=$(ps -ef | grep "${Q1_JAR_NAME}" | grep -v grep | awk '{print $2}')
if [ ! -z "$EXISTING_PID" ]; then
    echo "Killing process with PID: $EXISTING_PID"
    kill $EXISTING_PID
    sleep 5
    # Check if process was successfully killed
    if ps -p $EXISTING_PID > /dev/null; then
        echo "[WARNING] Process $EXISTING_PID still running. Try kill -9 $EXISTING_PID manually."
    else
        echo "[INFO] Previous process successfully terminated."
    fi
else
    echo "[INFO] No previous instance found running."
fi

# --- 2. Clean and Rebuild Module ---
echo ""
echo "[STEP 2] Cleaning and rebuilding q1_realtime_stream_processing module..."
cd "${PROJECT_ROOT_DIR}" || exit
mvn clean package -pl q1_realtime_stream_processing -am -DskipTests
if [ $? -ne 0 ]; then
    echo "[ERROR] Maven build failed. Exiting."
    exit 1
fi
echo "[SUCCESS] Maven build complete."

if [ ! -f "${Q1_JAR_PATH}" ]; then
    echo "[ERROR] JAR file not found at ${Q1_JAR_PATH}. Build might have failed or naming is different. Exiting."
    exit 1
fi

# --- 3. Ensure PostgreSQL table exists ---
echo ""
echo "[STEP 3] Ensuring PostgreSQL aggregated_campaign_stats table exists..."
docker exec -i tabularasa_postgres_db psql -U airflow -d airflow < "${Q1_MODULE_DIR}/ddl/postgres_aggregated_campaign_stats.sql"
if [ $? -ne 0 ]; then
    echo "[WARNING] PostgreSQL table creation might have failed. Continuing anyway."
else
    echo "[SUCCESS] PostgreSQL table check complete."
fi

# --- 4. Run Application ---
echo ""
echo "[STEP 4] Starting Q1RealtimeStreamProcessingApplication with 'spark' profile..."
# Export required Java options for Spark/Hadoop compatibility
export _JAVA_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
-Djava.security.manager=allow"

# Run in background and redirect output to log file
nohup java -Dspring.profiles.active=spark -jar "${Q1_JAR_PATH}" > "${APP_LOG_FILE}" 2>&1 &
APP_PID=$!
echo "[INFO] Application starting in background (PID: ${APP_PID}). Logs: ${APP_LOG_FILE}"
echo "[INFO] Waiting a few seconds for the application to initialize..."
sleep 15 # Adjust as needed

# Check if app is running
if ! ps -p ${APP_PID} > /dev/null; then
   echo "[ERROR] Application with PID ${APP_PID} does not seem to be running. Check ${APP_LOG_FILE} for errors."
   exit 1
fi

# --- 5. Reload Kafka Data ---
echo ""
echo "[STEP 5] Reloading sample data into Kafka topic 'ad-events'..."
cat "${KAFKA_SAMPLE_DATA}" | docker exec -i kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ad-events
if [ $? -ne 0 ]; then
    echo "[WARN] Kafka data load might have failed. Please check."
else
    echo "[INFO] Kafka data loaded successfully."
fi
echo "[INFO] Waiting 60 seconds for Spark processing..."
sleep 60

# --- 6. Verification & Next Steps ---
echo ""
echo "[STEP 6] Verification - Check data and API:"
echo "--------------------------------------------------"
echo "Tail application log:"
echo "  tail -f ${APP_LOG_FILE}"
echo ""
echo "Visit Spark UI in browser (if available):"
echo "  http://localhost:4040"
echo ""
echo "Check PostgreSQL for aggregated data:"
echo "  docker exec tabularasa_postgres_db psql -U airflow -d airflow -c \"SELECT campaign_id, event_type, TO_CHAR(window_start_time, 'YYYY-MM-DD HH24:MI:SS') as window_start_time, event_count, total_bid_amount, TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS') as updated_at FROM aggregated_campaign_stats ORDER BY window_start_time, campaign_id, event_type;\" | cat"
echo ""
echo "Test API endpoints:"
echo "  curl -X GET \"http://localhost:8083/api/v1/campaign-stats/5DC678F1375D62E9EA55EE1309B828DB?startTime=2020-08-31T00:00:00&endTime=2020-09-01T23:59:59\" | cat"
echo ""
echo "Test API for different campaign and time range:"
echo "  curl -X GET \"http://localhost:8083/api/v1/campaign-stats/8F7368B8AF08640770579ABB94D80993?startTime=2020-08-31T00:00:00&endTime=2020-09-01T23:59:59\" | cat"
echo "--------------------------------------------------"
echo "[INFO] Script finished. Please perform manual checks using the commands above."
echo "[INFO] To stop the background application: kill ${APP_PID}" 