#!/bin/bash

# Script to rebuild, restart, and help test the Q1 Simple Processor

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT_DIR="$( cd "${SCRIPT_DIR}/.." &> /dev/null && pwd )"
Q1_MODULE_DIR="${PROJECT_ROOT_DIR}/q1_realtime_stream_processing"
Q1_JAR_NAME="q1_realtime_stream_processing-0.0.1-SNAPSHOT-spring-boot.jar" # Changed to Spring Boot JAR
Q1_JAR_PATH="${Q1_MODULE_DIR}/target/${Q1_JAR_NAME}"
APP_LOG_DIR="${PROJECT_ROOT_DIR}/logs"
APP_LOG_FILE="${APP_LOG_DIR}/q1_simple_processor.log"
KAFKA_SAMPLE_DATA="/Users/sasha/IdeaProjects/tabularasa-bi-core/root/docker/spark_data/sample_ad_events.jsonl" # Ensure this path is correct

echo "=== Tabula Rasa BI - Q1 Simple Mode Test Helper ==="

# --- 0. Ensure logs directory exists ---
mkdir -p "$APP_LOG_DIR"
echo "[INFO] Log file will be: ${APP_LOG_FILE}"

# --- 1. Stop Previous Instance (Placeholder - Manual for now) ---
echo ""
echo "[STEP 1] Stopping previous application instance (if any)..."
echo "[WARN] This script doesn't automatically kill previous Java processes."
echo "Please ensure any previous instance of Q1RealtimeStreamProcessingApplication is stopped."
echo "You can try: pkill -f ${Q1_JAR_NAME}  OR  ps aux | grep ${Q1_JAR_NAME} and kill <PID>"
read -p "Press [Enter] to continue after ensuring the app is stopped..."

# --- 2. Clean and Rebuild Module ---
echo ""
echo "[STEP 2] Cleaning and rebuilding q1_realtime_stream_processing module..."
cd "${PROJECT_ROOT_DIR}" || exit
mvn clean package -pl q1_realtime_stream_processing -am
if [ $? -ne 0 ]; then
    echo "[ERROR] Maven build failed. Exiting."
    exit 1
fi
echo "[SUCCESS] Maven build complete."

if [ ! -f "${Q1_JAR_PATH}" ]; then
    echo "[ERROR] JAR file not found at ${Q1_JAR_PATH}. Build might have failed or naming is different. Exiting."
    exit 1
fi

# --- 3. Run Application ---
echo ""
echo "[STEP 3] Starting Q1RealtimeStreamProcessingApplication with 'simple' profile..."
# Run in background and redirect output to log file
nohup java -jar "${Q1_JAR_PATH}" --spring.profiles.active=simple > "${APP_LOG_FILE}" 2>&1 &
APP_PID=$!
echo "[INFO] Application starting in background (PID: ${APP_PID}). Logs: ${APP_LOG_FILE}"
echo "[INFO] Waiting a few seconds for the application to initialize..."
sleep 15 # Adjust as needed

# Check if app is running (simple check, might need refinement)
if ! ps -p ${APP_PID} > /dev/null; then
   echo "[ERROR] Application with PID ${APP_PID} does not seem to be running. Check ${APP_LOG_FILE} for errors."
   # exit 1 # Optional: exit if app fails to start
fi

# --- 4. Reload Kafka Data ---
echo ""
echo "[STEP 4] Reloading sample data into Kafka topic 'ad-events'..."
cat "${KAFKA_SAMPLE_DATA}" | docker exec -i kafka kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ad-events
if [ $? -ne 0 ]; then
    echo "[WARN] Kafka data load might have failed. Please check."
fi
echo "[INFO] Kafka data loaded. Waiting 60 seconds for processing..."
sleep 60

# --- 5. Verification & Next Steps ---
echo ""
echo "[STEP 5] Verification - Check data and API:"
echo "--------------------------------------------------"
echo "Tail application log:"
echo "  tail -f ${APP_LOG_FILE}"
echo ""
echo "Check PostgreSQL for cmp001 data:"
echo "  docker exec tabularasa_postgres_db psql -U airflow -d airflow -c \"SELECT campaign_id, event_type, TO_CHAR(window_start_time, 'YYYY-MM-DD HH24:MI:SS') as window_start_time, event_count, total_bid_amount, TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS') as updated_at FROM aggregated_campaign_stats WHERE campaign_id = 'cmp001' ORDER BY window_start_time, event_type;\" | cat"
echo ""
echo "Check PostgreSQL for cmp002 data:"
echo "  docker exec tabularasa_postgres_db psql -U airflow -d airflow -c \"SELECT campaign_id, event_type, TO_CHAR(window_start_time, 'YYYY-MM-DD HH24:MI:SS') as window_start_time, event_count, total_bid_amount, TO_CHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS') as updated_at FROM aggregated_campaign_stats WHERE campaign_id = 'cmp002' ORDER BY window_start_time, event_type;\" | cat"
echo ""
echo "Test API for cmp001:"
echo "  curl -X GET \"http://localhost:8083/api/v1/campaign-stats/cmp001?startTime=2024-07-28T10:00:00&endTime=2024-07-28T10:04:00\" | cat"
echo ""
echo "Test API for cmp002:"
echo "  curl -X GET \"http://localhost:8083/api/v1/campaign-stats/cmp002?startTime=2024-07-28T10:00:00&endTime=2024-07-28T10:16:00\" | cat"
echo "--------------------------------------------------"
echo "[INFO] Script finished. Please perform manual checks using the commands above."
echo "[INFO] To stop the background application: kill ${APP_PID}"
