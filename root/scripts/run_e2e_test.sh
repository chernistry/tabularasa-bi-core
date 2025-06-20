#!/bin/bash

# Exit on error
set -e

# ==============================================================================
# E2E TEST AND LIVE STREAMING ORCHESTRATOR
# ==============================================================================

# Ensure script is run from its own directory
cd "$(dirname "$0")"

# --- Cleanup function & trap ---
# This function is called when the script receives a signal (like Ctrl+C)
# or when it exits, ensuring all background processes are terminated.
cleanup() {
  echo -e "\n\n🧹 Cleaning up test environment..."

  if [[ -n "${SPARK_SUBMIT_PID:-}" ]] && ps -p "$SPARK_SUBMIT_PID" > /dev/null; then
    echo "🔪 Terminating Spark submit (PID $SPARK_SUBMIT_PID)..."
    kill "$SPARK_SUBMIT_PID" 2>/dev/null || true
    wait "$SPARK_SUBMIT_PID" 2>/dev/null
  fi

  # Kill Python producer if it's still running
  if [[ -n "${PRODUCER_PID:-}" ]] && ps -p "$PRODUCER_PID" > /dev/null; then
    echo "🔪 Terminating Python producer (PID $PRODUCER_PID)..."
    kill "$PRODUCER_PID" 2>/dev/null || true
    wait "$PRODUCER_PID" 2>/dev/null
  fi

  # Bring Docker stack down
  echo "🔥 Shutting down Docker services..."
  pushd ../docker >/dev/null
  docker-compose -f docker-compose.test.yml down -v --remove-orphans >/dev/null 2>&1
  popd >/dev/null
  echo "✅ Cleanup complete. Bye!"
  exit 0
}

# Catch Ctrl-C (SIGINT) and script termination (SIGTERM) signals
trap cleanup INT TERM

# Set the absolute project root path
export PROJECT_ROOT
PROJECT_ROOT=$(cd ../.. && pwd)

echo "---"
echo "Project Root determined as: ${PROJECT_ROOT}"
echo "---"

# --- E2E TEST ---
echo "🚀 Starting End-to-End test..."

echo "🛠️ Preparing environment..."

# Build Spark application
echo "📦 Building Spark application..."
cd ../q1_realtime_stream_processing
mvn clean package -DskipTests >/dev/null
cd ../scripts

# Ensure spark_apps directory exists and copy the correct JAR
# We copy the one WITHOUT the '-exec' classifier for Spark
SPARK_APPS_DIR="../docker/spark_apps"
JAR_SRC="../q1_realtime_stream_processing/target/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar"
JAR_DEST="$SPARK_APPS_DIR/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar"

mkdir -p "$SPARK_APPS_DIR"
echo "🚚 Copying application JAR to Spark's app directory..."
cp "$JAR_SRC" "$JAR_DEST"
if [ ! -f "$JAR_DEST" ]; then
    echo "❌ ERROR: JAR file not found after build. Aborting."
    exit 1
fi

# Start docker-compose from the correct directory
pushd ../docker >/dev/null
echo "🧹 Cleaning up previous run (if any)..."
docker-compose -f docker-compose.test.yml down -v --remove-orphans
echo "🚀 Starting Docker services in detached mode..."
docker-compose -f docker-compose.test.yml up -d
popd >/dev/null

echo "⏳ Waiting for services to initialize..."
sleep 20

# Ensure the JAR is present inside Spark containers (bind mounts can occasionally misbehave on some hosts).
echo "📤 Copying application JAR into Spark containers..."
docker cp "$JAR_DEST" spark-master:/opt/spark_apps/
docker cp "$JAR_DEST" spark-worker:/opt/spark_apps/ || true

# Prepare PostgreSQL table
echo "🗄️ Waiting for PostgreSQL to be ready..."
until docker exec postgres pg_isready -U airflow -d airflow >/dev/null 2>&1; do
  printf '.'
  sleep 2
done
echo " Postgres is ready. Creating aggregated_campaign_stats table if absent..."
docker exec -i postgres psql -U airflow -d airflow <../q1_realtime_stream_processing/ddl/postgres_aggregated_campaign_stats.sql

# Prepare Kafka topic
echo "📻 Creating topic 'ad-events' if it does not exist..."
docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server kafka:9093 --replication-factor 1 --partitions 1 \
  --topic ad-events

# Launch Python producer in the background
echo "🐍 Starting Python producer to stream live data..."
pip3 install --quiet --upgrade pip wheel six kafka-python==2.0.2 >/dev/null 2>&1 || true

# If script receives "--onepass" as its first arg, run producer without looping.
LOOP_FLAG=""
if [[ "$1" != "--onepass" ]]; then
  LOOP_FLAG="--loop"
fi

python3 ../scripts/ad_events_producer.py --broker localhost:9092 --file ../data/CriteoSearchData $LOOP_FLAG &
PRODUCER_PID=$!
sleep 5 # Give producer a moment to connect

# Submit Spark job
echo "🔥 Submitting Spark job to master in the background..."
echo "------------------------------------------------------"
(
  docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
    --class com.tabularasa.bi.q1_realtime_stream_processing.spark.AdEventSparkStreamer \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf "spark.driver.extraJavaOptions=-Duser.home=/tmp" \
    --conf "spark.executor.extraJavaOptions=-Duser.home=/tmp" \
    --conf "spark.streaming.stopGracefullyOnShutdown=true" \
    --conf "spark.streaming.kafka.consumer.poll.ms=1000" \
    --packages org.postgresql:postgresql:42.7.3 \
    /opt/spark_apps/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar \
    "kafka:9093" "ad-events" "jdbc:postgresql://postgres:5432/airflow" "airflow" "airflow"
) &
SPARK_SUBMIT_PID=$!

# Let the pipeline run for a bit to process data
echo "⏳ Waiting 60 seconds for the pipeline to process data..."
sleep 60

# Check if processes are still running
if ! ps -p "$PRODUCER_PID" > /dev/null; then
  echo "❌ ERROR: Producer process has terminated prematurely."
  exit 1
fi
if ! ps -p "$SPARK_SUBMIT_PID" > /dev/null; then
  echo "❌ ERROR: Spark submit process has terminated prematurely."
  exit 1
fi

# Check for data in the table
echo "🔍 Checking if data was properly processed..."
RECORD_COUNT=$(docker exec postgres psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM aggregated_campaign_stats;" | xargs)
echo "📊 Found $RECORD_COUNT records in aggregated_campaign_stats table."

if [[ "$RECORD_COUNT" -gt 10 ]]; then
  echo "✅ Test successful: Data is being processed and saved to PostgreSQL."
  echo "🎉 E2E test passed. Press Ctrl+C to stop and clean up."
  # Keep script running to allow for manual inspection.
  wait $SPARK_SUBMIT_PID
else
  echo "❌ TEST FAILED: Not enough records found in the database."
  exit 1
fi

# The script will only reach here if spark-submit finishes or fails.
# The trap will handle cleanup in all cases.
echo "🎉 Spark job finished or was interrupted. Exiting."
exit 0 