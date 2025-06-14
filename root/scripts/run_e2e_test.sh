#!/bin/bash

# Exit on error
set -e

# ==============================================================================
# DIAGNOSTIC AND E2E TEST SCRIPT
# ==============================================================================

# Ensure script is run from its own directory
dirname=$(dirname "$0")
cd "$dirname"

# Set the absolute project root path as an environment variable
# This makes volume mounts in docker-compose robust and independent of the execution directory.
export PROJECT_ROOT
PROJECT_ROOT=$(cd "$(dirname "$0")/../.." && pwd)

echo "---"
echo "Project Root determined as: ${PROJECT_ROOT}"
echo "---"

# --- DIAGNOSTIC STEP 1: VERIFY DOCKER VOLUME MOUNTING ---
echo "🩺 STEP 1: Running diagnostic to check Docker volume mounting..."

pushd ../docker > /dev/null
# Run the verification container. It will try to mount prometheus.yml and list it.
# If this fails, the issue is with Docker's file sharing permissions.
docker-compose -f docker-compose.verify.yml up > docker_verify_log.txt 2>&1
popd > /dev/null

# Check the log for the expected file.
if ! grep -q "prometheus.yml" ../docker/docker_verify_log.txt; then
  echo "❌ DIAGNOSTIC FAILED: Docker cannot access project files."
  echo "--------------------------------------------------------------------------"
  echo "  ROOT CAUSE: Docker Desktop does not have permission to access your"
  echo "  project directory: '${PROJECT_ROOT}'"
  echo ""
  echo "  TO FIX THIS ON MACOS:"
  echo "  1. Open Docker Desktop."
  echo "  2. Go to Settings (the gear icon)."
  echo "  3. Go to Resources -> FILE SHARING."
  echo "  4. Add your project's parent directory ('/Users/sasha/IdeaProjects') to the list."
  echo "  5. Click 'Apply & Restart'."
  echo "  6. After Docker restarts, run this script again."
  echo "--------------------------------------------------------------------------"
  # Clean up verification container
  pushd ../docker > /dev/null
  docker-compose -f docker-compose.verify.yml down -v --remove-orphans > /dev/null 2>&1
  rm docker_verify_log.txt
  popd > /dev/null
  exit 1
else
  echo "✅ Diagnostic PASSED. Docker volume mounting is working."
  # Clean up verification container
  pushd ../docker > /dev/null
  docker-compose -f docker-compose.verify.yml down -v --remove-orphans > /dev/null 2>&1
  rm docker_verify_log.txt
  popd > /dev/null
fi


# --- E2E TEST STEP 2: RUN THE FULL PIPELINE ---
echo ""
echo "🚀 STEP 2: Proceeding with the full End-to-End test..."

echo "🛠️ Preparing environment..."
# Ensure config files are readable (Prometheus & Alertmanager)
chmod -R a+r ../docker/prometheus || true
chmod -R a+r ../docker/alertmanager || true

# Build Spark application
echo "📦 Building Spark application..."
cd ../q1_realtime_stream_processing
mvn clean package -DskipTests > /dev/null
cd ../scripts

# Ensure spark_apps directory exists and copy the JAR
SPARK_APPS_DIR="../docker/spark_apps"
JAR_SRC="../q1_realtime_stream_processing/target/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar"
JAR_DEST="$SPARK_APPS_DIR/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar"

mkdir -p "$SPARK_APPS_DIR"
echo "🚚 Copying application JAR to Spark's app directory..."
cp "$JAR_SRC" "$JAR_DEST"

# Always start docker-compose from the correct directory
pushd ../docker > /dev/null
echo "🧹 Cleaning up previous run (if any)..."
docker-compose -f docker-compose.test.yml down -v --remove-orphans
echo "🚀 Starting Docker services..."
docker-compose -f docker-compose.test.yml up -d
popd > /dev/null

echo "⏳ Waiting for services to initialize..."
sleep 20

# Check Prometheus and Alertmanager container health
echo "🩺 Health checking monitoring stack..."
if ! docker ps | grep -q "prometheus"; then
  echo "❌ Prometheus container is not running. Logs:"
  docker logs prometheus || true
  exit 1
fi
if ! docker ps | grep -q "alertmanager"; then
  echo "❌ Alertmanager container is not running. Logs:"
  docker logs alertmanager || true
  exit 1
fi
echo "✅ Monitoring stack is running."

# STEP 4: Prepare Kafka (topic & sample data)
echo "📻 Creating topic 'ad-events' if it does not exist..."
docker exec kafka \
  /opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server kafka:9093 --replication-factor 1 --partitions 1 \
  --topic ad-events

# Launch Python producer in background
echo "🐍 Starting Python producer to stream live data..."
# Ensure Python dependencies are installed
pip3 uninstall -y --quiet kafka kafka-python > /dev/null 2>&1 || true
pip3 install --quiet --upgrade pip wheel six kafka-python==2.0.2 > /dev/null 2>&1 || true
python3 ../scripts/ad_events_producer.py --broker localhost:9092 --file ../data/CriteoSearchData &
PRODUCER_PID=$!

# STEP 5: Submit Spark job
echo "🔁 Submitting Spark job to master..."
docker exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --class com.tabularasa.bi.q1_realtime_stream_processing.spark.AdEventSparkStreamer \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf "spark.driver.extraJavaOptions=-Duser.home=/tmp" \
  --conf "spark.executor.extraJavaOptions=-Duser.home=/tmp" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.postgresql:postgresql:42.7.3 \
  /opt/spark_apps/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar \
  "kafka:9093" "ad-events" "jdbc:postgresql://postgres:5432/airflow" "airflow" "airflow"

# Give Spark some time to process
echo "⏳ Waiting for Spark processing (60 seconds)..."
sleep 60

echo "✅ Verifying results in PostgreSQL..."
docker exec postgres psql -U airflow -d airflow -c \
"SELECT campaign_id, SUM(event_count) as total_events FROM aggregated_campaign_stats GROUP BY campaign_id;"

echo "🎉 E2E Test Complete!"

# Stop Python producer
echo "🛑 Stopping Python producer..."
kill $PRODUCER_PID || true

echo "🧹 Cleaning up test environment..."
pushd ../docker > /dev/null
docker-compose -f docker-compose.test.yml down -v --remove-orphans
popd > /dev/null 