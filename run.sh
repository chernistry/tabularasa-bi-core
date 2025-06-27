#!/usr/bin/env bash
# ==============================================================================
# UNIFIED RUNNER FOR TABULARASA BI CORE
# ==============================================================================
# This script provides a single entry point for building, running, testing,
# and managing the application stack.
#
# Commands:
#   prod [simple|spark] [--skiptests] - Launch the main pipeline locally.
#   test [--onepass]                  - Execute the full end-to-end test.
#   kill                              - Terminate all demo-related processes.
#   down                              - Stop and remove Docker containers and volumes.
#   dash                              - Start the FastAPI dashboard backend.
#   fix-hadoop                        - Manually run the Hadoop user fix for Spark containers.
#
# ------------------------------------------------------------------------------
set -e
cd "$(dirname "$0")"

# --- GLOBAL VARIABLES ---
ROOT_DIR=$(pwd)
SCRIPTS_DIR="$ROOT_DIR/root/scripts"
DOCKER_DIR="$ROOT_DIR/root/docker"
Q1_DIR="$ROOT_DIR/root/q1_realtime_stream_processing"

# Fallback Docker config without credential helpers if helper binary is missing
if ! command -v docker-credential-desktop >/dev/null 2>&1; then
  export DOCKER_CONFIG="$ROOT_DIR/.docker_nocreds"
  mkdir -p "$DOCKER_CONFIG"
  echo '{}' > "$DOCKER_CONFIG/config.json"
fi

# --- UTILITY FUNCTIONS ---

# Print usage information
function usage() {
  cat <<EOF
Usage: ./run.sh <command> [options]

Commands:
  prod [simple|spark] [--skiptests]
                    Launch the main pipeline. Defaults to 'simple' profile.
                    --skiptests: Skip Maven tests before starting.
  test [--onepass]
                    Execute the full end-to-end test harness.
                    --onepass: Run the data producer only once instead of looping.
  kill              Terminate demo-related Java, Python, and Spark processes.
  down              Same as 'kill' plus 'docker-compose down -v'.
  dash              Start FastAPI dashboard backend (http://localhost:8000).
  fix-hadoop        Manually run the Hadoop/Spark user permission fix.
  help              Show this help message.
EOF
}

# Terminate running application processes
function kill_processes() {
  echo "🛑 [INFO] Killing demo-related processes…"
  pkill -f "q1_realtime_stream_processing.*.jar" 2>/dev/null || true
  pkill -f "AdEventSparkStreamer" 2>/dev/null || true
  pkill -f "ad_events_producer.py" 2>/dev/null || true
  pkill -f "uvicorn main:app" 2>/dev/null || true
  echo "✅ [INFO] Processes terminated."
}

# Stop and remove all docker-compose services and volumes
function docker_down() {
  echo "🧹 [INFO] Shutting down and removing Docker stack…"
  kill_processes
  if [ -d "$DOCKER_DIR" ]; then
    (cd "$DOCKER_DIR" && docker-compose -f docker-compose.yml down -v --remove-orphans >/dev/null 2>&1)
    (cd "$DOCKER_DIR" && docker-compose -f docker-compose.test.yml down -v --remove-orphans >/dev/null 2>&1)
  fi
  echo "✅ [INFO] Docker stack is down."
}

# Fix user permissions inside Spark containers to prevent Hadoop/Kerberos errors
function fix_hadoop_user() {
  if ! command -v docker &> /dev/null || ! docker ps &> /dev/null; then
    echo "⚠️ [WARNING] Docker is not available or not running. Skipping Hadoop setup."
    return
  fi

  echo "🐳 [INFO] Fixing Hadoop user configuration in Spark containers..."
  local containers=("spark-master" "spark-worker")
  for container in "${containers[@]}"; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
      echo "🔧 [HADOOP] Configuring container: $container"
      docker exec -u root "$container" bash -c '
        export HADOOP_USER_NAME=root
        mkdir -p /tmp/.sparkStaging /tmp/spark-events /tmp/spark_checkpoints /tmp/.ivy2/local /home/hadoop
        chmod -R 777 /tmp/.sparkStaging /tmp/spark-events /tmp/spark_checkpoints /tmp/.ivy2 /home/hadoop
        if ! grep -q "hadoop:x" /etc/passwd; then
          echo "hadoop:x:8000:8000:hadoop:/home/hadoop:/bin/bash" >> /etc/passwd
        fi
        getent passwd $(id -u) || echo "spark:x:$(id -u):$(id -g):Spark User:/tmp:/bin/bash" >> /etc/passwd
        echo "✅ [HADOOP] User setup complete in $0"
      '
    fi
  done
}

# --- MAIN WORKFLOWS ---

# Run application locally, connecting to services in Docker
function run_prod() {
  local profile="simple"
  local skip_tests=false
  for arg in "$@"; do
    case $arg in
      simple|spark)
        profile=$arg
        ;;
      --skiptests)
        skip_tests=true
        ;;
    esac
  done

  echo "🚀 [PROD] Starting 'prod' mode with profile: '$profile'"

  echo "🐳 [DOCKER] Starting Docker services for prod..."
  (cd "$DOCKER_DIR" && docker-compose -f docker-compose.yml up -d tabularasa_postgres_db kafka spark-master spark-worker)

  echo "⏳ [WAIT] Waiting for services to initialize..."

  # Fix Hadoop user before starting, especially for spark profile
  if [[ "$profile" == "spark" ]]; then
    fix_hadoop_user
  fi

  # Run tests unless skipped
  if [ "$skip_tests" = false ]; then
    echo "🧪 [INFO] Running all tests..."
    (cd "$Q1_DIR" && mvn test) || {
      echo "⚠️ [WARNING] Tests failed, but continuing with application startup..."
    }
  else
    echo "⏩ [INFO] Skipping tests as requested."
  fi

  # Wait for PostgreSQL and setup database schema
  echo "🐘 [INFO] Waiting for PostgreSQL container..."
  until docker-compose -f "$DOCKER_DIR/docker-compose.yml" exec -T tabularasa_postgres_db pg_isready -U tabulauser -d tabularasadb >/dev/null 2>&1; do
      printf '.' && sleep 2
  done
  echo "✅ [POSTGRES] DB is ready. Setting up schema..."
  docker-compose -f "$DOCKER_DIR/docker-compose.yml" exec -T tabularasa_postgres_db psql -U tabulauser -d tabularasadb < "$Q1_DIR/ddl/postgres_aggregated_campaign_stats.sql"

  # Wait for Kafka and create topic
  echo "📻 [KAFKA] Waiting for Kafka..."
  until docker-compose -f "$DOCKER_DIR/docker-compose.yml" exec -T kafka kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
      printf '.' && sleep 3
  done
  echo "✅ [KAFKA] Kafka is ready. Creating topic 'ad-events'..."
  docker-compose -f "$DOCKER_DIR/docker-compose.yml" exec -T kafka kafka-topics.sh --create --if-not-exists --topic ad-events \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1

  # Build the application JAR
  echo "🔨 [INFO] Building the project (skipping tests)..."
  (cd "$Q1_DIR" && mvn clean package -DskipTests)

  # Copy JAR to Spark apps directory for cluster submissions
  if [[ "$profile" == "spark" ]]; then
    local spark_apps_dir="$DOCKER_DIR/spark_apps"
    mkdir -p "$spark_apps_dir"
    cp -f "$Q1_DIR/target/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar" "$spark_apps_dir/" || true
  fi

  # Prefer Java 17 runtime – Spark 3.5 is only certified up to Java 17. Newer JVMs (e.g. 23) cause
  # deserialization errors like "unread block data" seen in logs. We attempt to locate a JDK 17 binary
  # via /usr/libexec/java_home (macOS) or fallback to `java` on PATH.
  local java_cmd
  if command -v /usr/libexec/java_home >/dev/null 2>&1; then
    java_cmd="$(/usr/libexec/java_home -v 17 2>/dev/null)/bin/java"
  fi
  [[ -x "$java_cmd" ]] || java_cmd=$(command -v java)

  # Run the Spring Boot application
  echo "🚀 [PROD] Running Q1 application with profile '$profile'..."
  local spark_master_url="local[*]"
  if [[ "$profile" == "spark" ]]; then
      spark_master_url="spark://localhost:7077"
  fi
  local hadoop_user
  hadoop_user=$(whoami)
  
  "$java_cmd" --add-opens=java.base/java.lang=ALL-UNNAMED \
       --add-opens=java.base/java.util=ALL-UNNAMED \
       --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
       --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
       --add-opens=java.base/java.nio=ALL-UNNAMED \
       --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
       -Dspring.profiles.active="$profile" \
       -Dspark.master="$spark_master_url" \
       -Dspring.datasource.url=jdbc:postgresql://localhost:5432/tabularasadb \
       -Dspring.kafka.bootstrap-servers=localhost:19092 \
       -Dapp.kafka.bootstrap-servers=localhost:19092 \
       -Dhadoop.security.authentication=simple \
       -Dorg.apache.hadoop.security.authentication=simple \
       -Dspark.hadoop.hadoop.security.authentication=simple \
       -Djavax.security.auth.useSubjectCredsOnly=false \
       -Djava.security.auth.login.config=/dev/null \
       -Djava.security.krb5.conf=/dev/null \
       -Djava.security.manager=allow \
       -DHADOOP_USER_NAME="$hadoop_user" \
       -jar "$Q1_DIR/target/q1_realtime_stream_processing-0.0.1-SNAPSHOT-exec.jar"
}

# Run the full end-to-end test suite
function run_test() {
  trap cleanup INT TERM
  
  echo "🚀 [E2E] Starting End-to-End test..."
  
  # Build Spark application
  echo "📦 [SPARK] Building Spark application..."
  (cd "$Q1_DIR" && mvn clean package -DskipTests >/dev/null)

  local spark_apps_dir="$DOCKER_DIR/spark_apps"
  local jar_src="$Q1_DIR/target/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar"
  local jar_dest="$spark_apps_dir/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar"

  mkdir -p "$spark_apps_dir"
  echo "📦 [SPARK] Copying application JAR to Spark's app directory..."
  cp "$jar_src" "$jar_dest"

  # Start docker-compose test environment
  (cd "$DOCKER_DIR" && \
   echo "🧹 [DOCKER] Cleaning up previous run..." && \
   docker-compose -f docker-compose.test.yml down -v --remove-orphans >/dev/null 2>&1 && \
   echo "🐳 [DOCKER] Starting Docker services for test..." && \
   docker-compose -f docker-compose.test.yml up -d)

  echo "⏳ [WAIT] Waiting for services to initialize (20s)..."
  sleep 20

  fix_hadoop_user

  # Prepare PostgreSQL
  echo "🗄️ [POSTGRES] Waiting for PostgreSQL..."
  until docker exec postgres pg_isready -U tabulauser -d tabularasadb >/dev/null 2>&1; do
    printf '.' && sleep 2
  done
  echo "✅ [POSTGRES] DB is ready. Setting up schema..."
  docker exec -i postgres psql -U tabulauser -d tabularasadb < "$Q1_DIR/ddl/postgres_aggregated_campaign_stats.sql"

  # Prepare Kafka
  echo "📻 [KAFKA] Waiting for Kafka..."
  until docker exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
      printf '.' && sleep 3
  done
  echo "✅ [KAFKA] Kafka is ready. Creating topic 'ad-events'..."
  docker exec kafka kafka-topics.sh --create --if-not-exists --topic ad-events \
    --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1

  # Start data producer
  local loop_flag="--loop"
  if [[ "$1" == "--onepass" ]]; then
    loop_flag=""
  fi
  echo "🐍 [PRODUCER] Starting Python producer..."
  docker rm -f ad_events_producer >/dev/null 2>&1 || true
  local producer_container_id
  producer_container_id=$(docker run -d --name ad_events_producer --network docker_bi_network \
    -v "$ROOT_DIR/root/data":/data \
    -v "$SCRIPTS_DIR":/scripts \
    python:3.10-slim bash -c "pip install -q --no-cache-dir kafka-python==2.0.2 && python /scripts/ad_events_producer.py --broker kafka:9092 --file /data/CriteoSearchData $loop_flag")
  
  sleep 5 # Give producer time to start

  # Submit Spark job
  echo "🔥 [SPARK] Submitting Spark job to master..."
  (
    docker exec -e HADOOP_USER_NAME=root spark-master /opt/bitnami/spark/bin/spark-submit \
      --class com.tabularasa.bi.q1_realtime_stream_processing.spark.AdEventSparkStreamer \
      --master spark://spark-master:7077 \
      --deploy-mode client \
      --packages org.postgresql:postgresql:42.7.3 \
      /opt/spark_apps/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar \
      "kafka:9092" "ad-events" "jdbc:postgresql://postgres:5432/tabularasadb" "tabulauser" "tabulapass"
  ) &
  local spark_submit_pid=$!

  # Monitor pipeline
  echo "⏳ [WAIT] Waiting 60 seconds for the pipeline to process data..."
  sleep 60

  # Verify results
  echo "🔍 [POSTGRES] Checking for processed data..."
  local record_count
  record_count=$(docker exec postgres psql -U tabulauser -d tabularasadb -t -c "SELECT COUNT(*) FROM aggregated_campaign_stats;" | xargs)
  echo "📊 [POSTGRES] Found $record_count records in aggregated_campaign_stats table."

  if [[ "$record_count" -gt 0 ]]; then
    echo "✅ [E2E] Test successful: Data is being processed."
    echo "🎉 [E2E] Press Ctrl+C to stop and clean up."
    wait $spark_submit_pid
  else
    echo "❌ [E2E] TEST FAILED: No records found in the database."
    cleanup
    exit 1
  fi
}

function cleanup() {
  echo -e "\n\n🧹 [CLEANUP] Cleaning up test environment..."
  if [[ -n "${spark_submit_pid:-}" ]] && ps -p "$spark_submit_pid" > /dev/null; then
    kill "$spark_submit_pid" 2>/dev/null || true
  fi
  if [[ -n "${producer_container_id:-}" ]] && docker ps -q --no-trunc | grep -q "${producer_container_id}"; then
    docker rm -f "$producer_container_id" >/dev/null 2>&1 || true
  fi
  (cd "$DOCKER_DIR" && docker-compose -f docker-compose.test.yml down -v --remove-orphans >/dev/null 2>&1)
  echo "✅ [CLEANUP] Cleanup complete."
  exit 0
}

# Run FastAPI dashboard
function run_dash() {
  kill_processes
  echo "📊 [INFO] Launching FastAPI dashboard backend (http://localhost:8000)…"
  cd "$ROOT_DIR/root/dashboard_backend"
  exec uvicorn main:app --reload
}


# --- SCRIPT ENTRYPOINT ---

case "$1" in
  prod)
    run_prod "${@:2}"
    ;;
  test)
    run_test "${@:2}"
    ;;
  kill)
    kill_processes
    ;;
  down)
    docker_down
    ;;
  dash)
    run_dash
    ;;
  fix-hadoop)
    fix_hadoop_user
    ;;
  ""|help|-h|--help)
    usage
    ;;
  *)
    echo "❌ [ERROR] Unknown command: $1" >&2
    usage
    exit 1
    ;;
esac
