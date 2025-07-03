#!/bin/bash
set -e

# This script is a wrapper for the original entrypoint.
# It waits for the application JAR to be available before starting Spark.

APP_JAR_PATH="/opt/spark_apps/q1_realtime_stream_processing-0.0.1-SNAPSHOT.jar"

# Loop until the JAR file exists
while [ ! -f "$APP_JAR_PATH" ]; do
  echo "Waiting for application JAR to be built..."
  sleep 5
done

echo "Application JAR found. Starting Spark..."

# Execute the original entrypoint
exec /opt/bitnami/scripts/spark/entrypoint.sh "$@" 