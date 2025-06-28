#!/bin/bash
# Script to ensure Spark versions are synchronized between local environment and containers
# This helps prevent serialization issues due to version mismatch

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")/.."

# Get Spark version from pom.xml
SPARK_VERSION=$(grep '<spark.version>' "$ROOT_DIR/root/pom.xml" | sed 's/.*>\(.*\)<.*/\1/')
echo "Detected Spark version: $SPARK_VERSION"

# Check if Docker is running
if ! docker ps &>/dev/null; then
  echo "Error: Docker is not running or not accessible"
  exit 1
fi

# Get list of running Spark containers
SPARK_CONTAINERS=$(docker ps --format '{{.Names}}' | grep -E 'spark-master|spark-worker')

if [ -z "$SPARK_CONTAINERS" ]; then
  echo "No Spark containers found running"
  exit 0
fi

echo "Found Spark containers: $SPARK_CONTAINERS"

# For each container, verify Spark version and fix if needed
for CONTAINER in $SPARK_CONTAINERS; do
  echo "Checking Spark version in container: $CONTAINER"
  
  # Get container Spark version
  CONTAINER_SPARK_VERSION=$(docker exec "$CONTAINER" bash -c 'if [ -f "$SPARK_HOME/VERSION" ]; then cat "$SPARK_HOME/VERSION"; else ls -d /opt/spark-* 2>/dev/null | grep -o "[0-9]\+\.[0-9]\+\.[0-9]\+" || echo "unknown"; fi')
  
  echo "Container $CONTAINER has Spark version: $CONTAINER_SPARK_VERSION"
  
  # If version mismatch, print warning
  if [ "$CONTAINER_SPARK_VERSION" != "$SPARK_VERSION" ]; then
    echo "WARNING: Version mismatch between project ($SPARK_VERSION) and container $CONTAINER ($CONTAINER_SPARK_VERSION)"
    echo "This may cause serialization issues. Consider rebuilding containers with matching versions."
  fi
  
  # Determine Spark configuration directory
  SPARK_CONF_DIR=$(docker exec "$CONTAINER" bash -c 'if [ -d "/opt/spark/conf" ]; then echo "/opt/spark/conf"; elif [ -d "/opt/bitnami/spark/conf" ]; then echo "/opt/bitnami/spark/conf"; elif [ -d "$SPARK_HOME/conf" ]; then echo "$SPARK_HOME/conf"; else echo ""; fi')
  
  if [ -z "$SPARK_CONF_DIR" ]; then
    # Try to find spark-defaults.conf file
    SPARK_DEFAULTS_CONF=$(docker exec "$CONTAINER" bash -c 'find / -name spark-defaults.conf 2>/dev/null | head -1')
    if [ -n "$SPARK_DEFAULTS_CONF" ]; then
      SPARK_CONF_DIR=$(dirname "$SPARK_DEFAULTS_CONF")
    else
      # Create a new conf directory if none exists
      echo "No Spark configuration directory found in $CONTAINER, creating one"
      docker exec "$CONTAINER" bash -c 'mkdir -p /opt/spark/conf'
      SPARK_CONF_DIR="/opt/spark/conf"
    fi
  fi
  
  # Find spark-env.sh file or path
  SPARK_ENV_SH=$(docker exec "$CONTAINER" bash -c "find / -name spark-env.sh 2>/dev/null | head -1")
  if [ -z "$SPARK_ENV_SH" ]; then
    SPARK_ENV_SH="$SPARK_CONF_DIR/spark-env.sh"
    # Create spark-env.sh if it doesn't exist
    docker exec "$CONTAINER" bash -c "touch $SPARK_ENV_SH && chmod +x $SPARK_ENV_SH"
  fi
  
  echo "Using Spark configuration directory: $SPARK_CONF_DIR"
  
  # Fix serialization settings in container
  echo "Updating serialization settings in $CONTAINER..."
  docker exec "$CONTAINER" bash -c "echo 'spark.serializer org.apache.spark.serializer.KryoSerializer' >> $SPARK_CONF_DIR/spark-defaults.conf"
  docker exec "$CONTAINER" bash -c "echo 'spark.kryo.registrationRequired false' >> $SPARK_CONF_DIR/spark-defaults.conf"
  docker exec "$CONTAINER" bash -c "echo 'spark.serializer.objectStreamReset 100' >> $SPARK_CONF_DIR/spark-defaults.conf"
  
  # Set system properties to disable serialVersionUID validation
  docker exec "$CONTAINER" bash -c "echo 'export SPARK_JAVA_OPTS=\"\$SPARK_JAVA_OPTS -Dsun.io.serialization.extendedDebugInfo=true -Djava.io.serialization.strict=false -Djdk.serialFilter=allow -Dsun.serialization.validateClassSerialVersionUID=false\"' >> $SPARK_ENV_SH"
done

echo "Synchronization complete" 