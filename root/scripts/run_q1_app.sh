#!/bin/bash

# Navigate to project root
d=$(dirname "$0")/..
cd "$d"

# Recommend running tests before starting the app
echo "[INFO] Running all tests (including property-based and edge-case coverage)..."
mvn test
if [ $? -ne 0 ]; then
  echo "[ERROR] Tests failed. Please fix test failures before running the application."
  exit 1
fi

# Определяем правильное имя контейнера PostgreSQL
echo "[INFO] Determining PostgreSQL container name..."
POSTGRES_CONTAINER="postgres"
if docker ps --format '{{.Names}}' | grep -q "tabularasa_postgres_db"; then
  POSTGRES_CONTAINER="tabularasa_postgres_db"
fi
echo "[INFO] Found PostgreSQL container: $POSTGRES_CONTAINER"

# Create the necessary table in PostgreSQL
echo "[INFO] Setting up PostgreSQL table..."
docker exec -i $POSTGRES_CONTAINER psql -U tabulauser -d tabularasadb < q1_realtime_stream_processing/ddl/postgres_aggregated_campaign_stats.sql

# Build the project with Maven
echo "[INFO] Building the project..."
mvn clean package -DskipTests

# Run the Spring Boot application with external config
echo "[INFO] Running Q1 application..."
java --add-opens=java.base/java.lang=ALL-UNNAMED \
     --add-opens=java.base/java.util=ALL-UNNAMED \
     --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
     --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
     -Djava.security.manager=allow \
     -Dhadoop.home.dir=/ \
     -DHADOOP_USER_NAME=`whoami` \
     -jar q1_realtime_stream_processing/target/q1_realtime_stream_processing-0.0.1-SNAPSHOT-spring-boot.jar \
     --spring.config.location=file:q1_realtime_stream_processing/src/main/resources/application.properties 