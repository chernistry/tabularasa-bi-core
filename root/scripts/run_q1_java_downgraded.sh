#!/bin/bash

# Navigate to project root
cd "$(dirname "$0")/.."

# Ensure target directory exists
mkdir -p q1_realtime_stream_processing/target

# Database setup if needed
if [ "$1" == "--db-setup" ]; then
    echo "Setting up PostgreSQL table..."
    docker exec -i postgres psql -U postgres -d tabularasa < q1_realtime_stream_processing/ddl/postgres_aggregated_campaign_stats.sql
fi

# Build the project with Maven if JAR doesn't exist or --build flag is present
if [ ! -f q1_realtime_stream_processing/target/q1_realtime_stream_processing-0.0.1-SNAPSHOT-spring-boot.jar ] || [ "$1" == "--build" ] || [ "$2" == "--build" ]; then
    echo "Building the project..."
    mvn clean package -DskipTests
fi

# Set environment variable to allow SecurityManager in Java 17+
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
-Djava.security.manager=allow \
-Dhadoop.home.dir=/"

# Run the Spring Boot application with external config
echo "Running Q1 application with compatibility flags for Java 17+..."
java -jar q1_realtime_stream_processing/target/q1_realtime_stream_processing-0.0.1-SNAPSHOT-spring-boot.jar 