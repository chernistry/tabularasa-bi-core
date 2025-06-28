FROM maven:3.9-eclipse-temurin-17-alpine AS builder

WORKDIR /build

# Copy only the POM files first to leverage Docker cache
COPY ./root/pom.xml ./pom.xml
COPY ./root/q1_realtime_stream_processing/pom.xml ./q1_realtime_stream_processing/pom.xml

# Download dependencies
RUN mvn -f ./q1_realtime_stream_processing/pom.xml dependency:go-offline -B

# Copy source code
COPY ./root/q1_realtime_stream_processing/src ./q1_realtime_stream_processing/src

# Build the application
RUN mvn -f ./q1_realtime_stream_processing/pom.xml clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre-alpine

# Create a non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy the JAR file from the builder stage
COPY --from=builder /build/q1_realtime_stream_processing/target/q1_realtime_stream_processing-*.jar /app/app.jar

# Set ownership to non-root user
RUN chown -R appuser:appgroup /app

# Switch to non-root user
USER appuser

# Set healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD wget -q -O- http://localhost:8083/actuator/health | grep -q '"status":"UP"' || exit 1

# Run the application
ENTRYPOINT ["java", "-jar", "/app/app.jar"] 