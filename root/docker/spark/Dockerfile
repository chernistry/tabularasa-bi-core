# Use the official Bitnami Spark image as a base
FROM docker.io/bitnami/spark:3.5

# Switch to the root user to modify permissions and copy files
USER root

# Copy custom configuration files into the Spark conf directory
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY spark-env.sh /opt/bitnami/spark/conf/spark-env.sh
COPY docker-entrypoint.sh /opt/bitnami/scripts/docker-entrypoint.sh

# Make the scripts executable
RUN chmod +x /opt/bitnami/spark/conf/spark-env.sh && \
    chmod +x /opt/bitnami/scripts/docker-entrypoint.sh

# Create the directory for the application JAR
RUN mkdir -p /opt/spark_apps && chown -R 1001:0 /opt/spark_apps

# Switch back to the non-root user for better security
USER 1001

# Set the custom entrypoint
ENTRYPOINT ["/opt/bitnami/scripts/docker-entrypoint.sh"]
CMD ["nami", "start", "--foreground", "spark"] 