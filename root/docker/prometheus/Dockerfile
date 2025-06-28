FROM prom/prometheus:v2.47.0

# Add configuration files
COPY prometheus.yml /etc/prometheus/
COPY alert.rules.yml /etc/prometheus/

# Set non-root user
USER nobody

# Add metadata labels
LABEL maintainer="TabulaRasa BI Team" \
      description="Prometheus monitoring server for TabulaRasa BI Core" \
      version="2.47.0"

# Set healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --spider -q http://localhost:9090/-/healthy || exit 1 