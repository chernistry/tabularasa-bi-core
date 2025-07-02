FROM prom/alertmanager:v0.26.0

# Add configuration file
COPY alertmanager.yml /etc/alertmanager/

# Set non-root user
USER nobody

# Add metadata labels
LABEL maintainer="TabulaRasa BI Team" \
      description="Alertmanager for TabulaRasa BI Core" \
      version="0.26.0"

# Set healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --spider -q http://localhost:9093/-/healthy || exit 1 