FROM python:3.12-slim

LABEL maintainer="log-aggregator"
LABEL description="Log Aggregator and Indexer — single-node Python implementation"

WORKDIR /app
COPY logagg ./logagg

# Data directory (mount a volume here for persistence)
RUN mkdir -p /app/data
VOLUME /app/data

EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

# Production: bind 0.0.0.0, use PORT env var, seed with demo data
CMD ["python", "-m", "logagg", "--host", "0.0.0.0", "--data-dir", "/app/data"]
