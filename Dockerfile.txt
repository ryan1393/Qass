# Stable Python version (pylogix compatible)
FROM python:3.11-slim

ENV HEALTH_PORT=8081

# Prevent Python output buffering
ENV PYTHONUNBUFFERED=1

# Create app directory
WORKDIR /app

# Install minimal OS tools (debug + healthcheck support)
RUN apt-get update && apt-get install -y \
    iputils-ping \
    curl \
 && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY worker_async.py .
COPY state_store.py .
COPY qass_normalizer.py .
COPY metrics.py .
COPY paths.py .

# Copy configs into image
COPY configs/ configs/

# Ensure directories exist at runtime
RUN mkdir -p /app/logs /app/state

# Default command (config passed by docker-compose)
CMD ["python3", "main.py"]

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD curl -sf http://localhost:${HEALTH_PORT}/health || exit 1
