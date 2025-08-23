# Multi-stage Dockerfile for ATS Analytics Service
FROM python:3.11-slim-bullseye as builder

# Build arguments
ARG SERVICE_NAME=analytics-service
ARG BUILD_DATE
ARG VCS_REF

# Labels for image metadata
LABEL org.opencontainers.image.title="ATS Analytics Service"
LABEL org.opencontainers.image.description="Unified analytics platform with portfolio analytics and real-time monitoring"
LABEL org.opencontainers.image.created=$BUILD_DATE
LABEL org.opencontainers.image.revision=$VCS_REF
LABEL org.opencontainers.image.vendor="ATS Team"
LABEL org.opencontainers.image.source="https://github.com/your-org/ats-genai"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r ats && useradd -r -g ats -s /bin/false ats

# Set working directory
WORKDIR /app

# Copy requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --user --no-warn-script-location -r requirements.txt

# Production stage
FROM python:3.11-slim-bullseye as production

# Copy user from builder
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    PATH="/home/ats/.local/bin:$PATH"

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Set working directory
WORKDIR /app

# Copy Python packages from builder
COPY --from=builder --chown=ats:ats /root/.local /home/ats/.local

# Copy application code
COPY --chown=ats:ats src/ src/
COPY --chown=ats:ats services/analytics-service/ services/analytics-service/
COPY --chown=ats:ats config/ config/

# Create logs directory
RUN mkdir -p /app/logs && chown -R ats:ats /app/logs

# Switch to non-root user
USER ats

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Expose port
EXPOSE 8080

# Set the entry point
ENTRYPOINT ["python", "services/analytics-service/unified_analytics_app.py"]