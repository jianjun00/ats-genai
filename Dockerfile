# Use Python 3.12 slim as the base image
FROM python:3.12.2-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONPATH="/app/src" \
    SKIP_DB_SETUP="true"

# Set working directory
WORKDIR /app

# Copy only requirements first to leverage Docker cache
COPY pyproject.toml .

# Install minimal dependencies in a single layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip install "uvicorn[standard]>=0.23.0" \
                  "fastapi>=0.103.1" \
                  "asyncpg>=0.28.0" \
                  "psycopg2-binary>=2.9.9" \
    && pip install -e .

# Copy application code after installing dependencies
COPY . /app/

# Clear any Python cache files to ensure fresh code is used
RUN find /app -name "__pycache__" -type d -exec rm -rf {} +; exit 0 && \
    find /app -name "*.pyc" -delete

# Create non-root user and set permissions
RUN groupadd --gid 1000 ats && \
    useradd --uid 1000 --gid ats --shell /bin/bash --create-home ats && \
    chown -R ats:ats /app

# Switch to non-root user
USER ats

# Expose the port the app runs on
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Command to run the application
CMD ["sh", "-c", "if [ \"$SKIP_DB_SETUP\" != \"true\" ]; then python src/db/setup_trading_db.py; fi && exec uvicorn src.main:app --host 0.0.0.0 --port 8080"]
