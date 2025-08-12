# Use Python 3.12 slim as the base image with explicit tag
FROM python:3.12.2-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH="/app/src" \
    SKIP_DB_SETUP="true"

# Set working directory
WORKDIR /app

# Install system dependencies
RUN echo "Installing system dependencies..." && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy only necessary files
COPY pyproject.toml .
COPY config/ config/
COPY src/ src/

# Install Python dependencies
RUN echo "Installing Python dependencies..." && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir "uvicorn[standard]" fastapi "psycopg2-binary>=2.9.9" && \
    pip install --no-cache-dir -e . && \
    echo "Python version: $(python --version)" && \
    echo "pip version: $(pip --version)" && \
    echo "Installed packages:" && \
    pip freeze

# Create non-root user
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

# Print installed packages for debugging
RUN pip freeze

# Command to run the application
# Run the application with environment variable check
CMD ["sh", "-c", "if [ \"$SKIP_DB_SETUP\" != \"true\" ]; then python src/db/setup_trading_db.py; fi && exec uvicorn src.main:app --host 0.0.0.0 --port 8080"]
