# Use Python 3.12 slim as the base image
FROM python:3.12.2-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PYTHONPATH="/app/src:/workspace/src" \
    SKIP_DB_SETUP="true"

# Set working directory
WORKDIR /app

# Copy requirements files first to leverage Docker cache
COPY pyproject.toml requirements.txt ./

# Install system dependencies and Python packages in a single layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        python3-dev \
        curl \
        git \
        build-essential \
        # Playwright browser dependencies
        libnss3-dev \
        libatk-bridge2.0-dev \
        libdrm2 \
        libxkbcommon0 \
        libgtk-3-dev \
        libgbm-dev \
        libasound2-dev \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir \
        requests \
        ray \
        yfinance \
        beautifulsoup4 \
        requests-html \
        transformers \
        torch \
        torchvision \
        nltk \
        textblob \
        plotly \
        seaborn \
        matplotlib \
        streamlit \
        jupyter \
        ipython \
        feedparser \
        playwright \
    && pip install --no-cache-dir -e . \
    && playwright install chromium firefox webkit \
    && playwright install-deps

# Copy application code after installing dependencies
COPY . /app/

# Clear any Python cache files to ensure fresh code is used
RUN find /app -name "__pycache__" -type d -exec rm -rf {} +; exit 0 && \
    find /app -name "*.pyc" -delete

# Create non-root user and set permissions
RUN groupadd --gid 1000 ats && \
    useradd --uid 1000 --gid ats --shell /bin/bash --create-home ats && \
    chown -R ats:ats /app && \
    # Make Playwright browsers accessible to ats user
    chmod -R 755 /root/.cache/ms-playwright || true

# Switch to non-root user
USER ats

# Set Playwright browser path for non-root user
ENV PLAYWRIGHT_BROWSERS_PATH=/root/.cache/ms-playwright

# Expose the port the app runs on
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Command to run the application
CMD ["sh", "-c", "if [ \"$SKIP_DB_SETUP\" != \"true\" ]; then python src/db/setup_trading_db.py; fi && exec uvicorn src.main:app --host 0.0.0.0 --port 8080"]
