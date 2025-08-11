# Multi-stage build for smaller production image
FROM python:3.11-slim as builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set environment variables
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Create and activate virtual environment
WORKDIR /app
RUN /bin/uv venv /opt/venv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies (including uvicorn and all project dependencies)
RUN /bin/uv sync --frozen --no-dev
# Explicitly install uvicorn using uv to ensure it's available
RUN /bin/uv add uvicorn --no-sync

# Production stage
FROM python:3.11-slim

# Copy virtual environment from builder stage
COPY --from=builder /opt/venv /opt/venv

# Make sure we use venv
ENV PATH="/opt/venv/bin:$PATH"

# Create non-root user for security
RUN groupadd --gid 1000 ats && \
    useradd --uid 1000 --gid ats --shell /bin/bash --create-home ats

# Set working directory
WORKDIR /app

# Copy application code
COPY --chown=ats:ats . .

# Switch to non-root user
USER ats

# Health check endpoint
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/health', timeout=10)"

# Entrypoint runs DB setup/migrations, then launches API
CMD ["sh", "-c", "python src/db/setup_trading_db.py && uvicorn src.main:app --host 0.0.0.0 --port 8080"]
