FROM python:3.9-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV ENVIRONMENT=dev

# Create directories for logs and reports
RUN mkdir -p /var/log/instrument-agent /app/reports

# Default command - can be overridden in Kubernetes deployment
CMD ["python", "-m", "src.market_data.agent.instrument_data_agent", "daily_update", "--environment", "dev"]
