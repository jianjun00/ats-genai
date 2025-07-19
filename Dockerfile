FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Entrypoint runs DB setup/migrations, then launches API
CMD ["sh", "-c", "python src/db/setup_trading_db.py && uvicorn src.main:app --host 0.0.0.0 --port 8080"]
