# Market Forecast App (ats-genai)

A server-side application for unified market data, event ingestion, signal computation, and deep learning-based forecasting. Modular, extensible, and production-ready.

## Features
- **Environment-Specific Configuration**: Support for test, integration, and production environments with automatic database/table prefixing
- Real-time and historical market data ingestion (Alpaca, Polygon.io, Finnhub, FMP, IEX, Yahoo, Quandl, Investing.com, etc)
- Unified event database with reconciliation from multiple sources
- Technical indicator computation (pandas_ta)
- Deep learning time series forecasting (PyTorch LSTM)
- REST API (FastAPI)
- Automated setup with Docker and .env
- Kubernetes-ready deployment for GCP
- Multi-duration interval support for trading analysis
- Database migration tools for environment setup

## Project Structure

```
src/
  db/                # Database setup, migrations, and utilities
  events/            # Event API, schemas, DB, and ingestion pipelines
    ingest/          # Source-specific ingestion modules
  market_data/       # Core market data logic
  pipeline/          # Signal extraction, orchestration, etc.
  state/             # Interval and state management (indicators, universe)
  trading/           # Trading logic, indicators, time durations
  universe/          # Universe management
  main.py            # FastAPI app entrypoint
```

## Environment Variables
- Copy `.env.example` to `.env` and fill in your API keys and DB URL.
- All secrets/keys are loaded via environment variables for security.

## Automated Setup (Recommended)

### Docker Compose

1. **Build and start the stack:**
    ```bash
    docker-compose up --build
    ```
2. **App and database will be ready on first run.**
3. **To run migrations or setup scripts:**
    - Use the provided entrypoint or run scripts inside the app container.

### Manual (Local) Setup

1. **Clone the repo**
2. **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3. **Install PostgreSQL and TimescaleDB** (see Timescale docs or use Homebrew)
4. **Set up DB and tables:**
    ```bash
    python src/db/setup_trading_db.py
    ```
5. **Set environment variables:**
    - Copy `.env.example` to `.env` and update values
6. **Run the FastAPI server:**
    ```bash
    uvicorn src.main:app --reload
    ```

## API Endpoints
- `GET /forecast` — Get the latest forecast
- `GET /health` — Health check
- `GET/POST /events` — Query and add unified events

## Advanced
- Modular ingestion: add new event/data sources by dropping a new fetcher in `src/events/ingest/`
- Unified reconciliation logic for multi-source event merging
- Automated batch ingestion and orchestration
- Multi-duration interval building with base duration support (5m, 15m, 1h, 1d, etc.)

## Notes
- For production, add error handling, persistent storage, and robust model retraining.
- See code for more details on extending ingestion and event reconciliation.
